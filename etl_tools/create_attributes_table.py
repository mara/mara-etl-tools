"""Parallel creation of an attribute lookup table for another table (e.g. for auto-completion)"""

import re

import data_integration.config
import mara_db.postgresql
from data_integration.commands.sql import ExecuteSQL
from data_integration.pipelines import Pipeline, ParallelTask, Task
from mara_page import _


class CreateAttributesTable(ParallelTask):
    def __init__(self, id: str, source_schema_name: str, source_table_name: str,
                 db_alias: str = None,
                 attributes_table_suffix: str = '_attributes',
                 max_number_of_parallel_tasks: int = None) -> None:
        """
        Creates an indexed lookup table for providing fast auto-completion on the values of a table

        Given a table `foo.bar` like

        a | b | c
        --+---+---
        1 | x | i
        2 | y | i
        3 | x | i

        it creates a table foo.bar_attributes such as

        attribute | value | row_count
        ----------+-------+----------
        b         | x     | 2
        b         | y     | 1
        c         | i     | 3

        Args:
            id: The id of the task
            source_schema_name: The schema of the original table, e.g. 'foo'
            source_table_name: The name of the original table, e.g. 'bar'
            db_alias: The database alias for the source and attributes table
            attributes_table_suffix: This suffix will be appended to the source table name
            max_number_of_parallel_tasks: How many child tasks to run at most
        """
        super().__init__(id,
                         description=f'Creates an attributes lookup table on {source_schema_name}.{source_table_name}.',
                         max_number_of_parallel_tasks=max_number_of_parallel_tasks)

        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.attributes_table_suffix = attributes_table_suffix
        self.db_alias = db_alias or data_integration.config.default_db_alias()

    def add_parallel_tasks(self, sub_pipeline: Pipeline) -> None:
        attributes_table_name = f'{self.source_schema_name}.{self.source_table_name}{self.attributes_table_suffix}'
        sub_pipeline.add_initial(
            Task(id='create_table', description='Creates the attributes table',
                 commands=[
                     ExecuteSQL(sql_statement=f'''
DROP TABLE IF EXISTS {attributes_table_name};

CREATE TABLE {attributes_table_name} (
    attribute TEXT NOT NULL, 
    value     TEXT NOT NULL, 
    row_count BIGINT NOT NULL
) PARTITION BY LIST (attribute);
''')
                 ]))

        with mara_db.postgresql.postgres_cursor_context(self.db_alias) as cursor:  # type: psycopg2.extensions.cursor
            cursor.execute(f'''
WITH enums AS (
    SELECT DISTINCT
      typname,
      nspname
    FROM pg_type
      JOIN pg_enum ON pg_type.oid = pg_enum.enumtypid
      JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
  )
SELECT column_name
FROM information_schema.columns
  LEFT JOIN enums ON udt_schema = enums.nspname AND udt_name = enums.typname
  WHERE table_schema = {'%s'}
      AND table_name = {'%s'}
      AND (data_type IN ('text', 'varchar') OR enums.typname IS NOT NULL);
''', (self.source_schema_name, self.source_table_name))

            i = 0

            for column_name, in cursor.fetchall():
                i += 1
                sub_pipeline.add(Task(
                    id=re.sub('\W+', '_', column_name.lower()),
                    description=f'Extracts attributes for the {column_name} column',
                    commands=[
                        ExecuteSQL(sql_statement=f'''
CREATE TABLE {attributes_table_name}_{i} PARTITION OF {attributes_table_name} FOR VALUES IN ('{column_name}');

INSERT INTO {attributes_table_name}_{i} 
SELECT '{column_name}', "{column_name}", count(*)
FROM {self.source_schema_name}.{self.source_table_name}
WHERE "{column_name}" IS NOT NULL
GROUP BY "{column_name}"
ORDER BY "{column_name}";

CREATE INDEX {self.source_table_name}_{self.attributes_table_suffix}_{i}__value 
   ON {attributes_table_name}_{i} USING GIN (value gin_trgm_ops);
''', echo_queries=False)
                    ]))

    def html_doc_items(self) -> [(str, str)]:
        return [('db', _.tt[self.db_alias]),
                ('source schema', _.tt[self.source_schema_name]),
                ('source table', _.tt[self.source_table_name]),
                ('attributes table suffix', _.tt[self.attributes_table_suffix])]
