"""Machinery for copying whole schemas from one PostgreSQL database to another"""

import shlex

import mara_db.dbs
import mara_db.postgresql
import mara_db.shell
from mara_pipelines.commands import bash
from mara_pipelines.commands.bash import RunBash
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task, ParallelTask, Command
from mara_page import _


def add_schema_copying_to_pipeline(pipeline: Pipeline, schema_name,
                                   source_db_alias: str, target_db_alias: str,
                                   max_number_of_parallel_tasks: int = 4):
    """
    Adds schema copying to the end of a pipeline.

    When the pipeline already has a final node, then the all except the last command are run before the copying,
    and the last command after.

    Args:
        pipeline: The pipeline to modify
        schema_name: The schema to copy
        source_db_alias: The alias of the PostgreSQL database to copy from
        target_db_alias: The alias of the PostgreSQL database to copy to
        max_number_of_parallel_tasks: How many operations to run at parallel at max.
    """
    task_id = "copy_schema"
    description = f"Copies the {schema_name} schema to the {target_db_alias} db"
    commands = []
    if pipeline.final_node:
        assert (isinstance(pipeline.final_node, Task))
        description = pipeline.final_node.description + ' + ' + description
        task_id = pipeline.final_node.id + '_and_' + task_id
        commands = pipeline.final_node.commands
        pipeline.remove(pipeline.final_node)

    pipeline.add_final(
        ParallelCopySchema(id=task_id, description=description, schema_name=schema_name,
                           source_db_alias=source_db_alias, target_db_alias=target_db_alias,
                           max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                           commands_before=commands[:-1], commands_after=commands[-1:]))


class ParallelCopySchema(ParallelTask):
    def __init__(self, id: str, description: str, max_number_of_parallel_tasks: int,
                 source_db_alias: str, target_db_alias: str, schema_name: str,
                 commands_before: [Command] = None, commands_after: [Command] = None) -> None:
        """In parallel copies a PostgreSQL database schema from one database to another."""

        ParallelTask.__init__(self, id=id, description=description,
                              max_number_of_parallel_tasks=max_number_of_parallel_tasks,
                              commands_before=commands_before, commands_after=commands_after)

        self.source_db_alias = source_db_alias
        self.target_db_alias = target_db_alias
        self.schema_name = schema_name

    def add_parallel_tasks(self, sub_pipeline: Pipeline) -> None:
        source_db = mara_db.dbs.db(self.source_db_alias)
        target_db = mara_db.dbs.db(self.target_db_alias)
        assert (isinstance(source_db, mara_db.dbs.PostgreSQLDB))
        assert (isinstance(target_db, mara_db.dbs.PostgreSQLDB))

        with mara_db.postgresql.postgres_cursor_context(self.source_db_alias) as cursor:
            pg_version = cursor.connection.server_version

        ddl_task = Task(
            id='create_tables_and_functions',
            description='Re-creates the schema, tables structure and functions on the target db',
            commands=[
                # schema and table structure
                bash.RunBash(
                    command="(echo 'DROP SCHEMA IF EXISTS " + self.schema_name + " CASCADE;';\\\n"
                            + "    pg_dump --username=" + source_db.user + " --host=" + source_db.host
                            + " --schema=" + self.schema_name
                            + " --section=pre-data --no-owner --no-privileges " + source_db.database + ") \\\n"
                            + "  | " + mara_db.shell.query_command(self.target_db_alias,
                                                                   echo_queries=False) + ' --quiet'),

                # function definitions
                bash.RunBash(
                    command=f'''echo "
SELECT CONCAT(pg_get_functiondef(pg_proc.oid),';') AS def 
FROM (SELECT oid, prokind, pronamespace
      FROM pg_proc p 
      WHERE {"p.prokind in ('p','f')" if pg_version >= 110000 else "NOT p.proisagg"}) pg_proc, pg_namespace
WHERE pg_proc.pronamespace = pg_namespace.oid
     AND nspname = '{self.schema_name}'" \\\n'''
                            + "  | " + mara_db.shell.copy_to_stdout_command(self.source_db_alias) + ' \\\n'
                            + "  | " + mara_db.shell.query_command(self.target_db_alias, echo_queries=False))
            ])
        sub_pipeline.add(ddl_task)

        # copy content of tables
        number_of_chunks = self.max_number_of_parallel_tasks * 3
        table_copy_chunks = {i: [] for i in range(0, number_of_chunks)}
        current_size_per_table_copy_chunk = [0] * number_of_chunks
        table_types = {}

        with mara_db.postgresql.postgres_cursor_context(
                self.source_db_alias) as cursor:  # type: psycopg2.extensions.cursor
            cursor.execute("""
SELECT 
    pg_class.relname AS table,
    relkind,
    CASE WHEN relkind = 'f' 
         THEN cstore_table_size(nspname || '.' || relname) * 10 -- cstore tables with similar size take longer to copy 
         ELSE  pg_total_relation_size(pg_class.oid)
    END / 1000000.0 AS size
FROM pg_class
JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE nspname = '""" + self.schema_name + """' AND relkind IN ('r', 'f') AND relhassubclass = 'f'
ORDER BY size DESC""")
            for table_name, type, size in cursor.fetchall():
                smallest_chunk_index = min(range(len(current_size_per_table_copy_chunk)),
                                           key=current_size_per_table_copy_chunk.__getitem__)
                current_size_per_table_copy_chunk[smallest_chunk_index] += size
                table_copy_chunks[smallest_chunk_index].append(table_name)
                table_types[table_name] = type

            copy_tasks = []
            for i, tables in table_copy_chunks.items():
                if tables:
                    task = Task(
                        id=f'copy_tables_{i}',
                        description='Copies table content to the frontend db',
                        commands=[RunBash(
                            command=f'echo {shlex.quote(f"COPY {self.schema_name}.{table_name} TO STDOUT")} \\\n'
                                    + '  | ' + mara_db.shell.copy_to_stdout_command(self.source_db_alias) + ' \\\n'
                                    + '  | ' + mara_db.shell.copy_from_stdin_command(self.target_db_alias,
                                                                                     target_table=f'{self.schema_name}.{table_name}'))
                            for table_name in tables])
                    copy_tasks.append(task)
                    sub_pipeline.add(task, upstreams=[ddl_task])

            # create indexes
            index_chunks = {i: [] for i in range(0, number_of_chunks)}
            current_size_per_index_chunk = [0] * number_of_chunks

            with mara_db.postgresql.postgres_cursor_context(self.source_db_alias) as cursor:
                cursor.execute(""" 
SELECT indexdef AS ddl, pg_total_relation_size(pg_class.oid) AS size
FROM pg_class
JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
JOIN pg_indexes ON pg_indexes.indexname = pg_class.relname AND pg_indexes.schemaname = nspname
WHERE nspname = '""" + self.schema_name + """' AND relkind = 'i'
ORDER BY size DESC;""")
                for ddl, size in cursor.fetchall():
                    smallest_chunk_index = min(range(len(current_size_per_index_chunk)),
                                               key=current_size_per_index_chunk.__getitem__)
                    current_size_per_index_chunk[smallest_chunk_index] += size
                    index_chunks[smallest_chunk_index].append(ddl)

            for i, index_statements in index_chunks.items():
                if index_statements:
                    index_task = Task(id=f'add_indexes_{i}', description='Re-creates indexes on frontend db',
                                      commands=[ExecuteSQL(sql_statement=statement, db_alias=self.target_db_alias)
                                                for statement in index_statements])
                    sub_pipeline.add(index_task, upstreams=copy_tasks)

    def html_doc_items(self) -> [(str, str)]:
        return [('schema', _.tt[self.schema_name]),
                ('source db', _.tt[self.source_db_alias]),
                ('target db', _.tt[self.target_db_alias])]
