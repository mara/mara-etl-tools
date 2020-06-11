import pathlib

from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task
from etl_tools import config


def utils_pipeline(with_hll=False, with_cstore_fdw=False):
    pipeline = Pipeline(
        id="initialize_utils",
        description="Creates an utils schema with a number of functions around the ETL best practices of Project A",
        base_path=pathlib.Path(__file__).parent)

    pipeline.add_initial(
        Task(
            id="create_utils_schema",
            description="Re-creates the utils schema",
            commands=[
                ExecuteSQL(sql_statement="DROP SCHEMA IF EXISTS util CASCADE; CREATE SCHEMA util;")
            ]))

    pipeline.add(
        Task(id='chunking',
             description='Runs file chunking.sql',
             commands=[
                 ExecuteSQL(sql_file_name='chunking.sql', echo_queries=False,
                            replace={'number_of_chunks': lambda: config.number_of_chunks()})
             ]))

    def add_task_for_file(file_name_without_extension):
        pipeline.add(
            Task(id=file_name_without_extension,
                 description=f'Runs file "{file_name_without_extension}.sql"',
                 commands=[
                     ExecuteSQL(sql_file_name=file_name_without_extension + '.sql',
                                echo_queries=False)
                 ]))

    for file_name_without_extension in ['consistency_checks', 'data_sets', 'partitioning',
                                        'indexes_and_constraints',  'schema_switching', 'enums']:
        add_task_for_file(file_name_without_extension)

    if with_hll:
        add_task_for_file('hll')

    if with_cstore_fdw:
        add_task_for_file('cstore_fdw')

    return pipeline