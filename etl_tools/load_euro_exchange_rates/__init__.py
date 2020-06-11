import pathlib
import os

from mara_pipelines.commands.files import ReadScriptOutput
from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task


def euro_exchange_rates_pipeline(db_alias: str):
    pipeline = Pipeline(
        id="load_euro_exchange_rates",
        description="Loads daily Euro exchange rates since 1999 from the European central bank",
        base_path=pathlib.Path(__file__).parent)

    pipeline.add(
        Task(id="create_schema_and_table",
             description="Re-creates currency exchange rate schema",
             commands=[
                 ExecuteSQL(sql_file_name='create_schema_and_table.sql', echo_queries=False)
             ]))

    pipeline.add(
        Task(id='load_exchange_rate', description='Loads exchange rates from the European central bank',
             commands=[ReadScriptOutput(file_name='load_exchange_rate.py', target_table='euro_fx.exchange_rate',
                       db_alias=db_alias)]),
        upstreams=['create_schema_and_table'])

    pipeline.add(
        Task(id="postprocess_exchange_rate",
             description="Adds values for missing days",
             commands=[
                 ExecuteSQL(sql_file_name='postprocess_exchange_rate.sql', echo_queries=False)
             ]),
        upstreams=['load_exchange_rate'])

    return pipeline
