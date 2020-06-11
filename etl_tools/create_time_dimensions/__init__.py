import pathlib

from mara_pipelines.commands.sql import ExecuteSQL
from mara_pipelines.pipelines import Pipeline, Task
from etl_tools import config

pipeline = Pipeline(
    id="create_time_dimensions",
    description="Creates a day and a duration dimension table",
    labels={"Schema": "time"},
    base_path=pathlib.Path(__file__).parent)

pipeline.add(
    Task(id="create_tables",
         description="Re-creates the day and duration table and their schema",
         commands=[
             ExecuteSQL(sql_file_name='create_tables.sql', echo_queries=False,
                        file_dependencies=['create_tables.sql'])
         ]))

pipeline.add(
    Task(id="populate_time_dimensions", description="fills the time dimensions for a configured time range",
         commands=[
             ExecuteSQL(sql_statement=lambda: "SELECT time.populate_time_dimensions('"
                                              + config.first_date_in_time_dimensions().isoformat() + "'::DATE, '"
                                              + config.last_date_in_time_dimensions().isoformat() + "'::DATE);")]),
    upstreams=['create_tables'])
