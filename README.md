# ETL Utils

A collection of utilities around [Project A](https://project-a.com/)'s best practices for creating [data integration](https://github.com/mara/data-integration) pipelines with mara. The package is intended as a start for new projects. Forks/ copies are preferred over PRs.

For more details on how to use this package, have a look at the [mara example project](https://github.com/mara/mara-example-project).


The package consists of a number modules that all can be used independently from each other:

## SQL utility functions

Function `initialize_utils` in [etl_tools/initialize_utils/__init__.py](etl_tools/initialize_utils/__init__.py) returns a pipeline that creates a `util` schema with a number of PostgreSQL functions for organizing data pipelines. Add to your root pipeline like this:

```python
from etl_tools import initialize_utils

my_pipeline.add(initialize_utils.utils_pipeline(with_hll=True, with_cstore_fdw=True))
```

Please have a look at the .sql files in [etl_tools/initialize_utils](etl_tools/initialize_utils) for available functions.


## Schema copying

The file The file [etl_tools/schema_copying.py](etl_tools/schema_copying.py) contains the function `add_schema_copying_to_pipeline` that copies a PostgreSQL database schema from on host to another at the end of a pipeline run. This is useful for running the ETL and frontend tools on different database servers so that a running ETL does not affect the performance of dashboard queries.


Given that there is a pipline `my_pipeline` that has a number of child pipelines with the `Schema` label set to the respective schema to copy, then this is how the schema copying can be added to those child pipelines.

```python
from mara_db import dbs
from data_integration.commands.sql import ExecuteSQL
from data_integration.pipelines import Task
from etl_tools.schema_copying import add_schema_copying_to_pipeline

# when etl und frontend db are different, add schema copying
if dbs.db('mdwh-etl').database != dbs.db('mdwh-frontend').database \
        or dbs.db('mdwh-etl').host != dbs.db('mdwh-frontend').host:

    # run some of the files from etl_tools/initalize_utils in frontend db
    initialize_frontend_db_commands = [ExecuteSQL(
        sql_statement="DROP SCHEMA IF EXISTS util CASCADE; CREATE SCHEMA util;", db_alias='mdwh-frontend')]

    for file_name in ['schema_switching.sql', 'data_sets.sql', 'hll.sql', 'cstore_fdw.sql']:
        initialize_frontend_db_commands.append(
            ExecuteSQL(sql_file_name=str(
                my_pipeline.nodes['utils'].nodes['initialize_utils'].base_path() / file_name),
                db_alias='mdwh-frontend'))

    my_pipeline.nodes['utils'].add(
        Task(id='initialize_frontend_db',
             description='Adds some functions to the frontend db so that schema copying works',
             commands=initialize_frontend_db_commands))

    # Add schema copying for time schema
    add_schema_copying_to_pipeline(pipeline=my_pipeline.nodes['utils'].nodes['create_time_dimensions'],
                                   schema_name='time',
                                   source_db_alias='dwh-etl', target_db_alias='dwh-frontend')

    # Add schema copying to all root pipelines
    for pipeline in my_pipeline.nodes.values():
        if "Schema" in pipeline.labels:
            schema = pipeline.labels['Schema']
            add_schema_copying_to_pipeline(pipeline=pipeline, schema_name=schema + '_next',
                                           source_db_alias='dwh-etl', target_db_alias='dwh-frontend')
            pipeline.final_node.commands_after.append(
                ExecuteSQL(sql_statement=f"SELECT util.replace_schema('{schema}', '{schema}_next')",
                           db_alias='mdwh-frontend')
            )
```
 

## Time dimensions

The file [etl_tools/create_time_dimensions/__init__.py](etl_tools/create_time_dimensions/__init__.py) defines a pipeline that creates and updates `time` schema with the tables `day` and `duration`:

```
select * from time.day order by _date desc limit 10;
     day_id  |     day_name     | year_id | iso_year_id | quarter_id | quarter_name | month_id | month_name | week_id |  week_name   | day_of_week_id | day_of_week_name | day_of_month_id |   _date    
   ----------+------------------+---------+-------------+------------+--------------+----------+------------+---------+--------------+----------------+------------------+-----------------+------------
    20190815 | Thu, Aug 15 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201933 | 2019 - CW 33 |              4 | Thursday         |              15 | 2019-08-15
    20190814 | Wed, Aug 14 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201933 | 2019 - CW 33 |              3 | Wednesday        |              14 | 2019-08-14
    20190813 | Tue, Aug 13 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201933 | 2019 - CW 33 |              2 | Tuesday          |              13 | 2019-08-13
    20190812 | Mon, Aug 12 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201933 | 2019 - CW 33 |              1 | Monday           |              12 | 2019-08-12
    20190811 | Sun, Aug 11 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              7 | Sunday           |              11 | 2019-08-11
    20190810 | Sat, Aug 10 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              6 | Saturday         |              10 | 2019-08-10
    20190809 | Fri, Aug 09 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              5 | Friday           |               9 | 2019-08-09
    20190808 | Thu, Aug 08 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              4 | Thursday         |               8 | 2019-08-08
    20190807 | Wed, Aug 07 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              3 | Wednesday        |               7 | 2019-08-07
    20190806 | Tue, Aug 06 2019 |    2019 |        2019 |      20193 | 2019 Q3      |   201908 | 2019 Aug   |  201932 | 2019 - CW 32 |              2 | Tuesday          |               6 | 2019-08-06
```

```
select * from time.duration where duration_id >= 0 order by duration_id limit 10;
 duration_id | days | days_name | weeks | weeks_name | four_weeks | four_weeks_name | months | months_name | sixth_years | sixth_years_name | half_years | half_years_name | years | years_name 
-------------+------+-----------+-------+------------+------------+-----------------+--------+-------------+-------------+------------------+------------+-----------------+-------+------------
           0 |    0 | 0 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           1 |    1 | 1 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           2 |    2 | 2 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           3 |    3 | 3 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           4 |    4 | 4 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           5 |    5 | 5 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           6 |    6 | 6 days    |     0 | 0-6 days   |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           7 |    7 | 7 days    |     1 | 7-13 days  |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           8 |    8 | 8 days    |     1 | 7-13 days  |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days
           9 |    9 | 9 days    |     1 | 7-13 days  |          0 | 0-27 days       |      0 | 0-29 days   |           0 | 0-59 days        |          0 | 0-179 days      |     0 | 0-359 days

```

Add the pipeline to your project with 

```bash
from etl_tools import create_time_dimensions

my_pipeline.add(create_time_dimensions.pipeline)
```

Set min and max dates by overwriting the `first_date_in_time_dimensions` and `last_date_in_time_dimensions` in [etl_tools/config.py](etl_tools/config.py).


## Euro currency exchange rates

The file [etl_tools/load_euro_exchange_rates/__init__.py](etl_tools/create_time_dimensions/__init__.py) contains a pipeline that loads (historic) Euro exchange rates from the European central bank. 


Add to your pipeline with 

```bash
from etl_tools import load_euro_exchange_rates

my_pipeline.add(load_euro_exchange_rates.euro_exchange_rates_pipeline('db-alias'))
```



