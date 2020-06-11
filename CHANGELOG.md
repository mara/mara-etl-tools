# Changelog

## 4.0.0 (2020-06-11)

- Adapt to renaming of `data-integration` package to `mara-pipelines`.

**required changes**

- Requires `mara-pipelines>=3.0.0`


## 3.1.1 (2020-05-25)

- Fix assert_almost_equal function on handling negative results


## 3.1.0 (2020-02-14)

- get schema copying working on PostgreSQL 12
- get function create_enum working on PostgreSQL 12


## 3.0.1 (2019-10-30)

- add `etl_tools.schema_check.AbortOnSchemaMisuse(schema_name)` Command
  and the helper
  `etl_tools.schema_check.add_schema_misuse_check_as_first_command_in_initial_task(pipeline)`
  which adds it as first command in the intial task in every (sub-) pipeline
  which has a `'Schema'` label.


## 3.0.0 (2019-07-07)

- Rename package from `etl-tools` to `mara-etl-tools` to avoid a PyPi name conflict.
- Avoid deadlocks in data set attribute table creation

**required-changes** 

- Adapt your requirements.txt like this: `-e git+https://github.com/mara/mara-etl-tools.git@3.0.0#egg=etl-tools
`


## 2.0.0 (2019-04-13)

- Change MARA_XXX variables to functions to delay importing of imports

**required changes** 

- Update `mara-app` to `>=2.0.0`


## 1.2.0 - 1.2.2 (2018-09-17)

- Add `year_name` and `iso_year_name` to `time.day` table
- Make schema copying work in PostgreSQL 11 (column `pg_proc.proisagg` was removed)
- Fix bug in `load_euro_exchange_rates` pipeline

**Required changes**

- Adapt ETL, especially when selecting `time.day.*`


## 1.1.0 - 1.1.2 (2018-08-18)

- Implement copying of PostgreSQL database schemas
- Add functions for creating and updating enums
- Raise error instead or warning for failed consistency checks
- Also process enum columns in util.create_data_set_attributes_table
- Add row count to data set attributes table
- Add parallel creation of attributes tables


## 1.0.0 (2018-04-11) 

- Move to Github

