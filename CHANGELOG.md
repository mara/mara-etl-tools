# Changelog

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

