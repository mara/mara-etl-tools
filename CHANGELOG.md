# Changelog

## 1.2.0 - 1.2.1 (2018-09-17)

- Add `year_name` and `iso_year_name` to `time.day` table
- Make schema copying work in PostgreSQL 11 (column `pg_proc.proisagg` was removed)

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

