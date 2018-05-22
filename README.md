# ETL Utils

A collection of utilities around [Project A](https://project-a.com/)'s best practices for creating [data integration](https://github.com/mara/data-integration) pipelines with mara. It consists of these modules (which all can be used independently from each other):
  

* [etl_tools/initialize_utils](etl_tools/initialize_utils): A pipeline that creates a list of sql utility functions.

* [etl_tools/create_time_dimensions](etl_tools/create_time_dimensions): A small pipeline for creating a time and a duration dimension.

* [etl_tools/load_euro_exchange_rates](etl_tools/create_time_dimensions): A pipeline that loads (historic) Euro exchange rates from the European central bank. 

For more details on how to use this package, have a look at the [mara example project](https://github.com/mara/mara-example-project).
