# Project 03: Data Warehouse

## Overview

In this project, I'd practice on
* setting up redshift database
* drop and create tables in redshift
* specify schema for tables created
* connect and transfer data between s3 and redshift successfully
* insert records to tables
* ensure correct transformation of data modeling from raw logs to star schema

## Steps on project re-run
* correctly configure required credentials in file `dwh_demo.cfg`
* drop and create table via script in `create_tables.py`
* the specific/solid set-up for table schema and how to transform and \
insert raw staging logs to tables in star schema are well composed in `sql_queries.py`
* file `etl.py` acts as the driver script to complete the etl process, \
including loading data and inserting data.
* in the final block of `sql_queries.py`, it's provided `example_query` for us to test in aws sql console, \
so that to confirm if these jobs mentioned above are conducted as expected and the result set is correct.