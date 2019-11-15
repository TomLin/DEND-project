# Project 04: Data Lake

## Overview

In this project, I'd practice on
* launch an AWS EMR
* set up spark session, including launching hadoop-aws module for use of interaction between spark and aws
* read in json files on the fly
* implement a series of ETL on the dataset
* save processed data in partitioned parquet

## Steps on project re-run
* specify or create s3 bucket (for data source and destination) if needed
* launch aws emr
* correctly configure required credentials in file `dl_demo.cfg`
* set up environmental variable of python path for spark using command<br>
`sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh`,<br> so that spark correctly use python3 to run program
* run `etl.py` script in emr command line, using command `spark-submit --master yarn etl.py`
* the final OLAP dataset is saved in destination bucket ready for use. 