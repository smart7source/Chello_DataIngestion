import string
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import re
import boto3
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from functools import reduce
from collections import Counter

from src.util.helper import (load_data_2_rds, write_data_2_s3, read_raw_source,
                             provide_datetime, read_ingestion_conf, read_raw_source_tps_data)
from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import INSERT_SQL, UPDATE_SQL

s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0
