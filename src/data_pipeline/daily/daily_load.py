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
from src.util.helper import (read_stage_table, read_ingestion_conf)
from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import INSERT_SQL, UPDATE_SQL

s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0

# This is where you need to come up with the Select Columns.
# Group Columns.
# This is where you need to go with the
def process_daily_load(job_id:string, conf_bucket:string, conf_file:string):
    print("####### Read Config File. ", conf_file)
    print("############################################################")
    daily_load_conf=read_ingestion_conf(conf_bucket, conf_file)
    stage_table=daily_load_conf["table_stage_table"]
    daily_table=daily_load_conf["table_daily_table"]
    stage_df=read_stage_table(glueContext, stage_table)
    # Exception handling in case of empty DataFrame.
    if stage_df.rdd.isEmpty():
        print(f"No data found for {stage_table}")
        return

    stage_select_columns=daily_load_conf["select_columns_stage_table"]
    stage_group_columns=daily_load_conf["group_columns_stage_table"]
    stage_agg_columns=daily_load_conf["group_columns_stage_table"]
    stage_df_select=stage_df.select(*stage_select_columns)
    stage_df_group=stage_df_select.groupBy(*stage_group_columns) \
        .agg(sum('Amount'))
    print("************************ This is the SELECT ")
    stage_df_select.show(100)
    print("************************ This is the SELECT ")
    stage_df_group.show(100)
    #TODO: I need to join these two DataFrames.. and write to the DB. Call Save method.

#/*----------------StagingtoDailyTable----------------*/
#select
#company_name,
#level_name,
#acc_id,
#acc_name,
#bal_dt,
#sum( bal_amt ) as bal_amt
#from STG_FS_BAL_ST
#/*  where file_load_dt = current_date*/
#group by
#company_name,
#level_name,
#acc_id,
#acc_name,
#bal_dt