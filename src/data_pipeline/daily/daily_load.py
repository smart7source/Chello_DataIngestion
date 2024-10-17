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
from src.util.helper import (read_parameter_query, read_ingestion_conf, read_table)
from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import INSERT_SQL, UPDATE_SQL
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from getpass import getpass
from pyspark.sql.types import *
import datetime
import string
import re
import yaml
import boto3
from awsglue.context import GlueContext
from pyspark.sql.dataframe import DataFrame

import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, array, when, array_remove


s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0


stage_select_columns = ['company_name', 'level_name', 'acc_id', 'acc_name', 'bal_dt', 'bal_amt']
concat_columns = ['company_name', 'level_name', 'acc_id', 'acc_name', 'bal_dt']
daily_query_1=f"""
select company_name, level_name, acc_id, acc_name, bal_dt, sum(bal_amt) as bal_amt from STG_FS_BAL_ST_V where file_load_dt = '2024-09-15' group by company_name, level_name, acc_id, acc_name, bal_dt
"""

def compare_data_frames(daily_table_df_select:DataFrame, stage_df_select:DataFrame):
    print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    print()
    joined_df=daily_table_df_select.join(stage_df_select,
                         (daily_table_df_select['company_name'] == stage_df_select['company_name']) &
                         (daily_table_df_select['level_name'] == stage_df_select['level_name']) &
                         (daily_table_df_select['acc_id'] == stage_df_select['acc_id']) &
                         (daily_table_df_select['acc_name'] == stage_df_select['acc_name']) &
                         (daily_table_df_select['bal_dt'] == stage_df_select['bal_dt']))
    print("What are the Common Elements.")
    return joined_df


def process_daily_load():
    stage_df=read_parameter_query(glueContext, daily_query_1)
    if stage_df.rdd.isEmpty():
        print(f"No data found for After executing Daily SQL.")
        #TODO: Job is failed.
        return

    daily_table_df=read_table(glueContext, "DAILY_FS_BAL_ST_TESTING_V")
    daily_table_df_select=daily_table_df.select(*stage_select_columns)
    stage_df_select=stage_df.select(*stage_select_columns)


    daily_table_df_select = daily_table_df_select.withColumn('concatenated_cols',concat_ws("$$", *[col(x) for x in concat_columns]))
    daily_table_df_select = daily_table_df_select.withColumnRenamed('company_name', 'company_name_daily').withColumnRenamed('level_name', 'level_name_daily').withColumnRenamed('acc_id', 'acc_id_daily').withColumnRenamed('acc_name', 'acc_name_daily').withColumnRenamed('bal_dt', 'bal_dt_daily').withColumnRenamed('bal_amt', 'bal_amt_daily')

    stage_df_select = stage_df_select.withColumn('concatenated_cols',concat_ws("$$", *[col(x) for x in concat_columns]))
    stage_df_select = stage_df_select.withColumnRenamed('company_name', 'company_name_stage').withColumnRenamed('level_name', 'level_name_stage').withColumnRenamed('acc_id', 'acc_id_stage').withColumnRenamed('acc_name', 'acc_name_stage').withColumnRenamed('bal_dt', 'bal_dt_stage').withColumnRenamed('bal_amt', 'bal_amt_stage')


    print("I am creating the Two Data Frames with the Columns that are needed..... ")
    print("Only the ")
    daily_table_df_select.show(100, truncate=False)

    print(daily_table_df_select.count())
    print("Hello 456")
    stage_df_select.show(100, truncate=False)
    daily_table_df_select.show(100, truncate=False)
    print("Hello 82374847")

    diff_df_left_anti=stage_df_select.join(daily_table_df_select, stage_df_select["concatenated_cols"] == daily_table_df_select["concatenated_cols"], "leftanti")
    print(" After After 7777777777777 BEFORE &&&&&&&&&&&&&&&&&&&&&&&& * ")
    print(diff_df_left_anti.count())
    diff_df_left_anti.show(100, truncate=False)
    diff_df_left_anti=diff_df_left_anti.withColumn("flag_del", lit("N")).withColumn("last_updt_dt", lit("9999-99-99"))
    print(" After After 7777777777777 AFTER &&&&&&&&&&&&&&&&&&&&&&&& * ")
    print(diff_df_left_anti.count())
    diff_df_left_anti.show(100, truncate=False)

    diff_df_left=stage_df_select.join(daily_table_df_select, (stage_df_select.concatenated_cols == daily_table_df_select.concatenated_cols), "left")
    print(" Left Join 1 ")
    print(diff_df_left.count())
    diff_df_left.show(100, truncate=False)
    diff_df_left = diff_df_left.withColumn("is_due_amt_updated", when(col("bal_amt_stage") != col("bal_amt_daily"), "YES")
                                           .otherwise("NO"))
    print(" Left Join 22222 ")
    print(diff_df_left.count())
    diff_df_left.show(100, truncate=False)

    final_df=diff_df_left.filter(diff_df_left['is_due_amt_updated'] == lit("YES")).select("company_name_stage","level_name_stage","acc_id_stage","acc_name_stage","bal_dt_stage","bal_amt_stage")

    print(final_df.count())
    final_df.show(100, truncate=False)
    final_df=final_df.withColumnRenamed('company_name_stage', 'company_name').withColumnRenamed('level_name_stage', 'level_name').withColumnRenamed('acc_id_stage', 'acc_id').withColumnRenamed('acc_name_stage', 'acc_name').withColumnRenamed('bal_dt_stage', 'bal_dt').withColumnRenamed('bal_amt_stage', 'bal_amt').withColumn("flag_del", lit("Y")).withColumn("last_updt_dt", f.date_format(f.current_timestamp(), 'yyyy-MM-dd'))
    print("***************************** Finsh DF before writing to DB ")
    print(final_df.count())
    final_df.show(100, truncate=False)
    print("---------------------Daily Job Completed---------------------")


def process_daily_load(job_id:string, conf_bucket:string, conf_file:string, curr_date:string):
    print("####### Read Config File. ", conf_file)
    print("############################################################")
    daily_load_conf=read_ingestion_conf(conf_bucket, conf_file)
    stage_table=daily_load_conf["table_stage_table"]
    sql_qry=daily_load_conf["sql_config"]
    daily_table=daily_load_conf["table_daily_table"]
    #TODO: Make this Query Parameterized.
    stage_df=read_parameter_query(glueContext, sql_qry)
    # Exception handling in case of empty DataFrame.
    if stage_df.rdd.isEmpty():
        print(f"No data found for {stage_table}")
        #TODO: Job is failed.
        return

    daily_table_df=read_table(glueContext, daily_table)
    stage_select_columns=daily_load_conf["select_columns_stage_table"]
    daily_table_df_select=daily_table_df.select(*stage_select_columns)
    stage_df_select=stage_df.select(*stage_select_columns)
    stage_df_select.show(100)
    print("************************ This is the SELECT ")
    daily_table_df_select.show(100)
    compare_data_frames(daily_table_df_select, stage_df_select)
