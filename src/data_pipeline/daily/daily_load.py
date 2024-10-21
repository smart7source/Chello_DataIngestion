import boto3
import string
from datetime import date
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from awsglue.context import GlueContext
import pyspark.sql.functions as f
from pyspark.sql.functions import col, array, when, array_remove
from src.util.helper import (read_parameter_query, load_data_2_rds_custom_mode, load_data_2_rds, read_table, write_data_2_s3, read_ingestion_conf)
from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import UPDATE_SQL_DAILY_LOAD


s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0

def process_daily_load(job_id:string, conf_bucket:string, conf_file:string):
    conf=read_ingestion_conf(conf_bucket, conf_file)
    concat_columns=conf["concat_columns_stage_table"]
    daily_query=conf["sql_config"]
    stage_df=read_parameter_query(glueContext, daily_query)
    if stage_df.rdd.isEmpty():
        print(f"No data found for After executing Daily SQL.")
        #TODO: Job is failed.
        return

    stage_table=conf["stage_table"]
    daily_table=conf["daily_table"]
    daily_table_df=read_table(glueContext, daily_table)

    daily_table_df = daily_table_df.withColumn('concatenated_cols',concat_ws("$$", *[col(x) for x in concat_columns]))
    daily_table_df = daily_table_df.withColumnRenamed('company_name', 'company_name_daily').withColumnRenamed('level_name', 'level_name_daily').withColumnRenamed('acc_id', 'acc_id_daily').withColumnRenamed('acc_name', 'acc_name_daily').withColumnRenamed('bal_dt', 'bal_dt_daily').withColumnRenamed('bal_amt', 'bal_amt_daily')

    stage_df = stage_df.withColumn('concatenated_cols',concat_ws("$$", *[col(x) for x in concat_columns]))
    stage_df = stage_df.withColumnRenamed('company_name', 'company_name_stage').withColumnRenamed('level_name', 'level_name_stage').withColumnRenamed('acc_id', 'acc_id_stage').withColumnRenamed('acc_name', 'acc_name_stage').withColumnRenamed('bal_dt', 'bal_dt_stage').withColumnRenamed('bal_amt', 'bal_amt_stage')

    print("Daily DF Counts & Data ******** ")
    daily_table_df.show(100, truncate=False)
    print(daily_table_df.count())

    print("Stage DF Counts & Data ******** ")
    stage_df.show(100, truncate=False)
    print(stage_df.count())

    diff_df_left_anti=stage_df.join(daily_table_df, stage_df["concatenated_cols"] == daily_table_df["concatenated_cols"], "leftanti")
    print(" DataFrame Left Anti Join. To Get the New/Individual Records. ")
    print(diff_df_left_anti.count())
    diff_df_left_anti.show(100, truncate=False)
    diff_df_left_anti=(diff_df_left_anti.drop("concatenated_cols").withColumn("flag_del", lit("N"))
                       .withColumn("last_updt_dt", lit("9999-12-31"))
                       .withColumnRenamed('company_name_stage', 'company_name')
                       .withColumnRenamed('level_name_stage', 'level_name').withColumnRenamed('acc_id_stage', 'acc_id')
                       .withColumnRenamed('acc_name_stage', 'acc_name')
                       .withColumnRenamed('bal_dt_stage', 'bal_dt').withColumnRenamed('bal_amt_stage', 'bal_amt'))
    print(" New/Individual Records with Flag indicator and Last Update Timestamp. ")
    print(diff_df_left_anti.count())
    diff_df_left_anti.show(100, truncate=False)
    load_data_2_rds(diff_df_left_anti, daily_table)

    diff_df_left=stage_df.join(daily_table_df, (stage_df.concatenated_cols == daily_table_df.concatenated_cols), "left")
    diff_df_left = diff_df_left.withColumn("is_due_amt_updated", when(col("bal_amt_stage") != col("bal_amt_daily"), "YES")
                                           .otherwise("NO"))
    print(" For the Common Records, what ROWS were changed in amount ? ")
    print(diff_df_left.count())
    diff_df_left.show(100, truncate=False)

    daily_updated_df=diff_df_left.filter(diff_df_left['is_due_amt_updated'] == lit("YES")).select("company_name_stage","level_name_stage","acc_id_stage","acc_name_stage","bal_dt_stage","bal_amt_stage")
    daily_expired_df=diff_df_left.filter(diff_df_left['is_due_amt_updated'] == lit("YES")).select("company_name_daily","level_name_daily","acc_id_daily","acc_name_daily","bal_dt_daily","bal_amt_daily")


    daily_updated_df=(daily_updated_df.drop("concatenated_cols").withColumnRenamed('company_name_stage', 'company_name')
                      .withColumnRenamed('level_name_stage', 'level_name')
                      .withColumnRenamed('acc_id_stage', 'acc_id')
                      .withColumnRenamed('acc_name_stage', 'acc_name')
                      .withColumnRenamed('bal_dt_stage', 'bal_dt')
                      .withColumnRenamed('bal_amt_stage', 'bal_amt')
                      .withColumn("flag_del", lit("N"))
                      .withColumn("last_updt_dt", lit("9999-12-31")))

    print(" Updated Records & Details. ")
    print(daily_updated_df.count())
    daily_updated_df.show(100, truncate=False)

    load_data_2_rds(daily_updated_df, daily_table)

    daily_expired_df=(daily_expired_df.drop("concatenated_cols").withColumnRenamed('company_name_daily', 'company_name')
                      .withColumnRenamed('level_name_daily', 'level_name')
                      .withColumnRenamed('acc_id_daily', 'acc_id')
                      .withColumnRenamed('acc_name_daily', 'acc_name')
                      .withColumnRenamed('bal_dt_daily', 'bal_dt')
                      .withColumnRenamed('bal_amt_daily', 'bal_amt')
                      .withColumn("flag_del", lit("Y"))
                      .withColumn("last_updt_dt", f.date_format(f.current_timestamp(), 'yyyy-MM-dd')))

    print(" Expired Records & Details. ")
    print(daily_expired_df.count())
    daily_expired_df.show(100, truncate=False)
    print("THis This This This This ****************************")
    for row in daily_expired_df.collect():
        print (row)
        update_record(UPDATE_SQL_DAILY_LOAD, (row.flag_del, row.last_updt_dt, row.company_name,row.level_name,row.acc_id,row.acc_name, row.bal_amt, row.bal_dt))

    #load_data_2_rds_custom_mode(daily_updated_df, daily_table, "")
    #load_data_2_rds_custom_mode(daily_expired_df, daily_table)
    print("************ DB is updated with updated data****************")

    print("************* Data is written to S3 *****************")
    #write_data_2_s3(glueContext, daily_updated_df, conf["s3_destination"])
    #write_data_2_s3(glueContext, daily_expired_df, conf["s3_destination"])
    print("---------------------Daily Job Completed---------------------")

