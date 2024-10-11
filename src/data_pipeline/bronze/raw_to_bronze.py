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
                             provide_datetime, read_ingestion_conf, enrich_data, read_raw_source_tps_data)
from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import INSERT_SQL, UPDATE_SQL

s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0

mysql_options = {
    "url": "jdbc:mysql://chello-insights.cb8gwiek2g7m.us-east-2.rds.amazonaws.com:3306/DB_Chello",
    "dbtable": "claims",
    "balance_sheet": "balance_sheet_debug",
    "job_tracking_table": "job_tracking",
    "user": "new_admin_user",
    "password": "Mychello08012024*"
}

def load_data_and_write_to_s3(processed_rows:DataFrame, conf):
    print(" Write Data to DB and Write data to S3 File.")
    load_data_2_rds(processed_rows, conf["table_bronze_layer"])
    print("************ DB is updated with updated data****************")
    print("************* Data is written to S3 *****************")
    write_data_2_s3(glueContext, processed_rows, conf["s3_destination"])


def prep_data(raw_data_df:DataFrame, conf):
    select_columns= conf["raw_json_flatten_columns"]
    cols_from_df=raw_data_df.columns
    remaining = Counter(cols_from_df)
    out = []
    for val in select_columns:
        if remaining[val]:
            remaining[val] -= 1
        else:
            out.append(val)

    for col_value in out:
        raw_data_df = raw_data_df.withColumn(col_value, f.lit(""))

    return raw_data_df



def perform_validation_ingestion_activity(raw_data_df:DataFrame, job_id:string, conf_bucket:string, conf_file:string, raw_file_path:string):
    print("####### Read Config File. ", conf_file)
    print("############################################################")
    conf=read_ingestion_conf(conf_bucket, conf_file)
    raw_2_db_mappings= conf["raw_json_2_database"]
    select_columns= conf["raw_json_flatten_columns"]
    print("############################################################")
    raw_data_df = prep_data(raw_data_df, conf)
    raw_data_df_select=raw_data_df.select(*select_columns)
    print("raw_data_df_selectraw_data_df_select Total Record Count Before Flatten...", raw_data_df_select.count())
    print("############################################################")

    enrich_df=enrich_data(raw_data_df_select, raw_2_db_mappings)
    print("These are the details of the countssss")
    print(enrich_df.count())

    enrich_df=enrich_df.dropDuplicates()
    print("These are the details of the counts after the DROP Duplicates......")
    print(enrich_df.count())
    total_record_count=enrich_df.count()
    total_record_count_str=str(total_record_count)
    dq_rules_dict=conf["dataquality"]


    insert_record(INSERT_SQL,(job_id,'PAYMENT',total_record_count_str, '0', '0',provide_datetime(),provide_datetime(),raw_file_path, '', 'STARTED'))
    validate_raw_df=validatedata(job_id, enrich_df, dq_rules_dict)

    null_values=validate_raw_df.filter(validate_raw_df['null_check_result'] == True)
    processed_good_rows = validate_raw_df.filter(validate_raw_df['null_check_result'] == False)

    processed_rows=processed_good_rows.drop('null_check_result')
    null_values=null_values.drop('null_check_result')
    null_values_int=null_values.count()
    processed_rec_count=processed_rows.count()
    processed_rec_count_str=str(processed_rec_count)
    null_rec_count_str=str(null_values_int)

    load_data_and_write_to_s3(processed_rows, conf)

    dq_file_path=conf['dataquality_output_path']
    dq_location=''
    if null_values_int > 0:
        print("There was a DQ, and here is the file...")
        dq_location=dq_file_path+job_id+"/"
        write_data_2_s3(glueContext, null_values, dq_location)

    update_record(UPDATE_SQL, (total_record_count_str,processed_rec_count_str,null_rec_count_str,provide_datetime(),dq_location,'COMPLETED',job_id))


def process_ingestion_tps(raw_file_path:string, job_id:string, conf_bucket:string, conf_file:string):
    print("############################################################")
    print("####### Start of Ingestion. ", raw_file_path)
    print("####### Start of Ingestion for the Job iD. ", job_id)
    print("####### Read Config File. ", conf_file)
    print("############################################################")
    raw_data_df=read_raw_source_tps_data(glueContext, raw_file_path, job_id)
    print(" A:: A;; tje Data **************************")
    print(raw_data_df.printSchema())
    raw_data_df.show(100)
    print("Raw File Total Record Count Before Flatten...", raw_data_df.count())

    #TODO: Same method for the Trizetto and Codat.
    perform_validation_ingestion_activity(raw_data_df, job_id, conf_bucket, conf_file, raw_file_path)
    print("********************** TPS Data Ingestion Completed **************************")


def process_ingestion(raw_file_path:string, job_id:string, conf_bucket:string, conf_file:string):
    print("############################################################")
    print("####### Start of Ingestion. ", raw_file_path)
    print("####### Start of Ingestion for the Job iD. ", job_id)

    print("############################################################")
    raw_data_df=read_raw_source(glueContext, raw_file_path, job_id)
    print("Raw File Total Record Count Before Flatten...", raw_data_df.count())

    #TODO: Same method for the Trizetto and Codat.
    perform_validation_ingestion_activity(raw_data_df, job_id, conf_bucket, conf_file, raw_file_path)
    print("********************** Data Ingestion Completed **************************")



def is_not_blank(s):
    return bool(s and not s.isspace())

def validatedata(job_id:string, raw_data_df_flatten: DataFrame, dq_rules_dict):
    column=dq_rules_dict['null_check_columns']
    if is_not_blank(column):
        columns=re.split(',', column)
        columns_size = len(columns)
        print("columns------------------>")
        print(columns)
        print("columns_size--------------->>>>")
        print(columns_size)
        print("columns_size--------------->>>>")
        print(columns_size)
        print("About to Validation Triggered...................")
        print("Validation Triggered..............")
        raw_data_df_flatten=raw_data_df_flatten.withColumn("null_check_result",
                                                           f.when(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in columns)), True).otherwise(False))
    else:
        raw_data_df_flatten=raw_data_df_flatten.withColumn("null_check_result", f.lit(False))

    print("DONE DONE DONE DONE DONE I am printing the ROWS HERE ********* ---------- Validation Triggered...................")
    raw_data_df_flatten.show(100)
    return raw_data_df_flatten


