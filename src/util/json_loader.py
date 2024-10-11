import string
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import datetime
import re
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as f
from functools import reduce

from src.util.db_utils import insert_record, update_record
from src.util.job_tracking import INSERT_SQL, UPDATE_SQL
from json.nested_json_utils import flatten_nested_data

import boto3

s3 = boto3.resource('s3')
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
null_rec_count=0

mysql_options = {
    "url": "jdbc:mysql://chello-insights.cb8gwiek2g7m.us-east-2.rds.amazonaws.com:3306/raw",
    "dbtable": "claims",
    "balance_sheet": "balance_sheet_debug",
    "job_tracking_table": "job_tracking",
    "user": "new_admin_user",
    "password": "Mychello08012024*"
}

def validatedata(job_id:string, raw_data_df_flatten: DataFrame, dq_rules_dict):
    column=dq_rules_dict['columns']
    columns=re.split(',', column)

    raw_data_df_flatten=raw_data_df_flatten.withColumn("null_check_result",
                                   f.when(reduce(lambda x, y: x | y, (f.col(x).isNull() for x in columns)), True).otherwise(False))
    return raw_data_df_flatten


def load_data_2_rds(raw_data_df:DataFrame, table_name:string):
    raw_data_df.write \
        .format("jdbc") \
        .option("url", mysql_options["url"]) \
        .option("dbtable", mysql_options[table_name]) \
        .option("user", mysql_options["user"]) \
        .option("password", mysql_options["password"]) \
        .mode("append") \
        .save()

def read_dq_json_file(dq_rules_conf_file:string, job_id:string):
    dq_file = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [dq_rules_conf_file],
        },
        transformation_ctx="dq_file",
    )
    dq_file_df = dq_file.toDF().repartition(1)
    dq_file_df_flatten=flatten_nested_data(dq_file_df)
    dq_file_df_flatten.show(100)
    dq_file_df=(dq_file_df_flatten.withColumn('job_id',f.lit(job_id)))
    dq_dict=dq_file_df.rdd.map(lambda row: row.asDict()).collect()
    return dq_dict

def validate_and_process_data(job_id:string, s3_location:string, dq_rules_conf_file:string):
    print("*****************************************************************")
    print(" Read Data... And Performing repartition.... ")
    print("*****************************************************************")
    raw_data = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": ",",
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [s3_location],
        },
        transformation_ctx="raw_data",
    )
    val=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    raw_data_df = raw_data.toDF().repartition(1)
    raw_data_df_flatten=flatten_nested_data(raw_data_df)
    total_record_count=raw_data_df_flatten.count()
    total_record_count_str=str(total_record_count)

    dq_rules=read_dq_json_file(dq_rules_conf_file, job_id)
    print("These are the DQ Rules.....")
    print(dq_rules)
    dq_rules_dict=dq_rules[0]
    print("These are the DQ Rules Dictionary")
    print(dq_rules_dict)

    dq_file_path=dq_rules_dict['dq_file_path']
    dq_location=dq_file_path+job_id+"/"

    # Validate and move forward.
    insert_record(INSERT_SQL,(job_id,'PAYMENT',total_record_count_str, '0', '0',val,val,s3_location,dq_location,'STARTED'))

    validate_raw_df=validatedata(job_id, raw_data_df_flatten, dq_rules_dict)
    null_values=validate_raw_df.filter(validate_raw_df['null_check_result'] == True)

    null_values.repartition(1).write.mode('overwrite').csv(dq_location,header = 'true')

    processed_rows = validate_raw_df.filter(validate_raw_df['null_check_result'] == False)

    processed_good_rows=processed_rows.drop('null_check_result')
    processed_rec_count=processed_good_rows.count()
    processed_rec_count_str=str(processed_rec_count)
    null_rec_count_str=str(null_values.count())
    end_ts=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    update_record(UPDATE_SQL, (total_record_count_str,processed_rec_count_str,null_rec_count_str,end_ts,dq_location,'COMPLETED',job_id))
    #generate_dq_report()