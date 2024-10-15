import datetime
import string
import re
import yaml
import boto3
from awsglue.context import GlueContext
from pyspark.sql.dataframe import DataFrame

from src.util.constants import PARTITION_KEYS
from src.util.json.flatten_json_df import flatten_nested_data
import pyspark.sql.functions as f
from awsglue.dynamicframe import DynamicFrame

def provide_datetime():
    val=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    return val

def provide_datetime_YYYYMMDD():
    val=datetime.datetime.now().strftime('%Y%m%d')
    return val

def add_date_partition_key_to_s3_prefix(s3_prefix):
    t = datetime.today()
    partition_key = f"import_year={t.strftime('%Y')}/import_month={t.strftime('%m')}/import_day={t.strftime('%d')}/import_date={t.strftime('%Y%m%d')}/"  # noqa
    return f"{s3_prefix}{partition_key}"


mysql_options = {
    "url": "jdbc:mysql://chello-insights.cb8gwiek2g7m.us-east-2.rds.amazonaws.com:3306/DB_Chello",
    "dbtable": "claims",
    "balance_sheet": "balance_sheet_debug",
    "job_tracking_table": "job_tracking",
    "user": "new_admin_user",
    "password": "Mychello08012024*"
}

def read_dq_json_file(glue_context: GlueContext,dq_rules_conf_file:string, job_id:string):
    dq_file = glue_context.create_dynamic_frame.from_options(
        format_options={
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

def read_raw_source_tps_data(glue_context: GlueContext, raw_file_path:string, job_id:string):
    raw_data = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [raw_file_path],
        },
        transformation_ctx="raw_data",
        format_options={
            "withHeader": True,
            "separator": "|"
        })
    raw_data_df = raw_data.toDF()
    raw_data_df=(raw_data_df.withColumn('job_id',f.lit(job_id)).withColumn('file_name',f.lit(raw_file_path))
                 .withColumn('file_load_dt',f.date_format(f.current_timestamp(), 'yyyy-MM-dd')))
    return raw_data_df


def read_raw_source(glue_context: GlueContext, raw_file_path:string, job_id:string):
    raw_data = glue_context.create_dynamic_frame.from_options(
        format_options={
            "multiline": False,
        },
        connection_type="s3",
        format="json",
        connection_options={
            "paths": [raw_file_path],
        },
        transformation_ctx="raw_data",
    )
    raw_data_df = raw_data.toDF()
    raw_data_df_flatten=flatten_nested_data(raw_data_df)
    raw_data_df_final=(raw_data_df_flatten.withColumn('job_id',f.lit(job_id))
                       .withColumn('file_name',f.lit(raw_file_path))
                       .withColumn('file_load_dt',f.date_format(f.current_timestamp(), 'yyyy-MM-dd')))
    return raw_data_df_final


def write_data_2_s3(glue_context: GlueContext, enrich_df:DataFrame, location:string):
    #Convert from Spark Data Frame to Glue Dynamic Frame
    data_df = DynamicFrame.fromDF(enrich_df, glue_context, "convert")
    glue_context.write_dynamic_frame.from_options(frame = data_df,
                                                  connection_type = "s3",
                                                  format = "parquet",
                                                  connection_options = {"path": location},
                                                  transformation_ctx = "data_sink_1")


def read_stage_table(glue_context: GlueContext, table_name:string):
    query = "SELECT * FROM " + table_name
    print("Hey I am printing the Query.............. ")
    mysql_dynamic_frame = glue_context.read.format("jdbc") \
        .option("driver", "com.mysql.jdbc.Driver").option("url", mysql_options["url"]) \
        .option("user", mysql_options["user"]) \
        .option("password", mysql_options["password"]).option("dbtable", query) \
        .load()

    return mysql_dynamic_frame

def load_data_2_rds(raw_data_df:DataFrame, table_name:string):
    raw_data_df.write \
        .format("jdbc") \
        .option("url", mysql_options["url"]) \
        .option("dbtable", table_name) \
        .option("user", mysql_options["user"]) \
        .option("password", mysql_options["password"]) \
        .mode("append") \
        .save()


#TODO: In Case of Data is landing in multiple files or How to find the paritions
def add_import_date_columns_from_path(df,partition_path):
    key_import_date = find_importdate(partition_path)
    import_day = key_import_date[6:]
    import_month = key_import_date[4:6]
    import_year = key_import_date[:4]
    print(f'Import Dates: {key_import_date}: Y{import_year} M{import_month} D{import_day}')

    df = df.withColumn("import_date", f.lit(key_import_date)).withColumn("import_day", f.lit(import_day)).withColumn("import_month", f.lit(import_month)).withColumn("import_year", f.lit(import_year))

    return df



def read_ingestion_conf(conf_bucket:string, conf_file:string):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=conf_bucket, Key=conf_file)
    try:
        conf = yaml.safe_load(response["Body"])
    except yaml.YAMLError as exc:
        return exc

    return conf


def enrich_data(raw_data_df:DataFrame, raw_2_db_mappings):
    for old_col, new_col in zip(raw_2_db_mappings.keys(), raw_2_db_mappings.values()):
        raw_data_df = raw_data_df.withColumnRenamed(old_col, new_col)

    raw_data_df.show()
    return raw_data_df




# Get Raw zone latest partition
def find_importdate(importstring):
    import_date = re.search("[0-9]{8}", importstring).group()
    return import_date


def get_all_partitions(s3_client, bucket, prefix, raw_date):
    print(f'Bucket: {bucket}')
    print(f'Prefix: {prefix}')

    list_of_folders = []

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            filestring = obj["Key"]
            import_date = find_importdate(filestring)
            if int(import_date) > int(raw_date):
                filestring = re.sub(string=filestring,
                                    pattern="\/(?![\s\S]*\/).*",
                                    repl="")
                list_of_folders.append(filestring)
            else:
                continue

    deduped_list = list(set(list_of_folders))
    deduped_list.sort()

    return deduped_list
