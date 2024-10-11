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



args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
logger.info(" Before Saving Data Frame  ")


def child_struct(nested_df):
    # Creating python list to store dataframe metadata
    list_schema = [((), nested_df)]
    # Creating empty python list for final flattern columns
    flat_columns = []

    while len(list_schema) > 0:
        # Removing latest or recently added item (dataframe schema) and returning into df variable
        parents, df = list_schema.pop()
        flat_cols = [  col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if c[1][:6] != "struct"   ]

        struct_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]

        flat_columns.extend(flat_cols)
        #Reading  nested columns and appending into stack list
        for i in struct_cols:
            projected_df = df.select(i + ".*")
            list_schema.append((parents + (i,), projected_df))
    return nested_df.select(flat_columns)


def parent_array(df):
    array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    while len(array_cols)>0:
        for c in array_cols:
            df = df.withColumn(c,explode_outer(c))
        df = child_struct(df)
        array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    return df

file_name='s3://chello/Json_2_CSV/input/Codat_balanceSheet_4zHQESS7hgOegY2fMyMr3dXn_001_20240914233907.json'
json_df = spark.read.option("multiLine", True).option("mode", "PERMISSIVE").json(file_name)
flatten_data = parent_array(json_df)
flatten_data.show(100)
flatten_data.coalesce(1).write.format("csv").option('header', True).mode("append").save("s3://chello/Json_2_CSV/output/")
