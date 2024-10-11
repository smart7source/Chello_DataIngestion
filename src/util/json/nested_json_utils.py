from pyspark.sql.types import *
from pyspark.sql.functions import col, explode_outer

def flatten_nested_data(nested_df):
    ##Fetching Complex Datatype Columns from Schema
    field_names = dict([(field.name, field.dataType) for field in nested_df.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    while len(field_names)!=0:
        field_name=list(field_names.keys())[0]

        if type(field_names[field_name]) == StructType:
            extracted_fields = [col(field_name +'.'+ innerColName).alias(innerColName) for innerColName in [ colName.name for colName in field_names[field_name]]]
            nested_df=nested_df.select("*", *extracted_fields).drop(field_name)

        elif type(field_names[field_name]) == ArrayType: ##If we enable the ArrayType in Line 2 & 15, we end up having multiple duplicate records. Each array column value will create new record, which is worst.
            nested_df=nested_df.withColumn(field_name, explode_outer(field_name))

        field_names = dict([(field.name, field.dataType) for field in nested_df.schema.fields if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return nested_df
