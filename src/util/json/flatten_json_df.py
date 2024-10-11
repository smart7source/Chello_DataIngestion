from pyspark.sql.functions import *

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


def flatten_nested_data(df):
    array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    while len(array_cols)>0:
        for c in array_cols:
            df = df.withColumn(c,explode_outer(c))
        df = child_struct(df)
        array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    return df