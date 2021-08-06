# Databricks notebook source
# MAGIC %md 
# MAGIC # Ingest Circuits csv file

# COMMAND ----------

# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Importing ...

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DoubleType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up source details

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/circuits.csv"
source_format = "csv"
source_schema = StructType(
  [
    StructField("circuitId",IntegerType(),True),
    StructField("circuitRef",StringType(),True),
    StructField("name",StringType(),True),
    StructField("location",StringType(),True),
    StructField("country",StringType(),True),
    StructField("lat",DoubleType(),True),
    StructField("lng",DoubleType(),True),
    StructField("alt",DoubleType(),True),
    StructField("url",StringType(),True)
  ]
)
source_read_options = {
  "header": "true"
}

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Read csv file from dataframe

# COMMAND ----------

source_df = spark.\
read.\
format(source_format).\
schema(source_schema).\
options(**source_read_options).\
load(source_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Select particular columns 

# COMMAND ----------

cols = source_df.columns

required_columns = map(col, filter(lambda x : x not in {"url"},  cols))

column_pruned_df = source_df.select(*required_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Rename Columns

# COMMAND ----------

renames = {
  "circuitId" : "circuit_id" ,
  "circuitRef" : "circuit_ref" ,
  "lat" : "lattitude" ,
  "lng" : "longitude",
  "alt" : "altitude"
}

applied_renames = apply_renames(renames)
  
renamed_cols_df = column_pruned_df.transform(applied_renames)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Add Audit Column

# COMMAND ----------


audit_df = renamed_cols_df.transform(audit_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Target data details

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/circuits"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

audit_df.write.format(target_format).mode(target_write_mode).save(target_path)

# COMMAND ----------

display(dbutils.fs.ls(target_path))

# COMMAND ----------


