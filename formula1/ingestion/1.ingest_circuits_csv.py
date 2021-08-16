# Databricks notebook source
# MAGIC %md 
# MAGIC # Ingest Circuits csv file

# COMMAND ----------

# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Importing ...

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DoubleType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_data_source", "Ergast")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("v_file_date", "2021-03-21")
file_dt = dbutils.widgets.get("v_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up source details

# COMMAND ----------

source_path  = f"{raw_folder_path}{file_dt}/circuits.csv"
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


audit_df = renamed_cols_df.transform(audit_columns).transform(add_data_source(data_source)).transform(file_date(file_dt))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Target data details

# COMMAND ----------

target_path = f"{processed_folder_path}circuits"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

audit_df.write.format(target_format).mode(target_write_mode).save(target_path)
