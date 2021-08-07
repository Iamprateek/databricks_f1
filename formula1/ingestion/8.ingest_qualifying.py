# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat, when

# COMMAND ----------

# MAGIC %fs ls /mnt/saadlspsd/raw/qualifying

# COMMAND ----------

# MAGIC %md
# MAGIC # ingesting results data

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/qualifying"
source_format = "json"
source_schema = 'qualifyId bigint,raceId bigint,driverId bigint,constructorId bigint,position bigint,q1 string,q2 string,q3 string'
source_read_options = {
  "header": "false", "inferSchema":"false", "multiLine":"true"
}

# COMMAND ----------

source_df = spark.\
read.\
format(source_format).\
schema(source_schema).\
options(**source_read_options).\
load(source_path)

# COMMAND ----------

renames = {
  "qualifyId":"qulify_id",
  "constructorId":"constructor_id",
  "driverId": "driver_id" ,
  "raceId" : "race_id"
}

# COMMAND ----------

transformations = [remove_null_string('\\N'), apply_renames(renames), audit_columns]

# COMMAND ----------

final_df = source_df.transform(apply_transformation(transformations))

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/qualifying"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

final_df.write.format(target_format).mode(target_write_mode).save(target_path)
