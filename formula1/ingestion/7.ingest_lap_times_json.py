# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat, when

# COMMAND ----------

# MAGIC %fs ls /mnt/saadlspsd/raw/lap_times

# COMMAND ----------

# MAGIC %md
# MAGIC # ingesting results data

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/lap_times"
source_format = "csv"
source_schema = 'raceId bigint,driverId bigint,lap bigint,position bigint,time string, millisecond bigint'
source_read_options = {
  "header": "false", "inferSchema":"true"
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
  "driverId": "driver_id" ,
  "raceId" : "race_id"
}

# COMMAND ----------

transformations = [remove_null_string('\\N'), apply_renames(renames), audit_columns]

# COMMAND ----------

final_df = source_df.transform(apply_transformation(transformations))

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/lap_times"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

final_df.write.format(target_format).mode(target_write_mode).save(target_path)
