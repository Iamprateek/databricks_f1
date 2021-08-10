# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat, when

# COMMAND ----------

dbutils.widgets.text("p_data_source", "Ergast")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC # ingesting results data

# COMMAND ----------

source_path  = f"{raw_folder_path}results.json"
source_format = "json"
source_schema = 'constructorId bigint,driverId bigint,fastestLap bigint,fastestLapSpeed string,fastestLapTime string,grid bigint,laps bigint,milliseconds bigint,number bigint,points double,position bigint,positionOrder bigint,positionText string,raceId bigint,rank bigint,resultId bigint,statusId bigint,time string'
source_read_options = {
  "header": "true"
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
  "constructorId": "constructor_id",
  "driverId": "driver_id" ,
  "fastestLap" : "fastest_lap" ,
  "fastestLapSpeed" : "fastest_lap_speed" ,
  "fastestLapTime" : "fastest_lap_time" ,
  "positionOrder" : "position_order" ,
  "positionText" : "position_text" ,
  "resultId" : "result_id" ,
  "statusId" : "status_id" ,
  "raceId" : "race_id"
}

# COMMAND ----------

transformations = [remove_null_string('\\N'), apply_renames(renames), audit_columns, add_data_source(data_source)]

# COMMAND ----------

final_df = source_df.transform(apply_transformation(transformations))

# COMMAND ----------

target_path = f"{processed_folder_path}results"
target_format = "parquet"
target_write_mode = "overwrite"
partition_columns = ["race_id"]

# COMMAND ----------

final_df.repartition('race_id').write.format(target_format).partitionBy(*partition_columns).mode(target_write_mode).save(target_path)
