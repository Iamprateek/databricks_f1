# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat, to_timestamp

# COMMAND ----------

dbutils.widgets.text("p_data_source", "Ergast")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

source_path  = f"{raw_folder_path}races.csv"
source_format = "csv"
source_schema = 'raceId int,year int,round int,circuitId int,name string,date string,time string,url string'
source_read_options = {
  "header": "true", "inferSchema" : "true"
}

# COMMAND ----------

source_df = spark.\
read.\
format(source_format).\
schema(source_schema).\
options(**source_read_options).\
load(source_path)

# COMMAND ----------

cols = source_df.columns

required_columns = map(col, filter(lambda x : x not in {"url"},  cols))

column_pruned_df = source_df.select(*required_columns)

# COMMAND ----------

renames = {
  "raceId" : "race_id" ,
  "circuitId" : "circuit_id"
}


renamed_cols_df = column_pruned_df.transform(apply_renames(renames))

# COMMAND ----------

added_timestamp_df = renamed_cols_df.withColumn('race_timestamp', to_timestamp(concat(col("date"), lit(' '), col('time')), 'yyyy-MM-dd hh:mm:ss'))

# COMMAND ----------

audit_df = added_timestamp_df.transform(audit_columns).transform(add_data_source(data_source))

# COMMAND ----------

target_path = f"{processed_folder_path}races"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

audit_df.write.format(target_format).mode(target_write_mode).save(target_path)
