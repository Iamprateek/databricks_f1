# Databricks notebook source
display(dbutils.fs.ls("/mnt/saadlspsd"))

# COMMAND ----------

# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

from pyspark.sql.types import StructType, IntegerType, DoubleType, StructField, StringType
from pyspark.sql.functions import col, lit, current_timestamp, concat, to_timestamp

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/races.csv"
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

added_timestamp_df = column_pruned_df.withColumn('race_timestamp', to_timestamp(concat(col("date"), lit(' '), col('time')), 'yyyy-MM-dd hh:mm:ss'))

# COMMAND ----------

audit_df = added_timestamp_df.transform(audit_columns)

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/races"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

audit_df.write.format(target_format).mode(target_write_mode).save(target_path)

# COMMAND ----------

display(spark.read.parquet(target_path))
