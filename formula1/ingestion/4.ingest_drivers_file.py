# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp, concat


# COMMAND ----------

# MAGIC %md
# MAGIC # ingesting drivers data

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/drivers.json"
source_format = "json"
source_schema = 'code string,dob date,driverId bigint,driverRef string,name struct<forename:string,surname:string>,nationality string,number int,url string'
source_read_options = {
  "header": "true", "nullValue":"\\N", "dateFormat": "yyyy-MM-dd"
}

# COMMAND ----------

source_df = spark.\
read.\
format(source_format).\
schema(source_schema).\
options(**source_read_options).\
load(source_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC # dataset specefic trnsformations

# COMMAND ----------

# Fetch data from struct
def construct_name(df):
  return df.withColumn("name", concat(col("name.forename"), lit(' '), col("name.surname")))

# To drop a url
def drop_url(df):
  return df.drop('url')

# COMMAND ----------

renames = {
  "driverId" : "driver_id" ,
  "driverRef" : "driver_ref"
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Applying transformation 

# COMMAND ----------

transformations = [construct_name, apply_renames(renames), drop_url, audit_columns]

# COMMAND ----------



# COMMAND ----------

final_df = source_df.transform(apply_transformation(transformations))

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/drivers"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

final_df.write.format(target_format).mode(target_write_mode).save(target_path)

# COMMAND ----------

