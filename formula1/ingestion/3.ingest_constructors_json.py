# Databricks notebook source
# MAGIC %run /formula1/helpers/helper_functions

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC # ingesting constructors data

# COMMAND ----------

source_path  = "/mnt/saadlspsd/raw/constructors.json"
source_format = "json"
source_schema = 'constructorId int,constructorRef string,name string, nationality string,url string'
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

# MAGIC %md
# MAGIC # reject columns

# COMMAND ----------

cols = source_df.columns

required_columns = map(col, filter(lambda x : x not in {"url"},  cols))

column_pruned_df = source_df.select(*required_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC # rename column

# COMMAND ----------

renames = {
  "constructorId" : "constructor_id" ,
  "constructorRef" : "constructor_ref"
}

renamed_cols_df = column_pruned_df.transform(apply_renames(renames))

# COMMAND ----------

# MAGIC %md 
# MAGIC # add audit columns

# COMMAND ----------

audit_df = renamed_cols_df.transform(audit_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC # write to target

# COMMAND ----------

target_path = "/mnt/saadlspsd/processed/constructors"
target_format = "parquet"
target_write_mode = "overwrite"

# COMMAND ----------

audit_df.write.format(target_format).mode(target_write_mode).save(target_path)
