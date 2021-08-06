# Databricks notebook source
from functools import reduce, partial
from inspect import signature 

# COMMAND ----------

def curry(fnc):
  def inner(arg):
    if len(signature(fnc).parameters) == 1 :
      return fnc(arg)
    return curry(partial(fnc, arg))
  return inner 

# COMMAND ----------

@curry
def apply_renames(renames, df):
  return reduce(lambda acc, val : acc.withColumnRenamed(val, renames[val]), renames, df)

# COMMAND ----------

def construct_schema_string(df_dtype):
  return ','.join(map(lambda x : f"{x[0]} {x[1]}", df_dtype))

# COMMAND ----------

def audit_columns(df):
    return df.withColumn("create_date", current_timestamp()).withColumn("created_by", lit("PSD"))
