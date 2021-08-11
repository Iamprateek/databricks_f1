# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

from pyspark.sql.functions import  sum,col, count, lit, current_timestamp, rank,  when
from pyspark.sql.window import Window

# COMMAND ----------

source_path = f"{presentation_folder_path}race_results"
source_format = "parquet"

race_results_df = spark.read.format(source_format).load(source_path)

# COMMAND ----------

grouping_cols = ['race_year', 'team']

# COMMAND ----------

results_df = race_results_df.\
groupBy(*grouping_cols).\
agg(sum('points').alias('total_points'), sum(when(col('position') == lit(1), lit(1)).otherwise(lit(0))).alias('wins'))


# COMMAND ----------

rank_win_spec = Window.partitionBy("race_year").orderBy(col("total_points").desc() , col('wins').desc() )

# COMMAND ----------

ranked_results_df = results_df.withColumn("ranking", rank().over(rank_win_spec))

# COMMAND ----------


target_path = f"{presentation_folder_path}constructor_standings"
target_format = "parquet"
target_mode = "overwrite"
ranked_results_df.write.format(target_format).mode(target_mode).save(target_path)
