# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col, count, lit, current_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC # races DF

# COMMAND ----------

races_processed_details = {
  "format":"parquet" ,
  "folder_name":"races"
}
races_required_cols = ["year", "name", "date", "race_id", "circuit_id"]

prepare_columns = map(lambda x : col(x).alias(f'race_{x}' if 'race' not in x else x), races_required_cols)

racesDF = spark.read.\
          format(races_processed_details["format"]).\
option("inferSchema", "false").\
          load(f"{processed_folder_path}"+races_processed_details["folder_name"]).\
select(*prepare_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Circuits Df

# COMMAND ----------

circuits_meta = {
  "format":"parquet" ,
  "folder_name":"circuits"
}
###################
circuits_required_cols = ["location", "circuit_id"]

circuits_prepare_columns = map(lambda x : col(x).alias(f'circuit_{x}' if 'circuit' not in x else x), circuits_required_cols)
##################
circuitsDF = spark.read.\
          format(circuits_meta["format"]).\
option("inferSchema", "false").\
          load(f"{processed_folder_path}"+circuits_meta["folder_name"])\
.select(*circuits_prepare_columns)

# COMMAND ----------

# MAGIC %md 
# MAGIC # DriversDF

# COMMAND ----------

drivers_meta = {
  "format":"parquet" ,
  "folder_name":"drivers"
}
###################
drivers_required_cols = ["name","number", "nationality", "driver_id"]

drivers_prepare_columns = map(lambda x : col(x).alias(f'driver_{x}' if 'driver' not in x else x), drivers_required_cols)
##################
driversDF = spark.read.\
          format(drivers_meta["format"]).\
option("inferSchema", "false").\
          load(f"{processed_folder_path}"+drivers_meta["folder_name"]).\
select(*drivers_prepare_columns)


# COMMAND ----------

# MAGIC %md 
# MAGIC # Constructor

# COMMAND ----------

constructors_meta = {
  "format":"parquet" ,
  "folder_name":"constructors"
}

###################
constructors_required_cols = ["name as team", "constructor_id"]

##################

constructorsDF = spark.read.\
          format(constructors_meta["format"]).\
option("inferSchema", "false").\
          load(f"{processed_folder_path}"+constructors_meta["folder_name"]).\
selectExpr(*constructors_required_cols)


# COMMAND ----------

# MAGIC %md 
# MAGIC # Results

# COMMAND ----------

results_meta = {
  "format":"parquet" ,
  "folder_name":"results"
}
results_required_cols = ['grid', 'fastest_lap', 'time', 'points', 'constructor_id', 'driver_id', 'race_id', 'result_id', 'position']
resultsDF = spark.read.\
          format(results_meta["format"]).\
          option("inferSchema", "false").\
          load(f"{processed_folder_path}"+results_meta["folder_name"]).\
select(results_required_cols)


# COMMAND ----------

joined_df = resultsDF.join(driversDF, "driver_id", "inner").join(constructorsDF, "constructor_id").join(racesDF, "race_id").join(circuitsDF, circuitsDF["circuit_id" ]== racesDF["race_circuit_id"])

# COMMAND ----------

result_df = joined_df.drop("race_circuit_id").withColumnRenamed("time", "race_time").withColumn("create_date", current_timestamp())

# COMMAND ----------



# COMMAND ----------

target_path = f"{presentation_folder_path}race_results"
target_format = "parquet"
target_write_mode = "overwrite"

result_df.write.format(target_format).mode(target_write_mode).save(target_path)
