# Databricks notebook source
races_df = spark.read.format('delta').load('abfss://processed@dlformulaone2024.dfs.core.windows.net/races')\
    .withColumnRenamed('name','race_name')\
    .withColumnRenamed('race_timestamp', 'race_date')

# COMMAND ----------

circuit_df = spark.read.format('delta').load('abfss://processed@dlformulaone2024.dfs.core.windows.net/circuits')\
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

driver_df = spark.read.format('delta').load('abfss://processed@dlformulaone2024.dfs.core.windows.net/drivers')\
    .withColumnRenamed('name','driver_name')\
    .withColumnRenamed('number', 'driver_number')\
    .withColumnRenamed('nationality', 'driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format('delta').load('abfss://processed@dlformulaone2024.dfs.core.windows.net/constructors')\
    .withColumnRenamed('name','constructor_name')

# COMMAND ----------

results_df = spark.read.format('delta').load('abfss://processed@dlformulaone2024.dfs.core.windows.net/results')\
    .withColumnRenamed('time','race_time')\
    .withColumnRenamed('race_id','results_race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join Transform

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_tbl = results_df.join(races_df, races_df.race_id == results_df.results_race_id, 'inner')\
    .join(driver_df, results_df.driver_id == driver_df.driver_id, 'inner')\
    .join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, 'inner')\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id,'inner')\
    .select(races_df.race_id,races_df.race_year, races_df.race_name, races_df.race_date, circuit_df.circuit_location, driver_df.driver_name, driver_df.driver_number, driver_df.driver_nationality,constructors_df.constructor_name,results_df.grid,results_df.fastest_lap, results_df.race_time,results_df.points,results_df.position)\
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

final_tbl.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable("f1_presentation.race_results")

# COMMAND ----------

df = spark.read.format('delta').load('abfss://presentation@dlformulaone2024.dfs.core.windows.net/race_results')

# COMMAND ----------

#display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(1) from f1_presentation.race_results