# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_processed cascade;
# MAGIC create database f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database f1_processed

# COMMAND ----------

circuits = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/circuits')
races = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/races')
constructors = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/constructors')
drivers = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/drivers')
results = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/results')
pit_stops = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/pit_stops')
lap_times = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/lap_times')
qualifying = spark.read.parquet('abfss://processed@dlformulaone2024.dfs.core.windows.net/qualifying')

# COMMAND ----------

results.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_processed.circuits;
# MAGIC drop table if exists f1_processed.races;
# MAGIC drop table if exists f1_processed.constructors;
# MAGIC drop table if exists f1_processed.drivers;
# MAGIC drop table if exists f1_processed.results;
# MAGIC drop table if exists f1_processed.pit_stops;
# MAGIC drop table if exists f1_processed.lap_times;
# MAGIC drop table if exists f1_processed.qualifying;

# COMMAND ----------

circuits.write.format('parquet').saveAsTable('f1_processed.circuits')
races.write.format('parquet').saveAsTable('f1_processed.races')
constructors.write.format('parquet').saveAsTable('f1_processed.constructors')
drivers.write.format('parquet').saveAsTable('f1_processed.drivers')
results.write.format('parquet').saveAsTable('f1_processed.results')
pit_stops.write.format('parquet').saveAsTable('f1_processed.pit_stops')
lap_times.write.format('parquet').saveAsTable('f1_processed.lap_times')
qualifying.write.format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

# MAGIC %sql
# MAGIC desc database f1_presentation