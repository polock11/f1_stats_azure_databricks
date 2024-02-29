# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists f1_presentation;
# MAGIC create database f1_presentation;

# COMMAND ----------

constructor_standings = spark.read.parquet('abfss://presentation@dlformulaone2024.dfs.core.windows.net/constructor_standings')
driver_standings = spark.read.parquet('abfss://presentation@dlformulaone2024.dfs.core.windows.net/driver_standings')
race_results = spark.read.parquet('abfss://presentation@dlformulaone2024.dfs.core.windows.net/race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_presentation.constructor_standings;
# MAGIC drop table if exists f1_presentation.driver_standings;
# MAGIC drop table if exists f1_presentation.race_results;

# COMMAND ----------

constructor_standings.write.format('parquet').saveAsTable('f1_presentation.constructor_standings')
driver_standings.write.format('parquet').saveAsTable('f1_presentation.driver_standings')
race_results.write.format('parquet').saveAsTable('f1_presentation.race_results')