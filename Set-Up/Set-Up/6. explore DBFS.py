# Databricks notebook source
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/'))

# COMMAND ----------

spark.read.csv('/FileStore/circuits.csv').show()