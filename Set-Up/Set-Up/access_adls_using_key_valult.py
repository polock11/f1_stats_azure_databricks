# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake useing access keys
# MAGIC 1. set the spark cofig fs.azure.account.key
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##Access format
# MAGIC abfss://container_name@storage_account_name.dfs.core.widows.net/folder_path/file_name
# MAGIC
# MAGIC ##Access Using Access Keys
# MAGIC To access using key, have to set spark conf. 
# MAGIC
# MAGIC format --> spark.conf.set("fs.azure.account.key.storage-account.dfs.core.windows.net", 'access_key')
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

formula1_access_key = dbutils.secrets.get(scope='formula1-scope',key='formula1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dlformulaone2024.dfs.core.windows.net",
    formula1_access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformulaone2024.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@dlformulaone2024.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT country, count(1) from tableA  GROUP BY country").show()


# COMMAND ----------

spark.sql("SELECT name, location  from tableA ")
