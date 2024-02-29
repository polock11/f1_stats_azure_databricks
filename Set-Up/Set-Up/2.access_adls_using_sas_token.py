# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake useing access keys
# MAGIC 1. set the spark cofig SAS Token
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##Access format
# MAGIC abfss://container_name@storage_account_name.dfs.core.widows.net/folder_path/file_name
# MAGIC
# MAGIC ##Access Using SAS tokens
# MAGIC To access using key, have to set spark conf. 
# MAGIC
# MAGIC spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
# MAGIC
# MAGIC spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC
# MAGIC spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net",<sas-token-key>)
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

sas_token = dbutils.secrets.get(scope='formula1-scope', key='formula-1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlformulaone2024.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlformulaone2024.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlformulaone2024.dfs.core.windows.net", sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformulaone2024.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@dlformulaone2024.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT country, count(1) from tableA  GROUP BY country").show()
