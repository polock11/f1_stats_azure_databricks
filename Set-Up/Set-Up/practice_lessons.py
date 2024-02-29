# Databricks notebook source
dbutils.secrets().help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

access_key = dbutils.secrets.get(scope='formula1-scope',key='formula1-dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dlformulaone2024.dfs.core.windows.net",
    access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://test-lessons@dlformulaone2024.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv('abfss://test-lessons@dlformulaone2024.dfs.core.windows.net/ecommerce.csv', header=True)
df.show()

# COMMAND ----------

sas_token = dbutils.secrets.get(scope='formula1-scope',key='test-lesson-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlformulaone2024.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlformulaone2024.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlformulaone2024.dfs.core.windows.net", sas_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://test-lessons@dlformulaone2024.dfs.core.windows.net"))