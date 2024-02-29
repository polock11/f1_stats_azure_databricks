# Databricks notebook source
formula1_access_key = dbutils.secrets.get(scope='formula1-scope',key='formula1-dl-account-key')
spark.conf.set(
    "fs.azure.account.key.dlformulaone2024.dfs.core.windows.net",
    formula1_access_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformulaone2024.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@dlformulaone2024.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

# COMMAND ----------

df

# COMMAND ----------

df.createOrReplaceTempView("tableA")
spark.sql("SELECT country, count(1) from tableA  GROUP BY country").show()


# COMMAND ----------

spark.sql("SELECT name, location  from tableA ")


# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

sas_token = dbutils.secrets.get(scope="formula1-scope", key='test-lesson-sastkn')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlformulaone2024.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlformulaone2024.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlformulaone2024.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://test-lessons@dlformulaone2024.dfs.core.windows.net"))