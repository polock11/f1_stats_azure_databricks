# Databricks notebook source
race_results = spark.read.format('delta').load('abfss://presentation@dlformulaone2024.dfs.core.windows.net/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, desc, count, when, col

# COMMAND ----------

grouped_df = race_results.groupBy('race_year','constructor_name')\
.agg(sum('points').alias('total_points'),
     count(when(col('position') == 1, True)).alias('wins'))\
         .withColumnRenamed('constructor_name','team')


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

consRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
ranked_df = grouped_df.withColumn('rank', rank().over(consRankSpec))

# COMMAND ----------

ranked_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable("f1_presentation.constructor_standings")