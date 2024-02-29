# Databricks notebook source
race_results = spark.read.format('delta').load('abfss://presentation@dlformulaone2024.dfs.core.windows.net/race_results')

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, desc, count, when, col

# COMMAND ----------

grouped_df = race_results.groupBy('race_year','driver_name','driver_nationality','constructor_name')\
    .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))\
    .orderBy(desc('race_year'), desc('total_points'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
ranked_df = grouped_df.withColumn('rank', rank().over(driverRankSpec))

# COMMAND ----------

#ranked_df.write.mode('overwrite').parquet('abfss://presentation@dlformulaone2024.dfs.core.windows.net/driver_standings')

# COMMAND ----------

ranked_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable("f1_presentation.driver_standings")