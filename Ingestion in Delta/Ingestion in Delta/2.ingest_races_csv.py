# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import current_timestamp, to_timestamp,concat, lit, col

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

#defin spark schema
# inferSchema increase the spark job which is computationaly costly.

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

races_df = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/races.csv')

# COMMAND ----------

#.lit() for adding similar valued column as a new row. ex df.withColumn('colNamne' lit('col val'))

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                 .withColumn('data_source', lit(v_data_source))\
                                 .withColumn('file_date', lit(v_file_date))


# COMMAND ----------

#selecting necessary columns
races_df_selected = races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'),col('data_source'),col('file_date'))

# COMMAND ----------

races_df_selected.write.mode('overwrite').format('delta').saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('Success')