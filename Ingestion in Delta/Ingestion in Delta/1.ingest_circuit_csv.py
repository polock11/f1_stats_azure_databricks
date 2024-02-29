# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit,current_timestamp

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

#defin spark schema
# inferSchema increase the spark job which is computationaly costly.

circuit_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True),
])

# COMMAND ----------

circuit_df = spark.read \
    .option('header', True) \
    .schema(circuit_schema) \
    .csv(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/circuits.csv')

# COMMAND ----------

#selecting necessary columns
circuit_df_selected = circuit_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

circuit_renamed_df = circuit_df_selected.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "lattitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude")\
                                        .withColumn('data_source', lit(v_data_source))\
                                        .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

#.lit() for adding similar valued column as a new row. ex df.withColumn('colNamne' lit('col val'))

circuit_final_df = circuit_renamed_df.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

circuit_final_df.write.mode('overwrite').format('delta').saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit('Success')