# Databricks notebook source
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DateType
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename',StringType(), True),
                                 StructField('surname', StringType(), True)]
)

# COMMAND ----------

drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), True),
                                   StructField('driverRef', StringType(), True),
                                   StructField('number', IntegerType(), True),
                                   StructField('code', StringType(), True),
                                   StructField('name', name_schema, True),
                                   StructField('dob', DateType(), True),
                                   StructField('nationality', StringType(), True),
                                   StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.schema(drivers_schema).json(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/drivers.json')

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                    .withColumn('ingestion_date', current_timestamp()) \
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
                                    .withColumn('data_source', lit(v_data_source))\
                                    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop('url')

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit('Success')