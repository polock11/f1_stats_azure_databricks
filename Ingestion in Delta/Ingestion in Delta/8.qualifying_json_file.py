# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[
                                      StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),                                      
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),                                      
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                      ])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option('multiLine', True)\
    .json(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/qualifying')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('qualifyId', 'qualify_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.qualifying")):

  r = DeltaTable.forPath(spark,'abfss://processed@dlformulaone2024.dfs.core.windows.net/qualifying')
  
  r.alias('tgt')\
    .merge(final_df.alias('upd'),
           'tgt.qualify_id = upd.qualify_id and tgt.race_id = upd.race_id')\
    .whenNotMatchedInsert(values=
                          { 
                           'qualify_id' : 'upd.qualify_id',
                            'race_id' : 'upd.race_id',
                            'driver_id' : 'upd.driver_id',
                            'constructor_id' : 'upd.constructor_id',  
                            'number' : 'upd.number',
                            'position' : 'upd.position',
                            'q1' : 'upd.q1',
                            'q2' : 'upd.q2',
                            'q3' : 'upd.q3',
                            'file_date' : 'upd.file_date',
                            'ingestion_date' : 'upd.ingestion_date',
                            'data_source' : 'upd.data_source'
                          })\
    .execute()
else:
  final_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id, count(1) from f1_processed.qualifying
# MAGIC -- group by race_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from f1_processed.qualifying
# MAGIC -- where race_id = 1053

# COMMAND ----------

dbutils.notebook.exit('Success')