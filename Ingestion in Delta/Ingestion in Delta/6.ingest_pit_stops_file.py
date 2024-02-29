# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                      ])

# COMMAND ----------

pit_stops_df = spark.read\
    .schema(pit_stops_schema)\
    .option('multiLine', True)\
    .json(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn('data_source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))\
    .withColumn('update_date', current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.pit_stops")):

  r = DeltaTable.forPath(spark,'abfss://processed@dlformulaone2024.dfs.core.windows.net/pit_stops')
  
  r.alias('tgt')\
    .merge(final_df.alias('upd'),
           'tgt.race_id = upd.race_id and tgt.driver_id = upd.driver_id and tgt.stop = upd.stop')\
    .whenNotMatchedInsert(values=
                          {
                            'race_id' : 'upd.race_id',
                            'driver_id' : 'upd.driver_id',
                            'stop': 'upd.stop',
                            'lap' : 'upd.lap',
                            'time' : 'upd.time',
                            'duration' : 'upd.duration',
                            'milliseconds' : 'upd.milliseconds',
                            'file_date' : 'upd.file_date',
                            'ingestion_date' : 'upd.ingestion_date',
                            'data_source' : 'upd.data_source'
                          })\
    .execute()
else:
  final_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id, count(1) from f1_processed.pit_stops
# MAGIC -- group by race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from f1_processed.pit_stops
# MAGIC -- where race_id = 1052

# COMMAND ----------

dbutils.notebook.exit('Success')