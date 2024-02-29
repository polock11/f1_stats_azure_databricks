# Databricks notebook source
dbutils.widgets.text('p_data_source','')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

constructor_schema = 'resultId INT, raceId INT, driverId INT, constructorId INT, number INT, grid INT, position INT, positionText STRING, positionOrder INT ,points FLOAT, laps INT, time STRING, milliseconds INT, fastestLap INT, rank INT, fastestLapTime STRING, fastestLapSpeed STRING, statusId INT'

# COMMAND ----------

results_df = spark.read \
                  .schema(constructor_schema) \
                  .json(f'abfss://raw@dlformulaone2024.dfs.core.windows.net/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ####rename columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

results_renamed_df = results_df.withColumnRenamed('resultId', 'result_id') \
                                .withColumnRenamed('raceId', 'race_id')\
                                .withColumnRenamed('driverId','driver_id')\
                                .withColumnRenamed('constructorId','constructor_id')\
                                .withColumnRenamed('positionText','position_text')\
                                .withColumnRenamed('fastestLap','fastest_lap')\
                                .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                                .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                                .withColumnRenamed('positionOrder','position_order')\
                                .withColumn('ingestion_time', current_timestamp())\
                                .withColumn('data_source', lit(v_data_source))\
                                .withColumn('file_date', lit(v_file_date))\
                                .withColumn('update_date',current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.drop('statusId')

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):

  r = DeltaTable.forPath(spark,'abfss://processed@dlformulaone2024.dfs.core.windows.net/results')

  r.alias('tgt').merge(
    results_final_df.alias('upd'),
    'tgt.result_id = upd.result_id and tgt.race_id = upd.race_id'
  )\
  .whenMatchedUpdate(set = {
                          'race_id' : 'upd.race_id',
                          'driver_id' : 'upd.driver_id',
                          'constructor_id' : 'upd.constructor_id',
                          'position_text' : 'upd.position_text',
                          'fastest_lap' : 'upd.fastest_lap',
                          'fastest_lap_time' : 'upd.fastest_lap_time',
                          'fastest_lap_speed' : 'upd.fastest_lap_speed',
                          'position_order' : 'upd.position_order',
                          'ingestion_time' : 'upd.ingestion_time',
                          'data_source' : 'upd.data_source',
                          'file_date' : 'upd.file_date',
                          'number' : 'upd.number',
                          'grid' : 'upd.grid',
                          'position' : 'upd.position',
                          'points': 'upd.points',
                          'laps': 'upd.laps',
                          'time': 'upd.time',
                          'milliseconds' : 'upd.milliseconds',
                          'rank' : 'upd.rank',
                          "update_date" : "current_timestamp()"
                          })\
  .whenNotMatchedInsert(values=
                        {
                          'result_id' : 'upd.result_id',
                          'race_id' : 'upd.race_id',
                          'driver_id' : 'upd.driver_id',
                          'constructor_id' : 'upd.constructor_id',
                          'position_text' : 'upd.position_text',
                          'fastest_lap' : 'upd.fastest_lap',
                          'fastest_lap_time' : 'upd.fastest_lap_time',
                          'fastest_lap_speed' : 'upd.fastest_lap_speed',
                          'position_order' : 'upd.position_order',
                          'ingestion_time' : 'upd.ingestion_time',
                          'data_source' : 'upd.data_source',
                          'file_date' : 'upd.file_date',
                          'number' : 'upd.number',
                          'grid' : 'upd.grid',
                          'position' : 'upd.position',
                          'points': 'upd.points',
                          'laps': 'upd.laps',
                          'time': 'upd.time',
                          'milliseconds' : 'upd.milliseconds',
                          'rank' : 'upd.rank'
                        })\
  .execute()
else:
    results_final_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id, count(1) from f1_processed.results
# MAGIC -- group by race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(1) from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select race_id,driver_id ,count(1) from f1_processed.results
# MAGIC -- group by race_id,driver_id
# MAGIC -- having count(1) > 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(1) from f1_processed.results