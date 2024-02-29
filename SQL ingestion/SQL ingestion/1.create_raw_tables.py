# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_raw CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;
# MAGIC USE f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating tables for csv files

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.circuits;
# MAGIC create table if not exists f1_raw.circuits(
# MAGIC   circuitId int,
# MAGIC   circuitRef string,
# MAGIC   name string,
# MAGIC   location string,
# MAGIC   country string,
# MAGIC   lat double,
# MAGIC   lng double,
# MAGIC   alt int,
# MAGIC   url string
# MAGIC )
# MAGIC using csv
# MAGIC options (path "abfss://raw@dlformulaone2024.dfs.core.windows.net/circuits.csv", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.races;
# MAGIC create table if not exists f1_raw.races(
# MAGIC   raceId int,
# MAGIC   year int,
# MAGIC   round string,
# MAGIC   circuitId int,
# MAGIC   name string,
# MAGIC   date date,
# MAGIC   time timestamp,
# MAGIC   url string
# MAGIC )
# MAGIC using csv
# MAGIC options (path "abfss://raw@dlformulaone2024.dfs.core.windows.net/races.csv", header true);                                    

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating tables for json files

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.constructors;
# MAGIC create table if not exists f1_raw.constructors(
# MAGIC   constructorId int,
# MAGIC   constructorRef string,
# MAGIC   name string,
# MAGIC   nationality string,
# MAGIC   url string
# MAGIC )
# MAGIC using json
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/constructors.json", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.drivers;
# MAGIC create table if not exists f1_raw.drivers(
# MAGIC   driverId int,
# MAGIC   driverRef string,
# MAGIC   number int,
# MAGIC   code string,
# MAGIC   name struct<forename: string, surname:string>,
# MAGIC   dob date,
# MAGIC   nationality string,
# MAGIC   url string
# MAGIC )
# MAGIC using json
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/drivers.json", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.results;
# MAGIC create table if not exists f1_raw.results(
# MAGIC   resultId int,
# MAGIC   raceId int,
# MAGIC   driverId int,
# MAGIC   constructorId int,
# MAGIC   number int,
# MAGIC   grid int,
# MAGIC   position int,
# MAGIC   positionText string,
# MAGIC   positionOrder int,
# MAGIC   points int,
# MAGIC   laps int,
# MAGIC   time string,
# MAGIC   milliseconds int,
# MAGIC   fastestLap int,
# MAGIC   rank int,
# MAGIC   fastestLapTime string,
# MAGIC   fastestLapSpeed float,
# MAGIC   statusId string
# MAGIC )
# MAGIC using json
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/results.json", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.pit_stops;
# MAGIC create table if not exists f1_raw.pit_stops(
# MAGIC   driverId int,
# MAGIC   duration string,
# MAGIC   lap int,
# MAGIC   milliseconds int,
# MAGIC   raceId int,
# MAGIC   stop int,
# MAGIC   time string
# MAGIC )
# MAGIC using json
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/pit_stops.json", multiline true);

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating tables form splitted files 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.lap_times;
# MAGIC create table if not exists f1_raw.lap_times(
# MAGIC   raceId int,
# MAGIC   driverId int,
# MAGIC   lap int,
# MAGIC   position int,
# MAGIC   time string,
# MAGIC   milliseconds int
# MAGIC )
# MAGIC using csv
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/lap_times", header true);

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_raw.qualifying;
# MAGIC create table if not exists f1_raw.qualifying(
# MAGIC   constructorId int,
# MAGIC   driverId int,
# MAGIC   number int,
# MAGIC   position int,
# MAGIC   q1 string,
# MAGIC   q2 string,
# MAGIC   q3 string,
# MAGIC   qualifyId int,
# MAGIC   raceId int
# MAGIC )
# MAGIC using json
# MAGIC options(path "abfss://raw@dlformulaone2024.dfs.core.windows.net/qualifying", multiline true);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended results
# MAGIC