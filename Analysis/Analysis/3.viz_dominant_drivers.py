# Databricks notebook source
html = """ <h1 style="color:Black;text-align:center;font-family:Ariel"> Report on Dominant Formula 1 Drivers </h1> """
displayHTML(html)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view v_dominant_drivers 
# MAGIC as
# MAGIC select driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points,
# MAGIC        rank() over(order by avg(calculated_points) desc) as driver_rank 
# MAGIC   from f1_presentation.calculated_race_results 
# MAGIC group by driver_name
# MAGIC having count(1) > 50
# MAGIC order by avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC        race_year,
# MAGIC        driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results 
# MAGIC   where driver_name in (
# MAGIC     select driver_name 
# MAGIC         from v_dominant_drivers 
# MAGIC     where driver_rank <=10
# MAGIC   )
# MAGIC group by race_year, driver_name
# MAGIC order by race_year, avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC        race_year,
# MAGIC        driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results 
# MAGIC   where driver_name in (
# MAGIC     select driver_name 
# MAGIC         from v_dominant_drivers 
# MAGIC     where driver_rank <=10
# MAGIC   )
# MAGIC group by race_year, driver_name
# MAGIC order by race_year, avg_points desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC        race_year,
# MAGIC        driver_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from f1_presentation.calculated_race_results 
# MAGIC   where driver_name in (
# MAGIC     select driver_name 
# MAGIC         from v_dominant_drivers 
# MAGIC     where driver_rank <=10
# MAGIC   )
# MAGIC group by race_year, driver_name
# MAGIC order by race_year, avg_points desc

# COMMAND ----------

# MAGIC %md
# MAGIC dashboard link: https://adb-6480446341130099.19.azuredatabricks.net/?o=6480446341130099#notebook/3378506018578651/dashboard/3378506018578673/present
# MAGIC