# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_presentation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from calculated_race_results
# MAGIC   group by team_name
# MAGIC   having total_races >= 100
# MAGIC   order by avg_points desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from calculated_race_results
# MAGIC   where race_year between 2010 and 2020
# MAGIC   group by team_name
# MAGIC   having total_races >= 100
# MAGIC   order by avg_points desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select team_name, 
# MAGIC        count(1) as total_races,
# MAGIC        sum(calculated_points) as total_points,
# MAGIC        avg(calculated_points) as avg_points
# MAGIC   from calculated_race_results
# MAGIC   where race_year between 2000 and 2010
# MAGIC   group by team_name
# MAGIC   having total_races >= 100
# MAGIC   order by avg_points desc;