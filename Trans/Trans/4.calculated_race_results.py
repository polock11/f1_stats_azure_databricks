# Databricks notebook source
# MAGIC %sql
# MAGIC use f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_presentation.calculated_race_results;
# MAGIC create table f1_presentation.calculated_race_results
# MAGIC using delta
# MAGIC as
# MAGIC select races.race_year,
# MAGIC        constructors.name as team_name,
# MAGIC        drivers.name as driver_name,
# MAGIC        results.position,
# MAGIC        results.points,
# MAGIC        11 - results.position as calculated_points
# MAGIC     from results
# MAGIC     join drivers on (results.driver_id = drivers.driver_id)
# MAGIC     join constructors on (results.constructor_id = constructors.constructor_id)
# MAGIC     join races on (results.race_id = races.race_id)
# MAGIC     where results.position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(1) from f1_presentation.calculated_race_results