# Databricks notebook source
# MAGIC %md 
# MAGIC ####Notebook Workflow

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

v_data_source = 'Ergast_API'
v_file_date = '2021-04-18'

# COMMAND ----------

v_results_1 = dbutils.notebook.run('1.ingest_circuit.csv_file', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})

if v_results_1 == "Success":
    print('Notebook 1.ingest_circuit.csv_file executed')
    v_results_2 = dbutils.notebook.run('2.ingest_races.csv_file', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
    print('Notebook 2.ingest_races.csv_file executed')
    if v_results_2 == "Success":
        v_results_3 = dbutils.notebook.run('3.ingest_constructor_json', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
        print('Notebook 3.ingest_constructor_json executed')
        if v_results_3 == "Success":
            v_results_4 = dbutils.notebook.run('4.ingest_driver_files', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
            print('Notebook 4.ingest_driver_files executed')
            if v_results_4 == "Success":
                v_results_5 = dbutils.notebook.run('5.ingest_results_json', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
                print('Notebook 5.ingest_results_json executed')
                if v_results_5 == "Success":
                    v_results_6 = dbutils.notebook.run('6.ingest_pit_stops_file', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
                    print('Notebook 6.ingest_pit_stops_file executed')
                    if v_results_6 == "Success":
                        v_results_7 = dbutils.notebook.run('7.ingest_lap_times_file', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
                        print('Notebook 7.ingest_lap_times_file executed')
                        if v_results_7 == "Success":
                            v_results_8 = dbutils.notebook.run('8.qualifying_json_file', 0,{'p_data_source' : v_data_source, 'p_file_date': v_file_date})
                            print('Notebook 8.qualifying_json_file executed')
                                                                                        
