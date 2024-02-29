# Databricks notebook source
'''
    re_arrange_partition_column function takes dataframe and name of the partition column.
    It returns a dataframe that is rearranged by setting the partition column at the last
    of the dataframe. 
'''

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

"""
    overwrite_partition function overwrites the new data for as on data in the re run.
    it rearranges the dataframe column first. Then it sets the overwrite conf to dynamic 
    to overwrite only the new data for a new date.

    for the cut-off(historic) data it hits the else block. it creates a manged table with db and table
    name and write it as a parquet file on the datalake. 

    for new date it hits the if block and adds or overwrited new data.
"""

def overwrite_partition(input_df,db_name,table_name,partition_column):
    output_df = re_arrange_partition_column(input_df,partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")