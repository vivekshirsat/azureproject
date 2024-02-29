# Databricks notebook source
# MAGIC %run
# MAGIC /connection_folder/Utility

# COMMAND ----------

list_table_info = [('STREAMING UPDATE', 'Plane',100),('STREAMING UPDATE', 'Airport',100),('STREAMING UPDATE', 'Cancellation',100),('STREAMING UPDATE', 'Flight',100),('STREAMING UPDATE', 'Unique_Carriers',100),('write', 'Airlines',100)]

for i in list_table_info:
    f_count_check('azure_project',i[0],i[1],i[2])

# COMMAND ----------


