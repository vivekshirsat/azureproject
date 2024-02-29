# Databricks notebook source
# MAGIC %run
# MAGIC /azure_project/connection_folder/Utility

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.dtypes

# COMMAND ----------

df1 = spark.read.format("json").load('/mnt/raw_datalake/airlines')

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = df1.select(explode("response"),"Date_Part")

df_final = df2.select("col.*","Date_Part")


# COMMAND ----------

df_final.write.format("delta").mode("overwrite").save("/mnt/cleansed_datalake/Airlines")

# COMMAND ----------


f_delta_cleansed_load('Airlines' , '/mnt/cleansed_datalake/Airlines' , schema , 'azure_project')

# COMMAND ----------


