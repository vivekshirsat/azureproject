# Databricks notebook source
# MAGIC %run
# MAGIC /azure_project/connection_folder/Utility

# COMMAND ----------

df =  spark.readStream.format("cloudFiles").option("cloudFiles.format",'parquet')\
        .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/Cancellation")\
        .load('/mnt/raw_datalake/Cancellation/')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC run below code that is removing if any files present in directory then run next code  **note - only when it is throwing error 

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport', True)

# COMMAND ----------

df_base =  df.selectExpr("replace(Code,'\"' , '') as Code"
                         ,"replace(Description,'\"' , '') as Description" 
                         ,"to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
display(df_base)

# COMMAND ----------


df_base.writeStream.trigger(once=True)\
    .format("delta")\
        .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
            .start("/mnt/cleansed_datalake/Cancellation")

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_datalake/Cancellation')


# COMMAND ----------

df.dtypes

# COMMAND ----------


f_delta_cleansed_load('Cancellation' , '/mnt/cleansed_datalake/Cancellation' , schema , 'azure_project')

# COMMAND ----------


