# Databricks notebook source
# MAGIC %run
# MAGIC /azure_project/connection_folder/Utility

# COMMAND ----------

df =  spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
        .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/Airport")\
        .load('/mnt/raw_datalake/Airport/')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC run below code that is removing if any files present in directory then run next code  **note - only when it is throwing error 

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Airport', True)

# COMMAND ----------

df_base =  df.selectExpr("Code", "split(Description,',')[0] as City","split(split(Description,',')[1],':')[0] as Country",
                         "split(split(Description,',')[1],':')[1] as Airport", "to_date(Date_Part,'yyyy-MM-dd') as Date_Part")
display(df_base)

# COMMAND ----------


df_base.writeStream.trigger(once=True)\
    .format("delta")\
        .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Airport")\
            .start("/mnt/cleansed_datalake/Airport")

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed_datalake/Airport')


# COMMAND ----------

df.dtypes

# COMMAND ----------


f_delta_cleansed_load('Airport' , '/mnt/cleansed_datalake/Airport' , schema , 'azure_project')

# COMMAND ----------


