# Databricks notebook source
# MAGIC %run
# MAGIC /azure_project/connection_folder/Utility

# COMMAND ----------

df =  spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
        .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/PLANE")\
        .load('/mnt/raw_datalake/PLANE/')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/PLANE', True)

# COMMAND ----------

df_base =  df.selectExpr("tailnum as tailid","type","manufacturer","to_date(issue_date) as issue_date","model","status","aircraft_type","engine_type",
                         "cast('year' as int) as year","to_date('Date_Part','yyyy-MM-dd') as Date_Part")

df_base.writeStream.trigger(once=True)\
    .format("delta")\
        .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Plane")\
            .start("/mnt/cleansed_datalake/Plane")

# COMMAND ----------


f_delta_cleansed_load('Plane' , '/mnt/cleansed_datalake/Plane' , schema , 'azure_project')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azure_project.plane;

# COMMAND ----------


