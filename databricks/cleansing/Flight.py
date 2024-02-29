# Databricks notebook source
# MAGIC %run
# MAGIC /azure_project/connection_folder/Utility

# COMMAND ----------

df =  spark.readStream.format("cloudFiles").option("cloudFiles.format",'csv')\
        .option("cloudFiles.schemaLocation","/dbfs/Filestore/tables/schema/flight")\
        .option("cloudFiles.schemaEvolutionMode" , "rescue")\
        .load('/mnt/raw_datalake/flight/')

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df_base = df.selectExpr("to_date(concat_ws('-', Year,Month,Dayofmonth),'yyyy-MM-dd')as Date",
                             "from_unixtime(unix_timestamp(case when DepTime=2400 then 0 else DepTime End,'HHmm'),'HH:mm') as deptime"
                             ,"from_unixtime(unix_timestamp(case when CRSDepTime=2400 then 0 else CRSDepTime End,'HHmm'),'HH:mm') as CRSDepTime"
                             ,"from_unixtime(unix_timestamp(case when ArrTime=2400 then 0 else ArrTime End,'HHmm'),'HH:mm') as ArrTime"
                             ,"from_unixtime(unix_timestamp(case when CRSArrTime=2400 then 0 else CRSArrTime End,'HHmm'),'HH:mm') as CRSArrTime"
                             ,"UniqueCarrier","cast(FlightNum as int) as FlightId","cast(TailNum as int )as TailId","cast(ActualElapsedTime as int) as ActualElapsedTime","cast(CRSElapsedTime as int) as CRSElapsedTime","cast(AirTime as int) as AirTime","cast(ArrDelay as int) as ArrDelay","cast(DepDelay as int) as DepDelay","Origin","Dest","cast(Distance as int) as Distance","cast(TaxiIn as int) as TaxiIn","cast(TaxiOut as int) as TaxiOut","Cancelled","CancellationCode","cast(Diverted as int)as Diverted","cast(CarrierDelay as int) as CarrierDelay","cast(WeatherDelay as int) as WeatherDelay","cast(NASDelay as int) as NASDelay","cast(SecurityDelay as int) as SecurityDelay","cast(LateAircraftDelay as int) as LateAircraftDelay","to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
                             )




# COMMAND ----------

display(df_base.limit(5))

# COMMAND ----------

dbutils.fs.rm('/dbfs/FileStore/tables/checkpointLocation/Flight', True)

# COMMAND ----------


df_base.writeStream.trigger(once=True)\
    .format("delta")\
        .option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Flight")\
            .start("/mnt/cleansed_datalake/Flight")

# COMMAND ----------


f_delta_cleansed_load('Flight' , '/mnt/cleansed_datalake/Flight' ,   'azure_project')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azure_project.Flight;

# COMMAND ----------


