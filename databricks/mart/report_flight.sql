-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.flight; 

-- COMMAND ----------

desc  azure_project.flight;

-- COMMAND ----------

select Date, ArrDelay, DepDelay, Origin, Cancelled, CancellationCode, UniqueCarrier, FlightId , TailId, deptime from azure_project.flight

-- COMMAND ----------

drop table if exists report_flight

-- COMMAND ----------

create table if not exists report_flight(
  Date date,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Cancelled string,
  CancellationCode string,
  UniqueCarrier string,
  FlightId int,
  TailId int,
  deptime string
) using delta 
 location '/mnt/mart_datalake/report_flight'

-- COMMAND ----------

insert overwrite report_flight
select  Date ,
  ArrDelay ,
  DepDelay ,
  Origin ,
  Cancelled ,
  CancellationCode ,
  UniqueCarrier ,
  FlightId,
  TailId ,
  deptime 
 from azure_project.flight

-- COMMAND ----------


