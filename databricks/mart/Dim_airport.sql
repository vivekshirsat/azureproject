-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.airport; 

-- COMMAND ----------

desc  azure_project.airport;

-- COMMAND ----------

drop table if exists dim_airport

-- COMMAND ----------

create table if not exists dim_airport(
  Code string,
  City string,
  Country string,
  Airport string,
  Date_part date
) using delta location '/mnt/mart_datalake/dim_airport'

-- COMMAND ----------

insert
  overwrite dim_airport
select
  Code,
  City,
  Country,
  Airport,
  Date_Part
from
  azure_project.airport

-- COMMAND ----------


