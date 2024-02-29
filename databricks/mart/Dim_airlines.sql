-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.airlines; 

-- COMMAND ----------

desc  azure_project.airlines;

-- COMMAND ----------

drop table if exists dim_airlines

-- COMMAND ----------

create table if not exists dim_airlines(
  iata_code string,
  icao_code string,
  name string,
  Date_Part date
) using delta location '/mnt/mart_datalake/dim_airlines'

-- COMMAND ----------

insert
  overwrite dim_airlines
select
  iata_code,
  icao_code,
  name,
  Date_Part
from
  azure_project.airlines

-- COMMAND ----------


