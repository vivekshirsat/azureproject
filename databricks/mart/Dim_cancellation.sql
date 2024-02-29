-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.cancellation; 

-- COMMAND ----------

desc  azure_project.cancellation;

-- COMMAND ----------

drop table if exists dim_cancellation;

-- COMMAND ----------

create table if not exists dim_cancellation(
  Code string,
  Description string,
  Date_part date
) using delta location '/mnt/mart_datalake/dim_cancellation'

-- COMMAND ----------

insert
  overwrite dim_cancellation
select
  Code,
  Description,
  Date_Part
from
  azure_project.cancellation

-- COMMAND ----------


