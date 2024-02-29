-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.unique_carriers; 

-- COMMAND ----------

desc  azure_project.unique_carriers;

-- COMMAND ----------

drop table if exists dim_unique_carriers

-- COMMAND ----------

create table if not exists dim_unique_carriers(
  Code string,
  Description string,
  Date_part date
) using delta location '/mnt/mart_datalake/dim_unique_carriers'

-- COMMAND ----------

insert
  overwrite dim_unique_carriers
select
  Code,
  Description,
  Date_Part
from
  azure_project.unique_carriers

-- COMMAND ----------


