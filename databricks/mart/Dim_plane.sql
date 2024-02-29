-- Databricks notebook source
use data_mart;

-- COMMAND ----------

select * from azure_project.plane; 

-- COMMAND ----------

desc  azure_project.plane;

-- COMMAND ----------

drop table if exists dim_plane

-- COMMAND ----------

create table if not exists dim_plane(
tailid string,
type string,
manufacturer string,
issue_date date, 
model string,
status string,
aircraft_type string,
engine_type string,
year date
) using delta
location '/mnt/mart_datalake/dim_plane'


-- COMMAND ----------

insert
  overwrite dim_plane
select
  tailid,
  type,
  manufacturer,
  issue_date,
  model,
  status,
  aircraft_type,
  engine_type,
  year
from
  azure_project.plane

-- COMMAND ----------


