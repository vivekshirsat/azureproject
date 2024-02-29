# Databricks notebook source
pip install tabula-py

# COMMAND ----------

# MAGIC %pip install --upgrade tabula-py
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/mnt/source_plane/')

# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_plane') if(i.name.split('.')[1] == 'pdf')]

print(list_files)

# COMMAND ----------

import tabula

from datetime import date

dbutils.fs.mkdirs(f"/mnt/raw_datalake/PLANE/Date_Part={str(date.today())}/")

# COMMAND ----------

tabula.convert_into("/dbfs/mnt/source_plane/PLANE.pdf", f"/dbfs/mnt/raw_datalake/PLANE/Date_Part={str(date.today())}/PLANE.csv", output_format="csv", pages="all")

# COMMAND ----------

# MAGIC %md
# MAGIC if there are multiple pdf file present in the datalake then we have to use below approach 

# COMMAND ----------

# MAGIC %md
# MAGIC idk below method is not working will look later on it

# COMMAND ----------

import tabula
from datetime import date

def f_source_pdf_datalake(source_path, sink_path, output_format, page, file_name):
    try:
        dbutils.fs.mkdirs(f"/{sink_path}{file_name.split('.')[0]}/")
        tabula.convert_into("{source_path}{file_name}",f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format= output_format , pages= page)                     
    except Exception as err:
        print('Error occurred', str(err))


# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_plane') if(i.name.split('.')[1] == 'pdf')]
for i in list_files:
    f_source_pdf_datalake('/dbfs/mnt/source_plane/','/dbfs/mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------



# COMMAND ----------

import tabula
from datetime import date

def f_source_pdf_datalake(source_path, sink_path, output_format, page, file_name):
    try:
        # Define the output directory path
        output_dir = f"/dbfs/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/"

        # Convert PDF to CSV
        tabula.convert_into(f"{source_path}{file_name}", f"{output_dir}{file_name.split('.')[0]}.{output_format}", output_format=output_format, pages=page)

        # Create the output directory
        dbutils.fs.mkdirs(output_dir)

    except Exception as err:
        print('Error occurred', str(err))


# COMMAND ----------

list_files = [(i.name, i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/source_plane') if(i.name.split('.')[1] == 'pdf')]
for i in list_files:
    f_source_pdf_datalake('/dbfs/mnt/source_plane/','/dbfs/mnt/raw_datalake/','csv','all',i[0])

# COMMAND ----------


