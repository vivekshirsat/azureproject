# Databricks notebook source
def pre_schema(location):
    try:
        df = spark.read.format('delta').load(f'{location}').limit(1)
        schema = ""
        for i in df.dtypes: 
            schema = schema + i[0] + " " + i[1] + ","
        return schema [0:-1]
    except Exception as err:
        print("Error Occurred", str(err))

# COMMAND ----------

# MAGIC %py
# MAGIC def f_delta_cleansed_load(table_name, location, database):
# MAGIC     try:
# MAGIC         schema = pre_schema(f'{location}')
# MAGIC         spark.sql(f""" DROP TABLE IF EXISTS {database}.{table_name}""")
# MAGIC         spark.sql(f"""
# MAGIC         create table if not exists {database}.{table_name}
# MAGIC         ({schema})
# MAGIC         using delta 
# MAGIC         location '{location}' 
# MAGIC         """)
# MAGIC     except Exception as err:
# MAGIC         print("Error Occurred",str(err))

# COMMAND ----------



# COMMAND ----------

# MAGIC %py
# MAGIC def f_count_check(database, operation_type,table_name, number_diff):
# MAGIC         spark.sql(f""" DESC HISTORY {database}.{table_name} """).createOrReplaceTempView("Table_count")
# MAGIC         count_current = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version = (select max(version) from Table_count where 
# MAGIC         trim(lower(operation)) = lower('{operation_type}'))""")
# MAGIC         if (count_current.first() is None):
# MAGIC             final_count_current = 0
# MAGIC         else:
# MAGIC             final_count_current = int(count_current.first().numOutputRows) 
# MAGIC         count_previous = spark.sql(f"""select operationMetrics.numOutputRows from Table_count where version < (select version from Table_count where 
# MAGIC         lower(trim(operation)) = lower('{operation_type}') order by version desc limit 1)""")
# MAGIC         if (count_previous.first() is None):
# MAGIC             final_count_previous = 0
# MAGIC         else:
# MAGIC             final_count_previous = int(count_previous.first().numOutputRows) 
# MAGIC         if ((final_count_current - final_count_previous) > number_diff):
# MAGIC             raise Exception("difference is huge", table_name)
# MAGIC         else:
# MAGIC             pass
# MAGIC     

# COMMAND ----------


