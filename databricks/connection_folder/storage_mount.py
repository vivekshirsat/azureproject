# Databricks notebook source
dbutils.fs.unmount("/mnt/source_plane/") 

# COMMAND ----------

# MAGIC %scala
# MAGIC val containerName = dbutils.secrets.get(scope="azure-project",key="containername")
# MAGIC val storageAccountName = dbutils.secrets.get(scope="azure-project",key="storageaccountname")
# MAGIC val sas = dbutils.secrets.get(scope="azure-project",key= "sas")
# MAGIC val config  = "fs.azure.sas." + containerName +"."+ storageAccountName + ".blob.core.windows.net"
# MAGIC
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC     source = dbutils.secrets.get(scope="azure-project",key="blob-mount-path"), 
# MAGIC     mountPoint = "/mnt/source_plane/",
# MAGIC     extraConfigs = Map(config -> sas ))

# COMMAND ----------

dbutils.fs.ls("/mnt/source_plane/")

# COMMAND ----------

# MAGIC %md 
# MAGIC below we have set mount point for raw container

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC configs = {"fs.azure.account.auth.type":"OAuth",
# MAGIC             "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC             "fs.azure.account.oauth2.client.id":dbutils.secrets.get(scope="azure-project",key="app-id"),
# MAGIC             "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="azure-project",key="app-secret" ),
# MAGIC             "fs.azure.account.oauth2.client.endpoint":dbutils.secrets.get(scope="azure-project",key="client-refresh-url" )}
# MAGIC
# MAGIC mountPoint = "/mnt/raw_datalake/"
# MAGIC if not any(mount.mountPoint== mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC         source = dbutils.secrets.get(scope="azure-project",key="datalake-raw") ,  
# MAGIC         mount_point = mountPoint,
# MAGIC         extra_configs = configs
# MAGIC     )

# COMMAND ----------

dbutils.fs.ls("/mnt/raw_datalake/")

# COMMAND ----------

# MAGIC %md
# MAGIC below we have set mount point for cleansed container 

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC configs = {"fs.azure.account.auth.type":"OAuth",
# MAGIC             "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC             "fs.azure.account.oauth2.client.id":dbutils.secrets.get(scope="azure-project",key="app-id"),
# MAGIC             "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope="azure-project",key="app-secret" ),
# MAGIC             "fs.azure.account.oauth2.client.endpoint":dbutils.secrets.get(scope="azure-project",key="client-refresh-url" )}
# MAGIC
# MAGIC mountPoint = "/mnt/cleansed_datalake/"
# MAGIC if not any(mount.mountPoint== mountPoint for mount in dbutils.fs.mounts()):
# MAGIC     dbutils.fs.mount(
# MAGIC         source = dbutils.secrets.get(scope="azure-project",key="datalake-cleansed") ,  
# MAGIC         mount_point = mountPoint,
# MAGIC         extra_configs = configs
# MAGIC     )

# COMMAND ----------


