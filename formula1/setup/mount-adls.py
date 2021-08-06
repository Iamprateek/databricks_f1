# Databricks notebook source
storage_account_name = "saadlspsd"
client_id = dbutils.secrets.get(scope = 'f1-scope', key='f1-clientid') # service procipal app registration
tenant_id  = dbutils.secrets.get(scope = 'f1-scope', key='f1-tenantid') # service procipal app registration 
client_secret = dbutils.secrets.get(scope = 'f1-scope', key='f1-clientsecret') # created with role assignment on adls storage

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type":"OAuth" ,
  "fs.azure.account.oauth.provider.type" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" : f"{client_id}",
  "fs.azure.account.oauth2.client.secret" : f"{client_secret}",
  "fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{tenant_id}/oauth2/token" 
}

# COMMAND ----------

def mount_container(container_name, storage_account_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}", 
    extra_configs = configs
  )

# COMMAND ----------

container_raw = "raw"
container_processed = "processed"

# COMMAND ----------

def unmount_container(container_name, storage_account_name):
  dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

# COMMAND ----------

# unmount_container(container_raw, storage_account_name)

# COMMAND ----------

mount_container(container_raw, storage_account_name)

# COMMAND ----------

# dbutils.fs.ls("/mnt/saadlspsd/data")
# dbutils.fs.mounts()

# COMMAND ----------

# unmount_container(container_processed, storage_account_name)

# COMMAND ----------

mount_container(container_processed, storage_account_name)
