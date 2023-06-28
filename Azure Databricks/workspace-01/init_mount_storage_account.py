# Databricks notebook source
def mount_container(container_name, mount_point_name):
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvault_scope_01",key="application-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault_scope_01",key="secret01"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="keyvault_scope_01",key="tenant-id-01")+"/oauth2/token"}

    dbutils.fs.mount(
        source = f"abfss://{container_name}@tempstorageacc01.dfs.core.windows.net/",
        mount_point = f"/mnt/{mount_point_name}",
        extra_configs = configs)

# COMMAND ----------

# mount to Storage Account container
mount_container('test-container-01', 'test-container-01')

# COMMAND ----------

# use this to list all mount path on this ADB
dbutils.fs.mounts()

# use this to unmount Storage Account container
dbutils.fs.unmount('/mnt/test-container-01')