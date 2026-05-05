# Databricks notebook source
# MAGIC %md
# MAGIC ### Establish & Test Connection

# COMMAND ----------

# ===============================
# CONFIGURATION
# ===============================

storage_account = "adlsdevsales002"
tenant_id = dbutils.secrets.get(scope="sales-secret-scope", key="tenantid")
client_id = dbutils.secrets.get(scope="sales-secret-scope", key="clientid")
client_secrets = dbutils.secrets.get(scope="sales-secret-scope", key="clientsecretes")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")

spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)

spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
               client_secrets)

spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# ===============================
# TEST CONNECTION
# ===============================

dbutils.fs.ls("abfss://bronze@adlsdevsales002.dfs.core.windows.net/")

