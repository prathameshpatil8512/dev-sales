# Databricks notebook source
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

gold_base = "abfss://gold@adlsdevsales002.dfs.core.windows.net/"
dim_path = gold_base + "dim_customer"
fact_path = gold_base + "fact_sales"

display(spark.read.format("delta").load(dim_path))
display(spark.read.format("delta").load(fact_path))

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("sales-secret-scope")

# COMMAND ----------

dbutils.secrets.get(scope="sales-secret-scope", key="clientid")
dbutils.secrets.get(scope="sales-secret-scope", key="clientsecretes")
dbutils.secrets.get(scope="sales-secret-scope", key="tenantid")
