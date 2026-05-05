# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Ingestion

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

# ===============================
# BRONZE LAYER
# ===============================

from pyspark.sql.functions import *

raw_path = "abfss://bronze@adlsdevsales002.dfs.core.windows.net/raw/"
bronze_path = "abfss://bronze@adlsdevsales002.dfs.core.windows.net/delta/sales/"

# -------------------------------
# READ RAW FILES
# -------------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(raw_path)

# -------------------------------
# ENRICH METADATA
# -------------------------------
df_enriched = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", regexp_extract(input_file_name(), r'([^/]+$)', 1))

# -------------------------------
# PREVENT DUPLICATE FILE PROCESSING
# -------------------------------
try:
    existing_files = spark.read.format("delta") \
        .load(bronze_path) \
        .select("source_file").distinct()

    df_new = df_enriched.join(existing_files, "source_file", "left_anti")

except:
    df_new = df_enriched

# -------------------------------
# WRITE TO BRONZE
# -------------------------------
df_new.write.format("delta") \
    .mode("append") \
    .save(bronze_path)

