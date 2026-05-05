# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Transformation

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
# SILVER LAYER
# ===============================

from pyspark.sql.functions import *
from delta.tables import DeltaTable
from datetime import datetime

bronze_path = "abfss://bronze@adlsdevsales002.dfs.core.windows.net/delta/sales/"
silver_base = "abfss://silver@adlsdevsales002.dfs.core.windows.net/"

customer_path = silver_base + "customer_clean"
sales_path = silver_base + "sales_clean"
watermark_path = silver_base + "watermark"

# -------------------------------
# WATERMARK INIT
# -------------------------------
if not DeltaTable.isDeltaTable(spark, watermark_path):
    spark.createDataFrame([(datetime(1900,1,1),)], ["last_processed"]) \
        .write.format("delta").save(watermark_path)

last_processed = spark.read.format("delta").load(watermark_path).collect()[0][0]

# -------------------------------
# READ BRONZE
# -------------------------------
bronze_df = spark.read.format("delta").load(bronze_path)

inc_df = bronze_df.filter(col("ingestion_time") > last_processed)

# -------------------------------
# CLEAN DATA
# -------------------------------
clean_df = inc_df \
    .withColumn("OrderDate", to_date("OrderDate", "dd-MM-yyyy")) \
    .dropDuplicates(["OrderID"])

# -------------------------------
# CUSTOMER TABLE
# -------------------------------
customer_df = clean_df.select(
    "CustomerID",
    "Country",
    "City",
    "ingestion_time"
).dropDuplicates(["CustomerID"])

if DeltaTable.isDeltaTable(spark, customer_path):

    DeltaTable.forPath(spark, customer_path).alias("t").merge(
        customer_df.alias("s"),
        "t.CustomerID = s.CustomerID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll().execute()

else:
    customer_df.write.format("delta").save(customer_path)

# -------------------------------
# SALES TABLE
# -------------------------------
sales_df = clean_df.select(
    "OrderID",
    "OrderDate",
    "CustomerID",
    "ProductID",
    "Quantity",
    "TotalAmount",
    "ingestion_time"
)

if DeltaTable.isDeltaTable(spark, sales_path):

    DeltaTable.forPath(spark, sales_path).alias("t").merge(
        sales_df.alias("s"),
        "t.OrderID = s.OrderID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll().execute()

else:
    sales_df.write.format("delta").save(sales_path)

# -------------------------------
# UPDATE WATERMARK
# -------------------------------
new_wm = inc_df.agg(max("ingestion_time")).collect()[0][0]

if new_wm:
    spark.createDataFrame([(new_wm,)], ["last_processed"]) \
        .write.format("delta") \
        .mode("overwrite") \
        .save(watermark_path)
