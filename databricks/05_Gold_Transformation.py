# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold Transformation

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
# GOLD LAYER
# ===============================

from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import DateType

silver_base = "abfss://silver@adlsdevsales002.dfs.core.windows.net/"
gold_base = "abfss://gold@adlsdevsales002.dfs.core.windows.net/"

customer_clean = spark.read.format("delta").load(silver_base + "customer_clean")
sales_clean = spark.read.format("delta").load(silver_base + "sales_clean")

dim_path = gold_base + "dim_customer"
fact_path = gold_base + "fact_sales"

# -------------------------------
# REMOVE METADATA (IMPORTANT FIX)
# -------------------------------
customer_clean = customer_clean.drop("ingestion_time")
sales_clean = sales_clean.drop("ingestion_time")

# -------------------------------
# DIM CUSTOMER (SCD TYPE 2)
# -------------------------------
if not DeltaTable.isDeltaTable(spark, dim_path):

    customer_clean \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit(None).cast(DateType())) \
        .withColumn("is_current", lit(True)) \
        .write.format("delta").save(dim_path)

else:

    dim = DeltaTable.forPath(spark, dim_path)

    dim.alias("t").merge(
        customer_clean.alias("s"),
        "t.CustomerID = s.CustomerID AND t.is_current = true"
    ).whenMatchedUpdate(
        condition="t.City <> s.City OR t.Country <> s.Country",
        set={
            "end_date": current_date(),
            "is_current": lit(False)
        }
    ).whenNotMatchedInsert(
        values={
            "CustomerID": "s.CustomerID",
            "Country": "s.Country",
            "City": "s.City",
            "start_date": current_date(),
            "end_date": lit(None).cast(DateType()),
            "is_current": lit(True)
        }
    ).execute()

# -------------------------------
# DIM CURRENT CUSTOMER
# -------------------------------
dim_current = spark.read.format("delta") \
    .load(dim_path) \
    .filter("is_current = true") \
    .select("CustomerID")

# -------------------------------
# FACT TABLE BUILD
# -------------------------------
fact_df = sales_clean.join(dim_current, "CustomerID") \
    .select(
        "OrderID",
        "OrderDate",
        "CustomerID",
        "ProductID",
        "Quantity",
        "TotalAmount"
    )

# -------------------------------
# FACT LOAD (MERGE)
# -------------------------------
if DeltaTable.isDeltaTable(spark, fact_path):

    DeltaTable.forPath(spark, fact_path).alias("t").merge(
        fact_df.alias("s"),
        "t.OrderID = s.OrderID"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll().execute()

else:
    fact_df.write.format("delta").save(fact_path)
