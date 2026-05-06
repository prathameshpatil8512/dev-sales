                                        🚀 Enterprise Sales Analytics Engine (Azure & Databricks)

An enterprise-grade, End-to-End ELT data pipeline built on Microsoft Azure. This repository demonstrates modern data engineering practices using the Medallion Architecture, integrating Azure Data Lake Storage (ADLS Gen2), Azure Data Factory (ADF), Azure Databricks, PySpark, Delta Lake.
------------------------------------------------------------------------------------------------------------------------------------
                                        🧩 Architecture Overview

This project implements the Medallion Architecture to incrementally transform raw data into business-ready, analytical datasets.

Raw CSV/Data ──> [ADF / ADLS Gen2] ──> Bronze ──> Silver ──> Gold ──> Power BI
------------------------------------------------------------------------------------------------------------------------------------
                                        Data Flow Stages
Bronze Layer (Ingestion): Stores raw ingestion files (e.g., CSV sales data). Enriches data with ingestion timestamps and source filenames, and skips duplicate processing.

Silver Layer (Transformation): Cleans and enforces schema. Removes duplicate order records, standardizes data types, and saves to optimized Delta tables.

Gold Layer (Enrichment): Applies Slowly Changing Dimensions (SCD Type-2) on customers and joins data to create fact tables.
------------------------------------------------------------------------------------------------------------------------------------
                                        🛠️ Tech Stack
Data Storage: Azure Data Lake Storage (ADLS) Gen2 (adlsdevsales001, adlsdevsales002).

Orchestration: Azure Data Factory (ADF) - adfdevsales001, incorporating pipeline parameters and triggers.

Processing Engine: Azure Databricks (Runtime 13.3 LTS, Photon-accelerated, node Standard_D4ds_v5).

Data Lake Format: Delta Lake.

Metadata & Secrets: Azure Key Vault-backed secret scopes (sales-secret-scope).
------------------------------------------------------------------------------------------------------------------------------------
                                        🔄 Data Pipeline Workflow
Ingestion & Orchestration: An ADF master pipeline checks for raw .csv data uploads and triggers the initial copy and transformation sequence.

Configuration Load: Notebook 02_Config_Connection fetches connection strings and tenant secrets from Azure Key Vault, authenticating via OAuth 2.0.

Data Quality & Ingestion: Ingested files are loaded to the bronze container in Delta format while monitoring for duplicate files using metadata source_file.

Data Cleaning: Silver transformation handles standard business constraints: deduplication on keys and handling date formats using PySpark.

Gold Transformation: Establishes historical analysis tables via Delta Lake merge conditions and writes output records.
------------------------------------------------------------------------------------------------------------------------------------
                                        ⚙️ Key Features
Metadata-Driven Ingestion: Automatically captures input filename and ingestion timestamp to ensure pipeline immutability.

Incremental Updates (Upserts): Uses DeltaTable merge operations in Silver and Gold transformations to prevent redundant data reprocessing.

SCD Type 2 Implementation: Handles changing dimensions for customer address updates over time.

Secret Management: Programmatic retrieval of Azure Key Vault secrets, securing keys and identifiers.
------------------------------------------------------------------------------------------------------------------------------------
                                        📊 Sample Transformations / Logic

# Silver Layer Customer and Sales Splitting logic
clean_df = df.withColumn("OrderDate", to_date(col("OrderDate"), "dd-MM-yyyy")) \
             .dropDuplicates(["OrderID"])

# Gold Layer SCD Type-2 Upsert Example for Customer Table
DeltaTable.forPath(spark, dim_path).alias("t").merge(
    customer_df.alias("s"),
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
------------------------------------------------------------------------------------------------------------------------------------
                                        🧪 Testing & Validation
To ensure pipeline stability, the project includes validation scripts:

Quality Verification: Scripts verify that NOT NULL constraints are met for OrderID and CustomerID.

Data Reconciliation: Run 07_Manual_Testing_Gold_Layer.py to assert that Gold transformation measures and dimensions match downstream expectations.
------------------------------------------------------------------------------------------------------------------------------------
                                        📈 Future Enhancements
Auto Loader: Transition from standard Delta uploads to Databricks Auto Loader for continuous file updates.

CI/CD: Add automated deployments using GitHub Actions and Azure DevOps CI/CD pipelines.

Monitoring: Enable alerting in Azure Monitor for Databricks task failures.
------------------------------------------------------------------------------------------------------------------------------------
👤 Author & Contact
Prathamesh Patil
https://github.com/prathameshpatil8512
------------------------------------------------------------------------------------------------------------------------------------