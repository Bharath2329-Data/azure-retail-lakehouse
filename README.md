# Azure Retail Lakehouse Project

## Objective
Build an end-to-end retail analytics lakehouse using Azure cloud services.

## Architecture
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Databricks
- Bronze / Silver / Gold architecture (Medallion Architecture)

## Status
# Day 1: Environment setup complete
## Objective

Establish a secure, cost-optimized Azure foundation for building a scalable Retail Lakehouse architecture.

 Azure Resources Created

Resource Group: rg-retail-lakehouse-dev

Storage Account: retaillakehousebharath

Storage Type: Azure Data Lake Storage Gen2

Performance Tier: Standard

Redundancy: LRS (Locally Redundant Storage) – cost optimized

Hierarchical Namespace: Enabled (required for ADLS Gen2)

## Data Lake Structure (Medallion Architecture)

Created the following containers:

bronze → Raw data layer

silver → Cleaned & transformed data layer

gold → Business-ready analytics layer

logs → Pipeline and operational logs

This structure enables scalable and modular data processing following industry-standard lakehouse patterns.

### Cost Optimization Strategy

To ensure the project stays under a $20 budget:

Selected LRS redundancy (cheapest option)

Used Standard performance tier

Consolidated all resources inside a single Resource Group

Designed for easy teardown using:

az group delete --name rg-retail-lakehouse-dev

This ensures full environment cleanup to prevent unexpected charges.

### Security & Governance (Foundation)

Storage account secured via Azure RBAC

Public access disabled where possible

Designed to integrate with Managed Identity and Key Vault in future phases

### Git Version Control

Initialized Git repository

Created structured project folders

Added initial README and documentation skeleton

Committed environment setup changes


# Day 2 — Retail Data Ingestion (Bronze Layer)

Create realistic retail datasets and upload them into the Azure Data Lake Bronze layer as raw source data.

## Architecture (Current State)

Retail CSV Files
⬇
Azure Data Lake Storage Gen2
⬇
Bronze Layer (Raw Data)

📂 Datasets Created

The following retail datasets were generated:

### Dataset	Description
customers	Customer master data with loyalty tier
products	Product catalog with categories and pricing
stores	Store location details
orders	Order header information
order_items	Line-level product transactions
payments	Payment details per order

All datasets are stored locally under:

data/sample_raw/
###  Bronze Layer Structure (ADLS)

Uploaded to Azure Data Lake:

bronze/raw/customers/
bronze/raw/products/
bronze/raw/stores/
bronze/raw/orders/
bronze/raw/order_items/
bronze/raw/payments/
Bronze Layer Rules

Raw data only

No transformations

No schema enforcement

Immutable storage

# Day 3 — Azure Data Factory Setup & Bronze Integration
## Goal

Establish Azure Data Factory (ADF) to orchestrate data ingestion from Azure Data Lake Storage Gen2 (ADLS) Bronze layer.

This step introduces orchestration and enterprise-grade data pipeline design.

### Architecture (Updated)

Retail CSV Files
⬇
Azure Data Lake (Bronze Layer)
⬇
Azure Data Factory (Orchestration Layer)

### Azure Resources Created
1️⃣ Azure Data Factory (V2)

Name: adf-retail-lakehouse

Resource Group: rg-retail-lakehouse-bharath

Region: Same as ADLS

System Assigned Managed Identity: Enabled

### Role-Based Access Control (RBAC)

To allow ADF to access ADLS securely:

Assigned Role: Storage Blob Data Contributor

Assigned To: Azure Data Factory Managed Identity

This enables secure, password-less authentication using Azure RBAC.

### Linked Service Configuration

Created Linked Service:

Name: LS_ADLS_RETAIL

Type: Azure Data Lake Storage Gen2

Authentication: Managed Identity

Connection Test: Successful

This establishes secure connectivity between ADF and ADLS.

### Datasets Created (Bronze Layer)

The following datasets were configured in ADF:

Dataset Name	Source Path
DS_Bronze_Customers_CSV	bronze/raw/customers/customers.csv
DS_Bronze_Products_CSV	bronze/raw/products/products.csv
DS_Bronze_Stores_CSV	bronze/raw/stores/stores.csv
DS_Bronze_Orders_CSV	bronze/raw/orders/orders.csv
DS_Bronze_OrderItems_CSV	bronze/raw/order_items/order_items.csv
DS_Bronze_Payments_CSV	bronze/raw/payments/payments.csv

Each dataset:

Uses DelimitedText format

Reads from ADLS Gen2

Schema imported from source

Header row enabled

 What This Demonstrates

✔ Azure Data Factory configuration
✔ Managed Identity authentication
✔ Secure RBAC role assignment
✔ Enterprise data connectivity
✔ Dataset design for structured ingestion

This step transitions the project from static storage to orchestrated data engineering.

### ADF Configuration Exported

ADF ARM Template exported and stored in repository:

adf/arm_template/
  ARMTemplateForFactory.json
  ARMTemplateParametersForFactory.json

This enables Infrastructure-as-Code capability and reproducibility.

# Day 4 — Parameterized ADF Pipeline (Reusable Bronze Ingestion)
 Goal

Build a reusable Azure Data Factory (ADF) ingestion pattern using a single parameterized Copy pipeline to load any retail CSV file into the Bronze (raw) layer.

This implements an enterprise-style approach: build once, reuse for all datasets.

## Architecture (Updated)

Retail CSV Files
⬇
ADF Parameterized Pipeline (Copy Activity)
⬇
ADLS Gen2 — Bronze Layer (Raw)

## What Was Built
### 1️) Parameterized Datasets (Source + Sink)

Created two ADLS Gen2 DelimitedText datasets with dynamic path support:

DS_ADLS_CSV_SRC_PARAM

DS_ADLS_CSV_SINK_PARAM

Each dataset uses parameters:

pContainer

pFolder

pFileName

This allows reading/writing files without creating separate datasets per table.

### 2️) Reusable Pipeline

Created a parameterized pipeline:

PL_CopyToBronze_Parameterized

Pipeline parameters:

pContainer (example: bronze)

pSourceFolder (example: raw/customers)

pFileName (example: customers.csv)

pTargetFolder (example: raw/customers)

Copy Activity:

Copy_CSV_To_Bronze

This pipeline supports ingestion for all retail datasets by changing parameter values.

### Validation Performed

Executed a Debug run for customers.csv

Confirmed successful copy execution in ADF Monitor

Verified file presence/updates in ADLS Bronze folder

### ADF Configuration Saved to Git

Exported ADF ARM template to track configuration in source control:

adf/arm_template/
  ARMTemplateForFactory.json
  ARMTemplateParametersForFactory.json
### Technologies Used

Azure Data Factory (Copy Activity, Parameterization)

ADLS Gen2 (Bronze Layer)

Managed Identity (RBAC)

GitHub + PowerShell

# Day 5 — Bronze to Silver Transformation (Azure Databricks + Delta Lake)
### Goal

Transform raw retail data from the Bronze layer into a cleaned and structured Silver layer using Azure Databricks and PySpark.

The Silver layer applies basic data engineering transformations to prepare the data for analytics.

### Architecture (Updated)

Retail CSV Files
⬇
Azure Data Factory (Ingestion Pipeline)
⬇
Azure Data Lake Storage — Bronze (Raw Data)
⬇
Azure Databricks (PySpark Transformations)
⬇
Azure Data Lake Storage — Silver (Clean Delta Tables)

## Azure Databricks Setup

A minimal-cost Databricks cluster was created for data processing.

Setting	Value
Cluster Name	retail-cluster
Runtime	Databricks Runtime LTS
Worker Nodes	1
Auto Termination	15 minutes

Auto-termination ensures compute costs remain minimal.

## Databricks to ADLS Integration

Databricks was connected to Azure Data Lake Storage Gen2 using OAuth authentication.

Bronze and Silver storage paths:

Bronze Path
abfss://bronze@retaillakehousebharath.dfs.core.windows.net/

Silver Path
abfss://silver@retaillakehousebharath.dfs.core.windows.net/clean/
### Datasets Processed

The following Bronze datasets were transformed:

Dataset	Description
customers	Customer information and loyalty tiers
products	Product catalog and pricing
stores	Store location details
orders	Order transactions
order_items	Product line items per order
payments	Payment details for orders
#### Transformations Applied

Basic data engineering transformations were implemented:

• Removed duplicate records
• Converted date columns to proper date format
• Cast numeric fields to correct data types
• Added ingestion timestamp (ingest_ts)
• Standardized schema structure

Example transformation:

customers_silver = (
customers_df
.dropDuplicates(["customer_id"])
.withColumn("signup_date", to_date("signup_date"))
.withColumn("ingest_ts", current_timestamp())
)
### Silver Layer Storage (Delta Format)

Processed datasets were written as Delta tables into the Silver layer.

silver/clean/customers
silver/clean/products
silver/clean/stores
silver/clean/orders
silver/clean/order_items
silver/clean/payments

Using Delta Lake provides:

ACID transactions

Schema enforcement

Faster queries

Reliable data versioning

### Data Validation

After transformations:

Row counts were verified

Schema structure confirmed

Data preview validated in Databricks notebook

### Technologies Used

• Azure Databricks
• PySpark
• Delta Lake
• Azure Data Lake Storage Gen2
• Azure Data Factory
• GitHub

# Day 6 — Gold Layer Analytics (Business Data Marts)
### Goal

Transform curated Silver Delta tables into Gold analytics tables that provide business-ready insights for reporting and dashboards.

The Gold layer contains aggregated, analytics-focused datasets used by business teams.

### Architecture (Updated)

Retail CSV Files
⬇
Azure Data Factory (Ingestion)
⬇
Azure Data Lake Storage — Bronze (Raw Data)
⬇
Azure Databricks (Cleaning & Standardization)
⬇
Azure Data Lake Storage — Silver (Delta Tables)
⬇
Azure Databricks (Business Aggregations)
⬇
Azure Data Lake Storage — Gold (Analytics Tables)

### Gold Analytics Tables Created

The following business metrics were generated from Silver data.

Gold Table	Description
daily_revenue	Total revenue generated per day
sales_by_store_daily	Revenue breakdown by store and date
top_products_daily	Best selling products based on revenue
customer_lifetime_value	Total revenue generated by each customer

These datasets provide insights into sales performance, product trends, and customer value.

### Transformations Applied

The Gold layer performs business aggregations on Silver datasets:

• Join multiple datasets (orders, order_items, products, customers, stores)
• Calculate revenue using quantity, unit price, and discounts
• Filter completed transactions only
• Aggregate results using Spark group-by operations

Example calculation:

daily_revenue_df = (
    orders_df
    .join(order_items_df, "order_id")
    .filter(col("order_status") == "Completed")
    .groupBy("order_date")
    .agg(
        spark_sum(
            col("quantity") * col("unit_price") * (1 - col("discount_pct"))
        ).alias("daily_revenue")
    )
)
### Gold Storage Structure

Gold datasets were written as Delta tables into ADLS:

gold/marts/daily_revenue
gold/marts/sales_by_store_daily
gold/marts/top_products_daily
gold/marts/customer_lifetime_value

Using Delta Lake ensures:

ACID transactions

Reliable data consistency

Faster analytical queries

Scalable data lakehouse architecture

### Example Business Insights

The Gold layer enables business questions such as:

• Which stores generate the most revenue?
• What are the top-selling products by day?
• How much revenue does each customer generate?
• What are the daily revenue trends?

These insights can easily feed Power BI dashboards or analytics tools.

 ### Technologies Used

• Azure Databricks
• PySpark
• Delta Lake
• Azure Data Lake Storage Gen2
• Azure Data Factory
• GitHub

# Day 7: Data Quality Checks

### Objective

Implemented data quality validations in Azure Databricks to ensure reliability of the Silver and Gold layers in the retail lakehouse architecture.

These validations help detect missing keys, duplicate records, invalid numeric values, broken relationships between tables, and business rule inconsistencies before the data is used for reporting and analytics.

---

### Architecture with Data Quality Layer

```
CSV Data
   ↓
Azure Data Factory
   ↓
Bronze Layer (Raw Data)
   ↓
Databricks Processing
   ↓
Silver Layer (Clean Delta Tables)
   ↓
Data Quality Validation
   ↓
Gold Layer (Analytics Tables)
```

---

### Data Quality Checks Implemented

#### 1. Null Checks

Validated that critical identifier columns contain no null values:

* `customers.customer_id`
* `products.product_id`
* `stores.store_id`
* `orders.order_id`
* `order_items.order_item_id`
* `payments.payment_id`

---

#### 2. Duplicate Checks

Validated that primary keys remain unique across all tables:

* `customer_id`
* `product_id`
* `store_id`
* `order_id`
* `order_item_id`
* `payment_id`

---

#### 3. Numeric & Range Validation

Validated business logic rules for numeric fields:

* `quantity > 0`
* `unit_price >= 0`
* `discount_pct between 0 and 1`
* `payment amount >= 0`

Safe casting (`try_cast`) was used for the `payments.amount` column because the field is stored as a string and may contain non-numeric values.

---

#### 4. Referential Integrity Checks

Ensured relationships between tables are valid:

* `order_items.order_id` exists in `orders`
* `payments.order_id` exists in `orders`
* `orders.customer_id` exists in `customers`
* `orders.store_id` exists in `stores`
* `order_items.product_id` exists in `products`

---

#### 5. Business Rule Validation

Validated operational consistency:

Completed orders should have corresponding payment records.

```
Completed Orders → Must have Payment
```

---

### Data Quality Summary Table

All validation results were aggregated into a summary table:

| check_name                        | failed_records |
| --------------------------------- | -------------- |
| null_customer_id                  | 0              |
| duplicate_order_id                | 0              |
| invalid_quantity                  | 0              |
| invalid_order_items_orders        | 0              |
| completed_orders_without_payments | 0              |

This provides a clear overview of pipeline health.

---

### Data Quality Results Storage

Validation results were optionally written to ADLS Gen2 as a Delta table:

```
gold/marts/data_quality_results
```

This enables monitoring of data quality over time.

---

### Technologies Used

* Azure Databricks
* PySpark
* Delta Lake
* Azure Data Lake Storage Gen2

---

### Key Learning

This step demonstrates how to integrate **data quality validation into a modern lakehouse pipeline**. Ensuring data integrity before analytics prevents incorrect reporting and improves trust in downstream dashboards.

---

### Files Added

```
notebooks/data_quality/day7_data_quality_checks.py
screenshots/day7-dq-summary.png
screenshots/day7-null-checks.png
screenshots/day7-referential-integrity.png
```

---

### Git Commit

```
git add .
git commit -m "day7: add data quality checks for silver and gold layers"
git push
```

# Day 8 — CI/CD Automation with GitHub Actions
### Goal

Implement Continuous Integration (CI) using GitHub Actions to automatically validate the project whenever code is pushed to the repository.

This step introduces DevOps automation into the data engineering workflow.

## CI/CD Architecture
Developer Code Changes
        ↓
Git Push to GitHub
        ↓
GitHub Actions Workflow
        ↓
Automated Project Validation
        ↓
Quality Assurance for Data Engineering Project

This ensures the project maintains consistent structure and code quality.

### GitHub Actions Workflow

A GitHub workflow was created to run automatically on:

push events

pull_request events

Workflow file location:

.github/workflows/ci.yml
### CI Validation Steps

The CI pipeline performs the following automated checks:

• Verifies repository structure
• Ensures required folders exist
• Validates Python script syntax
• Confirms README documentation is present

Example workflow step:

- name: Validate repository structure
  run: python scripts/project_validation.py
### Project Validation Script

A lightweight validation script was created to check required project components.

Location:

scripts/project_validation.py

The script verifies that the following folders and files exist:

README.md
docs/
data/
notebooks/
adf/
screenshots/

This ensures the repository structure remains consistent as the project evolves.

### Technologies Used

• GitHub Actions
• YAML Workflow Configuration
• Python Validation Script
• Git Version Control


# Day 9 — Documentation, Runbook & Cost Management
🎯 Goal

Enhance the project with professional documentation and operational guidance to make the Azure Retail Lakehouse easy to understand, run, and maintain.

This step focuses on improving project usability, operational readiness, and portfolio quality.

### Documentation Architecture
Project Code
      ↓
Technical Documentation
      ↓
Runbook + Cost Management
      ↓
Operational Readiness
      ↓
Portfolio / Interview Ready Project

Documentation ensures that new developers or stakeholders can easily understand and execute the data pipeline.

### Documentation Added

The following documentation files were created:

Document	Description
architecture.md	Explains the end-to-end data architecture
runbook.md	Step-by-step guide for executing pipelines and notebooks
cost_management.md	Best practices to keep Azure costs low
demo.md	Walkthrough guide for presenting the project

Location:

docs/
### Architecture Overview

The project follows the Medallion Architecture pattern:

Retail CSV Data
        ↓
Azure Data Factory (Ingestion)
        ↓
ADLS Gen2 Bronze Layer
        ↓
Azure Databricks (Cleaning & Transformation)
        ↓
ADLS Gen2 Silver Layer
        ↓
Azure Databricks (Aggregations)
        ↓
ADLS Gen2 Gold Layer
        ↓
Analytics / Reporting

Each layer progressively improves data quality and business usability.

### Runbook

The runbook explains how to execute the project:

Upload raw retail CSV files to Bronze storage

Run Azure Data Factory ingestion pipeline

Execute Databricks notebook for Bronze → Silver transformation

Execute Databricks notebook for Silver → Gold analytics

Run data quality checks

Verify results in Gold layer

Runbook location:

docs/runbook.md
### Cost Management Strategy

The project was designed to minimize cloud costs.

Key practices used:

• Small datasets for development
• Azure Databricks single-node cluster
• Auto termination after 15 minutes
• Manual pipeline runs only
• No active triggers running continuously

Cost guide location:

docs/cost_management.md
### Resource Cleanup

To avoid unnecessary Azure charges:

Terminate Databricks clusters after use

Keep ADF triggers disabled

Delete the resource group when the project is no longer needed

Example cleanup command:

az group delete --name rg-retail-lakehouse-dev
### Project Evidence

Screenshots were captured throughout development to document pipeline execution, transformations, and CI automation.

Example files:

screenshots/day4-pipeline.png
screenshots/day5-silver-delta.png
screenshots/day6-daily-revenue.png
screenshots/day7-dq-summary.png
screenshots/day8-github-actions-success.png




