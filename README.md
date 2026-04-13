#  Azure Retail Lakehouse Project (End-to-End Data Engineering)

##  Project Overview

This project demonstrates the design and implementation of an **end-to-end Azure Data Engineering pipeline** using modern cloud technologies.

The solution follows the **Medallion Architecture (Bronze → Silver → Gold)** to ingest, transform, validate, and analyze retail data.

This project was built in **10 days** with a focus on **real-world data engineering practices, cost optimization, and CI/CD automation**.

---

##  Project Objective

- Build a scalable Azure-based data pipeline  
- Implement Bronze, Silver, and Gold layers  
- Perform data transformation using PySpark  
- Generate business insights using SQL  
- Apply data quality checks  
- Integrate CI/CD using GitHub Actions  
- Maintain a low-cost cloud architecture  

---

##  Architecture Overview

Retail CSV Files
↓
Azure Data Factory (Ingestion)
↓
ADLS Gen2 Bronze Layer (Raw Data)
↓
Azure Databricks (PySpark Transformations)
↓
ADLS Gen2 Silver Layer (Delta Tables)
↓
Azure Databricks (Aggregations)
↓
ADLS Gen2 Gold Layer (Analytics Tables)
↓
SQL Analytics / Reporting


---

## Architecture Diagram (Layered Lakehouse Design)

            ┌──────────────────────────────┐
            │      Retail CSV Sources      │
            │ (customers, orders, etc.)   │
            └──────────────┬───────────────┘
                           │
                           ▼
            ┌──────────────────────────────┐
            │   Azure Data Factory (ADF)   │
            │   Parameterized Pipelines    │
            └──────────────┬───────────────┘
                           │
                           ▼
    ┌────────────────────────────────────────────┐
    │            BRONZE LAYER (ADLS)            │
    │        Raw Data (No Transformations)       │
    └───────────────────┬────────────────────────┘
                        │
                        ▼
    ┌────────────────────────────────────────────┐
    │         SILVER LAYER (Delta Tables)        │
    │   Cleaned, Typed, Deduplicated Data        │
    └───────────────────┬────────────────────────┘
                        │
                        ▼
    ┌────────────────────────────────────────────┐
    │           GOLD LAYER (Analytics)           │
    │   Aggregations & Business Metrics          │
    └───────────────────┬────────────────────────┘
                        │
                        ▼
    ┌────────────────────────────────────────────┐
    │      SQL Analytics / Power BI / Reports    │
    └────────────────────────────────────────────┘


##  Technology Stack

- Azure Data Lake Storage Gen2 (ADLS)
- Azure Data Factory (ADF)
- Azure Databricks
- PySpark
- Delta Lake
- SQL (Spark SQL)
- GitHub
- GitHub Actions (CI/CD)
- PowerShell

---

##  Dataset Description

The project uses simulated retail datasets:

- customers  
- products  
- stores  
- orders  
- order_items  
- payments  

These datasets support analytics such as:

- revenue trends  
- product performance  
- customer lifetime value  

---

##  Medallion Architecture

### 🔹 Bronze Layer
- Raw CSV data stored in ADLS  
- No transformation applied  
- Immutable storage  

### 🔹 Silver Layer
- Cleaned and standardized data  
- Data type conversions  
- Duplicate removal  
- Added audit columns (`ingest_ts`)  
- Stored as Delta tables  

### 🔹 Gold Layer
- Business-ready analytics tables  
- Aggregated metrics  
- Optimized for reporting  

---

##  Day-by-Day Implementation

| Day | Task | Outcome |
|-----|------|--------|
| Day 1 | Project setup | Azure resources + repo structure |
| Day 2 | Data ingestion | Retail CSV datasets uploaded to Bronze |
| Day 3 | ADF setup | Linked service + datasets |
| Day 4 | Pipeline | Parameterized ingestion pipeline |
| Day 5 | Silver layer | Data cleaning using PySpark |
| Day 6 | Gold layer | Business analytics tables |
| Day 7 | Data quality | Validation checks implemented |
| Day 8 | CI/CD | GitHub Actions workflow |
| Day 9 | Documentation | Runbook, architecture, cost guide |
| Day 10 | Final polish | LinkedIn-ready project |

#  Day-by-Day Implementation Details

## 🔹 Day 1 — Project Setup

- Created GitHub repository
- Set up project folder structure
- Created Azure Resource Group
- Created Azure Data Lake Storage Gen2 (ADLS)
- Enabled hierarchical namespace
- Created containers:
  - bronze
  - silver
  - gold
  - logs

 Outcome:
Environment ready for data ingestion and processing

---

## 🔹 Day 2 — Data Ingestion (Bronze Layer)

- Generated retail datasets:
  - customers
  - products
  - stores
  - orders
  - order_items
  - payments
- Uploaded CSV files to ADLS Bronze layer
- Created folder structure:
  - bronze/raw/customers/
  - bronze/raw/orders/
  - etc.
- Created data dictionary documentation

 Outcome:
Raw data successfully stored in Bronze layer

---

## 🔹 Day 3 — Azure Data Factory Setup

- Created Azure Data Factory instance
- Enabled Managed Identity
- Assigned RBAC role (Storage Blob Data Contributor)
- Created Linked Service to ADLS
- Created datasets for Bronze files

 Outcome:
ADF successfully connected to ADLS

---

## 🔹 Day 4 — Parameterized ADF Pipeline

- Built reusable pipeline: `PL_CopyToBronze_Parameterized`
- Created parameterized datasets:
  - Source dataset
  - Sink dataset
- Added parameters:
  - pContainer
  - pSourceFolder
  - pFileName
  - pTargetFolder
- Executed debug run
- Verified data copy in ADLS

 Outcome:
Reusable ingestion pipeline created

---

## 🔹 Day 5 — Bronze to Silver Transformation

- Created Azure Databricks workspace
- Configured cluster with auto-termination
- Connected Databricks to ADLS using OAuth
- Loaded Bronze CSV files
- Applied transformations:
  - removed duplicates
  - cast data types
  - standardized dates
  - added `ingest_ts`
- Stored data in Silver layer as Delta tables

 Outcome:
Clean and structured Silver layer created

---

## 🔹 Day 6 — Gold Layer Analytics

- Loaded Silver Delta tables
- Built analytics datasets:
  - daily_revenue
  - sales_by_store
  - top_products
  - customer_lifetime_value
- Applied business logic:
  - revenue calculation
  - filtering completed orders
- Stored results in Gold layer

 Outcome:
Business-ready analytics tables created

---

## 🔹 Day 7 — Data Quality Checks

- Implemented validation checks:
  - null checks
  - duplicate checks
  - numeric validations
  - referential integrity checks
- Validated relationships:
  - orders → customers
  - order_items → orders
  - payments → orders
- Created summary DataFrame for results
- (Optional) Stored results in Gold layer

 Outcome:
Data quality validation framework implemented

---

## 🔹 Day 8 — CI/CD with GitHub Actions

- Created GitHub Actions workflow (`ci.yml`)
- Configured triggers:
  - push
  - pull_request
- Added validation steps:
  - folder structure check
  - file existence check
  - Python syntax validation
- (Optional) Added validation script

 Outcome:
Automated CI pipeline for project validation

---

## 🔹 Day 9 — Documentation & Runbook

- Created documentation files:
  - architecture.md
  - runbook.md
  - cost_management.md
  - demo.md
- Added runbook for executing pipelines
- Documented cost optimization practices
- Added cleanup instructions

 Outcome:
Project is operationally documented and easy to understand

---

## 🔹 Day 10 — Final Polish & Portfolio Preparation

- Cleaned repository structure
- Finalized README.md
- Added:
  - architecture overview
  - SQL queries
  - screenshots
  - project highlights
- Prepared LinkedIn post
- Organized screenshots for portfolio
- Performed final Git commit

  
---

## Author

**Bharath Reddy**

