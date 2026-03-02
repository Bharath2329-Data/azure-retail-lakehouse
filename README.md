# Azure Retail Lakehouse Project

## Objective
Build an end-to-end retail analytics lakehouse using Azure cloud services.

## Architecture
- Azure Data Lake Storage Gen2
- Azure Data Factory
- Azure Databricks
- Bronze / Silver / Gold architecture

## Status
## Day 1: Environment setup complete


## Day 2 — Retail Data Ingestion (Bronze Layer)

Create realistic retail datasets and upload them into the Azure Data Lake Bronze layer as raw source data.

## Architecture (Current State)

Retail CSV Files
⬇
Azure Data Lake Storage Gen2
⬇
Bronze Layer (Raw Data)

📂 Datasets Created

The following retail datasets were generated:

## Dataset	Description
customers	Customer master data with loyalty tier
products	Product catalog with categories and pricing
stores	Store location details
orders	Order header information
order_items	Line-level product transactions
payments	Payment details per order

All datasets are stored locally under:

data/sample_raw/
## ☁ Bronze Layer Structure (ADLS)

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

📘 Data Validation Performed

Using Databricks:

Loaded CSV files from Bronze

Verified schema structure

Checked row counts

Validated relational integrity between:

orders and order_items

orders and payments

Performed null checks

This ensures source data quality before transformation.

🛠 Technologies Used

Azure Data Lake Storage Gen2

Azure Portal

Databricks (PySpark)

GitHub

PowerShell (Git version control)

