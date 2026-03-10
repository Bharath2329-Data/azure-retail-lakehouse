# Runbook

## Purpose
This runbook explains how to execute the Azure Retail Lakehouse project step by step.

## Step 1 — Upload Raw Data
Upload retail CSV files into the Bronze layer in ADLS Gen2:

- bronze/raw/customers/
- bronze/raw/products/
- bronze/raw/stores/
- bronze/raw/orders/
- bronze/raw/order_items/
- bronze/raw/payments/

## Step 2 — Execute ADF Pipeline
Open Azure Data Factory and run the parameterized pipeline:

PL_CopyToBronze_Parameterized

Example parameters:
- pContainer = bronze
- pSourceFolder = raw/customers
- pFileName = customers.csv
- pTargetFolder = raw/customers

## Step 3 — Run Bronze to Silver Notebook
In Azure Databricks, run:

day5_bronze_to_silver

This notebook:
- reads Bronze CSV files
- cleans and standardizes data
- writes Delta tables to the Silver layer

## Step 4 — Run Silver to Gold Notebook
In Azure Databricks, run:

day6_silver_to_gold

This notebook:
- reads Silver Delta tables
- builds Gold analytics tables
- writes results to the Gold layer

## Step 5 — Run Data Quality Checks
In Azure Databricks, run:

day7_data_quality_checks

This notebook:
- validates key columns
- checks duplicates
- checks business rules
- validates referential integrity

## Step 6 — Validate CI Workflow
Push code changes to GitHub and confirm GitHub Actions workflow completes successfully.

## Output Locations
- Silver Layer: silver/clean/
- Gold Layer: gold/marts/
- Data Quality Results: gold/marts/data_quality_results
