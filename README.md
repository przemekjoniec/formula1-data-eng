# Formula 1 Data Engineering Project: Azure Databricks & Spark

## 🏎️ Overview
This repository contains a complete, end-to-end data engineering project built on **Azure Databricks** and **Apache Spark**, utilizing real-world **Formula 1 Motor Racing data** as part of the *"Azure Databricks & Spark for Data Engineers: Hands-on Project"* course, focusing on real-world data engineering solutions and modern Databricks features.

The project demonstrates the implementation of a modern **Data Lakehouse** using the **Medallion Architecture**, moving away from legacy approaches to leverage modern Databricks capabilities such as Unity Catalog, Lakeflow Jobs, and Databricks SQL Dashboards.

## 🏗️ Architecture
The pipeline strictly follows the **Medallion Architecture** to ensure scalable and reliable data pipelines:
* **🥉 Bronze Layer:** Raw data ingestion from the source (Azure Data Lake Gen 2). Data is stored in its original format.
* **🥈 Silver Layer:** Cleansed, filtered, and transformed data using PySpark and Spark SQL. Schemas are enforced and data quality checks are applied.
* **🥇 Gold Layer:** Business-level aggregates and modeled data optimized for analytics, BI, and reporting.

## 🛠️ Tech Stack & Technologies
* **Cloud Platform:** Microsoft Azure (ADLS Gen2, Azure Data Factory)
* **Compute & Processing:** Azure Databricks, Apache Spark (PySpark, Spark SQL)
* **Storage:** Delta Lake
* **Governance:** Unity Catalog
* **Orchestration:** Lakeflow Jobs
* **Analytics & BI:** Databricks SQL & Dashboards

## 🚀 Key Features
* **Modern Lakehouse Design:** Implemented a full Lakehouse architecture using Delta Lake on Azure.
* **Data Governance:** Organized and governed datasets using Databricks Unity Catalog.
* **Incremental Processing:** Enhanced data pipelines with incremental data processing leveraging Delta Lake's capabilities.
* **Pipeline Orchestration:** Built and automated end-to-end workflows using Databricks Lakeflow Jobs.
* **Data Visualization:** Created analytical views and interactive dashboards directly within Databricks SQL.