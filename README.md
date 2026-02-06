# Data Warehouse Project: SQL ETL Pipeline (Medallion Architecture)

This project implements a robust **Medallion Architecture** using SQL Server to transform raw CRM and ERP data into a structured **Gold Layer** for business intelligence.

---

## 🏗 Data Warehouse Architecture
The project follows a three-tier structure to ensure data lineage and quality:

1.  **Bronze Layer**: Raw data ingestion via `BULK INSERT` from CSV files.
2.  **Silver Layer**: Data cleaning, de-duplication, and standardization.
3.  **Gold Layer**: Dimensional modeling (Star Schema) with established relationships.



---

## 🛠 ETL Process Breakdown

### 📥 1. Bronze Layer (Extract & Load)
Raw data is loaded from a Docker-mounted volume into staging tables.
* **CRM Data**: Customer info, Product info, and Sales details.
* **ERP Data**: Category mapping, Location info, and additional Customer metadata.

### 🥈 2. Silver Layer (Transform & Clean)
This layer transforms messy raw data into a "Source of Truth".
* **De-duplication**: Used `ROW_NUMBER()` to keep only the most recent record based on `cst_create_date`.
* **Standardization**: Converted codes (M/F, S/M) into readable labels (Male/Female, Single/Married).
* **Cross-Platform Fixes**: Explicitly handled **CRLF/LF** issues by removing hidden `CHAR(13)` and `CHAR(10)` characters that interfere with joins on Mac/Unix environments.
* **Business Logic**: Derived product end-dates using the `LEAD()` function for SCD Type 2 tracking.

### 🥇 3. Gold Layer (Dimensional Modeling)
The final layer creates a **Star Schema** for reporting.
* **Dimensions**: `dim_customers` and `dim_products` combine multiple sources into single, clean entities.
* **Facts**: `fact_sales` links transactions to dimensions using surrogate keys.



---

## 🔗 Schema Relationships (Referential Integrity)
To ensure data consistency, the following relationships are established in the model:

* **Fact Sales → Dim Products**: Linked via `product_key` (FK).
* **Fact Sales → Dim Customers**: Linked via `customer_key` (FK).
* **Dim Products → Category**: Linked via `category_id`.

---

## ⚙️ Tech Stack
* **DBMS**: Azure SQL Edge (Docker).
* **IDE**: Azure Data Studio.
* **Language**: T-SQL.
