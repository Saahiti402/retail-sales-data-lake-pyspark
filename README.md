# Retail Sales Data Lake – PySpark (Bronze–Silver–Gold Architecture)

## 📌 Project Overview

This project implements an end-to-end **Data Lake architecture** using PySpark following the industry-standard **Bronze–Silver–Gold layered design pattern**.

The pipeline ingests raw retail sales data, performs data cleansing and transformation, and generates business-ready aggregated datasets for analytics.

This project simulates an enterprise-grade ETL workflow similar to modern cloud data platforms such as **Azure Databricks**, **Delta Lake**, or large-scale distributed Spark environments.

---

## 📊 Dataset Used

**Dataset:** Superstore Retail Sales Dataset  
**Source:** Kaggle – Superstore Sales Data  
**Records Processed:** 9,800+  

### Dataset Includes:
- Order ID
- Order Date
- Ship Date
- Customer Details
- Region & State
- Product Category & Sub-Category
- Sales Amount

This dataset represents transactional retail sales data and is suitable for demonstrating real-world ETL transformations and aggregation workflows.

---

## 🏗 Architecture

```
Raw CSV Data
      ↓
Bronze Layer (Raw Ingestion)
      ↓
Silver Layer (Data Cleaning & Standardization)
      ↓
Gold Layer (Business Aggregations)
```

This layered design mirrors enterprise data lake implementations used in modern cloud data platforms.

---

## 🥉 Bronze Layer – Raw Ingestion

**Purpose:** Preserve raw data as the source of truth.

### Responsibilities:
- Ingest raw CSV file
- Apply schema inference
- Store structured raw dataset
- Maintain immutability

### Key Concepts:
- Raw zone storage
- Auditability
- Schema inference
- Distributed ingestion using Spark

---

## 🥈 Silver Layer – Data Cleansing & Standardization

**Purpose:** Transform raw data into clean, structured format.

### Transformations Applied:

- Deduplication of records
- Safe numeric casting for `sales` column
- Handling malformed numeric values
- Date conversion (Order Date & Ship Date)
- Null value handling
- Column name standardization
- Defensive casting using `try_cast`
- Schema enforcement

### Real-World Issues Resolved:

- CSV parsing inconsistencies
- Commas within quoted strings
- Malformed numeric values (e.g., '.', 'Blank', empty strings)
- Schema misalignment issues
- Safe numeric conversion in distributed processing

This layer ensures clean, analytics-ready structured data.

---

## 🥇 Gold Layer – Business Aggregations

**Purpose:** Produce analytics-ready datasets for reporting and BI consumption.

### Aggregations Created:

1. Revenue by Region  
2. Revenue by Product Category  
3. Monthly Sales Trend (Year + Month)

These outputs simulate datasets typically consumed by:
- BI dashboards
- Reporting tools
- Data analysts
- Executive stakeholders

---

## 🛠 Technologies Used

- Python
- PySpark
- Distributed Data Processing
- ETL Design Principles
- Layered Data Lake Architecture

---

## 📂 Project Structure

```
retail-data-lake/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── scripts/
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   └── gold_layer.py
│
└── README.md
```

---

## ▶ How to Execute

### 1️⃣ Install Requirements

```bash
pip install pyspark
```

---

### 2️⃣ Download Dataset

Download the **Superstore Sales Dataset** from Kaggle.

Place the CSV file inside:

```
data/raw/
```

Rename the file:

```
superstore.csv
```

---

### 3️⃣ Run Bronze Layer

```bash
python scripts/bronze_layer.py
```

This will:
- Ingest raw CSV
- Store structured Bronze dataset

---

### 4️⃣ Run Silver Layer

```bash
python scripts/silver_layer.py
```

This will:
- Clean data
- Handle malformed values
- Enforce schema
- Standardize column names
- Prepare analytics-ready dataset

---

### 5️⃣ Run Gold Layer

```bash
python scripts/gold_layer.py
```

This will generate:

```
data/gold/
   region_revenue.csv
   category_revenue.csv
   monthly_sales.csv
```

---

## 📈 Key Highlights

- Processed 9,800+ retail sales records
- Implemented enterprise-style layered data lake architecture
- Resolved real-world data quality issues
- Applied defensive data engineering practices
- Designed analytics-ready business output layer
- Simulated distributed ETL workflow using PySpark

---

## 🎯 Learning Outcomes

- Practical implementation of Data Lake architecture
- Handling dirty real-world CSV data
- Safe casting and schema enforcement
- Building business-ready aggregation layers
- Understanding enterprise ETL workflows
- Debugging distributed data processing issues

---

## 🚀 Future Enhancements

- Migration to Azure Databricks
- Integration with Delta Lake
- Workflow orchestration using Airflow or Azure Data Factory
- CI/CD integration
- Data validation framework
- Cloud storage integration (ADLS / GCS / S3)

---

## 📌 Author

Saahiti K S  
Computer Science Engineering  
Cloud & Data Engineering Enthusiast
