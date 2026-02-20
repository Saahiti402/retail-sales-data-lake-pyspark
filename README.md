\# Retail Sales Data Lake – PySpark (Bronze-Silver-Gold Architecture)



\## Project Overview

This project implements a multi-layered Data Lake architecture using PySpark.  

It processes retail sales data using a structured ETL approach following Bronze, Silver, and Gold layers.



\## Architecture



Raw CSV

&nbsp;  ↓

Bronze Layer (Raw Ingestion)

&nbsp;  ↓

Silver Layer (Data Cleaning \& Standardization)

&nbsp;  ↓

Gold Layer (Business Aggregations)



\## Layers Explained



\### Bronze Layer

\- Raw data ingestion

\- Schema inference

\- Immutable storage

\- Source of truth



\### Silver Layer

\- Deduplication

\- Safe numeric casting

\- Date conversion

\- Null handling

\- Column standardization



\### Gold Layer

\- Revenue by region

\- Revenue by category

\- Monthly sales trends

\- Business-ready datasets



\## Technologies Used

\- Python

\- PySpark

\- Distributed Data Processing

\- ETL Design Principles



\## Key Highlights

\- Processed 9,800+ records

\- Resolved CSV parsing inconsistencies

\- Implemented defensive casting using try\_cast

\- Designed enterprise-style layered architecture



\## How to Run



1\. Install PySpark:

&nbsp;  pip install pyspark



2\. Run Bronze:

&nbsp;  python scripts/bronze\_layer.py



3\. Run Silver:

&nbsp;  python scripts/silver\_layer.py



4\. Run Gold:

&nbsp;  python scripts/gold\_layer.py

