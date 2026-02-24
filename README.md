# 🚀 Supermarket Sales Data Pipeline (GCP Data Engineering Project)

## Overview

This project implements an **end-to-end Data Engineering pipeline** on **Google Cloud Platform (GCP)** to ingest, transform, store, and analyze supermarket sales data.

The pipeline follows modern **Data Warehouse + ETL best practices** using a **Star Schema** model.

---

## Architecture

**Pipeline Flow**

Kaggle API  
→ Python Extraction Script  
→ Cloud Storage (Raw Zone)  
→ Cloud Dataflow (Apache Beam ETL)  
→ BigQuery Data Warehouse  
→ Looker Studio Dashboards  

---

## Tech Stack

- Python
- Apache Beam
- Google Cloud Dataflow
- Google Cloud Storage
- BigQuery
- Looker Studio
- SQL
- GitHub

---

## Repository Structure
supermarket-data-pipeline/
│
├── architecture/
│   └── architecture_diagram.png        # GCP ETL architecture diagram
│
├── notebook/
│   └── supermarket_etl_solution.ipynb  # Local ETL validation (SQLite/Pandas)
│
├── sql/
│   ├── create_tables.sql               # BigQuery schema (Fact & Dimension tables)
│   └── reporting_queries.sql           # Analytical & reporting SQL queries
│
├── dataflow_pipeline.py                # Apache Beam Dataflow ETL pipeline
├── requirements.txt                    # Python dependencies
└── README.md                           # Project documentation

---

## Dataset

**Source:** Kaggle Supermarket Sales Dataset  

Dataset file used: supermarket_analysis.csv


---

## Data Warehouse Design

### Star Schema

### Dimension Tables

#### dim_customer

| Column | Description |
|-------|-------------|
| customer_id | Surrogate key |
| customer_type | Member / Normal |
| gender | Customer gender |
| payment | Payment method |

---

#### dim_product

| Column | Description |
|-------|-------------|
| product_id | Surrogate key |
| branch | Store branch |
| city | Store city |
| product_line | Product category |

---

### Fact Table

#### fact_sales

| Column | Description |
|-------|-------------|
| sale_id | Transaction ID |
| product_id | FK → dim_product |
| customer_id | FK → dim_customer |
| date | Sale date |
| time | Sale time |
| quantity | Items sold |
| unit_price | Price per item |
| tax | Tax amount |
| sales | Total sale value |

**Partitioned by:** `date`  
**Clustered by:** `product_id, customer_id`

---

## ETL Pipeline

### Extract (E)

- Download dataset using Kaggle API
- Python script extracts CSV data
- Upload dataset to **Cloud Storage**

---

### Transform (T)

Performed using **Cloud Dataflow (Apache Beam)**:

- Data cleaning
- Data validation
- Type conversion
- Deduplication
- Dimension table creation
- Surrogate key generation
- Star schema transformation

---

### Load (L)

- Load processed data into **BigQuery**
- Partitioned fact table
- Optimized analytical queries

---

## SQL Scripts

### create_tables.sql

Contains:
- BigQuery table schemas
- Partitioning & clustering configuration

---

### reporting_queries.sql

Contains analytical reports:

- Monthly Sales Trend Analysis
- Top Customers Contribution
- Product Performance by City
- Revenue by Gender
- Sales Ranking per City

---

## Reports & Analytics

Dashboards built using **Looker Studio**:

- Revenue Trends
- Customer Segmentation
- Product Performance
- City-wise Sales Analysis

---

## How to Run Dataflow Pipeline

### 1. Install Dependencie


```bash
pip install -r requirements.txt
```

## How to Run
python dataflow_pipeline.py \
--runner DataflowRunner \
--project PROJECT_ID \
--region asia-south1
--temp_location gs://YOUR_BUCKET/temp

## Orchestration (Optional)

Cloud Composer (Apache Airflow) can be used for:
- Pipeline scheduling
- Job monitoring
- Automated execution
- For assessment purposes, manual execution is supported.

## Deliverables Included

- Python ETL Pipeline (Dataflow)
- SQL Table Creation Scripts
- Reporting SQL Queries
- Architecture Diagram
- Jupyter Notebook (Local Testing)
- GitHub Repository

## Author

Kudarat Atar
Data Engineer | GCP | BigQuery | Dataflow