# Supermarket Sales Data Pipeline

## Overview
Automated GCP data pipeline using:
- Kaggle API
- Cloud Storage
- Dataflow (Apache Beam)
- BigQuery

## Architecture
Kaggle → GCS → Dataflow → BigQuery → Reporting

## Components
- dataflow_pipeline.py : ETL pipeline
- SQL scripts : schema & reporting
- Notebook : local testing
- Architecture diagram

## How to Run
python dataflow_pipeline.py \
--runner DataflowRunner \
--project PROJECT_ID \
--region asia-south1