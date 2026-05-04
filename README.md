# Overview
This project demonstrates a production-style data engineering pipeline that captures real-time changes from a MySQL database and loads them into Snowflake using a custom-built Change Data Capture (CDC) mechanism.
It implements Slowly Changing Dimension (SCD Type 2) to maintain full historical tracking of patient data.

## Architecture
Flow:
MySQL → Python CDC → Snowflake Bronze → Snowflake Silver (SCD Type 2) → dbt Gold

## Tech Stack
Python (CDC pipeline)
MySQL (source system)
Snowflake (data warehouse)
dbt (transformations)

## Features
Incremental data ingestion using watermarking
Near real-time pipeline (polling every 5 seconds)
Data cleaning for inconsistent formats (e.g. age strings)
SCD Type 2 implementation for historical tracking
Bronze / Silver / Gold data modeling
dbt transformations for analytics

## Key Concepts
Change Data Capture (CDC)
Watermarking strategy
Batch processing
Data warehouse modeling
Slowly Changing Dimensions (Type 2)

## How It Works
Extract new records from MySQL using updated_at
Store last processed timestamp in cdc_watermark
Load batch into Snowflake staging table
Apply SCD Type 2 logic:
Expire old records
Insert new versions
Transform data using dbt into analytics-ready tables

## Project Structure
cdc_pipeline.py → CDC ingestion logic
sql/ → Snowflake table creation and SCD logic
dbt/ → transformation models
docs/ → architecture and pipeline explanation

## Sample Query
SELECT *
FROM hospital_pipeline.silver.patients_scd
WHERE patient_id = 123
ORDER BY start_date DESC;
