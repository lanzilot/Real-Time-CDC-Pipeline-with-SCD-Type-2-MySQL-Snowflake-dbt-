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

## Snowflake SQL
CREATE DATABASE hospital_pipeline;

USE DATABASE hospital_pipeline;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

CREATE TABLE hospital_pipeline.bronze.patients_raw (
    patient_id INT,
    name STRING,
    age INT,
    gender STRING,
    updated_at TIMESTAMP,
    operation STRING, -- INSERT / UPDATE / DELETE
    source_system STRING,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE hospital_pipeline.silver.patients_clean AS
SELECT
    id,
    LOWER(name) AS name,
    age,
    sex,
    updated_at
FROM bronze.patients_raw
WHERE operation != 'DELETE';

CREATE OR REPLACE TABLE hospital_pipeline.gold.patient_summary AS
SELECT
    sex,
    COUNT(*) AS total_patients,
    AVG(age) AS avg_age
FROM silver.patients_clean
GROUP BY sex;

ALTER WAREHOUSE COMPUTE_WH RESUME;
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 300;
ALTER WAREHOUSE COMPUTE_WH RESUME;


CREATE SCHEMA IF NOT EXISTS hospital_pipeline.bronze;
CREATE SCHEMA IF NOT EXISTS hospital_pipeline.silver;
CREATE SCHEMA IF NOT EXISTS hospital_pipeline.gold;

CREATE TABLE IF NOT EXISTS hospital_pipeline.bronze.cdc_watermark (
    table_name STRING,
    last_updated TIMESTAMP
);

CREATE TABLE hospital_pipeline.bronze.patients_raw (
    patient_id NUMBER,
    name STRING,
    age STRING,            
    sex STRING,
    updated_at TIMESTAMP,
    operation STRING,
    source_system STRING
);

CREATE OR REPLACE TABLE hospital_pipeline.silver.patients_scd (
    patient_id NUMBER,
    name STRING,
    age NUMBER,
    sex STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);


CREATE OR REPLACE TABLE hospital_pipeline.bronze.patients_stage (
    patient_id INTEGER,
    name STRING,
    age INTEGER,
    sex STRING,
    updated_at TIMESTAMP
);

CREATE OR REPLACE TABLE hospital_pipeline.silver.patients_scd (
    patient_id INTEGER,
    name STRING,
    age INTEGER,
    sex STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);

