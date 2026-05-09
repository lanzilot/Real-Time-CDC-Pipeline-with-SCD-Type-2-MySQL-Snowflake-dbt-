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
