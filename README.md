# Overview
This project demonstrates a production-style data engineering pipeline that captures real-time changes from a MySQL database and loads them into Snowflake using a custom-built Change Data Capture (CDC) mechanism.
It implements Slowly Changing Dimension (SCD Type 2) to maintain full historical tracking of patient data.

## Architecture
Flow:
MySQL → Python CDC → Snowflake Bronze → Snowflake Silver (SCD Type 2) → dbt Gold
