-- =========================================================================
-- MAPUNGUBWE RISKWATCH: WAREHOUSE STRUCTURE INITIALIZATION
-- This script provisions cluster boundaries inside the PostgreSQL container.
-- =========================================================================

-- 1. Create Metastores for Airflow Cluster Coordination
CREATE DATABASE airflow_metadata_db;
CREATE DATABASE celery_results_db;

-- 2. Create the Dedicated Analytical Data Warehouse Database
CREATE DATABASE mapungubwe_db;

-- 3. Provision the Dedicated Application Admin User Profile
CREATE USER rachuhuli WITH PASSWORD 'X57tmQ846GYP3Jgb';

-- Grant absolute administrative operational privileges over the warehouse
GRANT ALL PRIVILEGES ON DATABASE mapungubwe_db TO rachuhuli;
ALTER DATABASE mapungubwe_db OWNER TO rachuhuli;

-- 4. Connect directly to our target data warehouse database
\c mapungubwe_db;

-- Construct multi-tier layered engineering architectures
CREATE SCHEMA IF NOT EXISTS bronze_dataset AUTHORIZATION rachuhuli;
CREATE SCHEMA IF NOT EXISTS silver_dataset AUTHORIZATION rachuhuli;
CREATE SCHEMA IF NOT EXISTS gold_dataset AUTHORIZATION rachuhuli;
