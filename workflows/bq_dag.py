import os
from airflow import DAG
from datetime import datetime, timedelta


IS_LOCAL = os.getenv("RUNNING_ENV") == "LOCAL_DOCKER"

if IS_LOCAL:
    from airflow.operators.python import PythonOperator
    from datawarehouse.data_utils import create_schema, create_table, load_csv_to_layer
else:
    from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "project-a2ce378b-71f9-4087-95b"
LOCATION = "africa-south1"
CSV_BASE_PATH = "/home/airflow/gcs/data"

SQL_FILE_PATH_1 = "/home/airflow/gcs/data/bigquery/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/bigquery/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/bigquery/gold.sql"

def read_sql_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        return "SELECT 1;"
    
BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

def run_local_postgres_pipeline():
    """Builds your schemas, tables, and streams your local CSV logs into Postgres."""
    print("Starting local Mapungubwe Data Warehouse Initialization...")
    
    create_schema("bronze_dataset")
    create_schema("silver_dataset")
    create_schema("gold_dataset")
    
    create_table("bronze_dataset", "dim_date")
    create_table("bronze_dataset", "dim_ranger")
    create_table("bronze_dataset", "dim_zone")
    create_table("bronze_dataset", "fact_incidents")
    create_table("bronze_dataset", "fact_patrol")
    
    load_csv_to_layer("bronze_dataset", "dim_date", f"{CSV_BASE_PATH}/dim_date.csv")
    load_csv_to_layer("bronze_dataset", "dim_ranger", f"{CSV_BASE_PATH}/dim_ranger.csv")
    load_csv_to_layer("bronze_dataset", "dim_zone", f"{CSV_BASE_PATH}/dim_zone.csv")
    load_csv_to_layer("bronze_dataset", "fact_incidents", f"{CSV_BASE_PATH}/fact_incidents.csv")
    load_csv_to_layer("bronze_dataset", "fact_patrol", f"{CSV_BASE_PATH}/fact_patrol.csv")
    print("All local schemas and tables populated successfully!")

ARGS = {
    "owner":"Rachi Huli",
    "start_date": datetime(2026, 1, 1), 
    "depend_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":["***@gmail.com"],
    "email_on_success":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id = "bigquery_dag",
    schedule_interval = None,
    description = "DAG to run bigquery jobs",
    default_args = ARGS,
    tags = ["gcs", "bq", "etl", "marvel"]
) as dag:
    
        
    bronze_tables = BigQueryInsertJobOperator(
        task_id = "bronze_tables",
        configuration = {"query": {"query": BRONZE_QUERY, "useLegacySql":False, "priority":"BATCH"}},
        location = LOCATION,
    )

    silver_tables = BigQueryInsertJobOperator(
        task_id = "silver_tables",
        configuration = {"query": {"query":SILVER_QUERY, "useLegacySql":False, "priority": "BATCH"}},
        location = LOCATION,
    )

    gold_tables = BigQueryInsertJobOperator(
        task_id = "gold_tables",
        configuration = {"query":{"query":GOLD_QUERY, "useLegacySql":False, "priority": "BATCH"}},
        location = LOCATION,
    )

    bronze_tables >> silver_tables >> gold_tables
