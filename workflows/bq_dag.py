import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Define constants
PROJECT_ID = "project-a2ce378b-71f9-4087-95b"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
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