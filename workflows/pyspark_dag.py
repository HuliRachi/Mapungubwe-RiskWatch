# Step 1 - Import modules

import airflow 
from airflow import DAG
from datetime import timedelta
from datetime import datetime, timedelta
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator


PROJECT_ID = "project-a2ce378b-71f9-4087-95b" 
REGION = "africa-south1"
CLUSTER_NAME = "kruger-cluster" 
COMPOSER_BUCKET = "africa-south1-kruger-compos-2b9db095-bucket" # composer bucket name

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/EKR-Electrical_Kruger_Records/ingestion/kruger_mysqlToLanding.py"
PYSPARK_JOB_1 = {"reference": {"project_id": PROJECT_ID}, "placement":{"cluster_name":CLUSTER_NAME}, "pyspark_job":{"main_python_file_uri":GCS_JOB_FILE_1}}

# Step 2 - Define default arguments

ARGS = {
    "owner": "RACHI HULI",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Step 3 - Instantiate the DAG

with DAG(
    dag_id = "pyspark_dag",
    schedule_interval = None,
    default_args = ARGS,
    tags = ["pyspark", "dataproc", "etl"]
)as dag:
    
    # Step 4 - Define tasks

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_1",
        job = PYSPARK_JOB_1,
        region = REGION,
        project_id = PROJECT_ID
    )
   
# Step 5 - Define Dependencies

pyspark_task_1