import os
from airflow import DAG
from datetime import timedelta
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

PROJECT_ID = "project-a2ce378b-71f9-4087-95b" 
REGION = "africa-south1"
CLUSTER_NAME = "mapungubwe-cluster" 
COMPOSER_BUCKET = "africa-south1-mapungubwe-co-1780652a-bucket" 

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/MR-Mapungubwe_Park_Records/ingestion/mapungubwe_mysqlToLanding.py"
PYSPARK_JOB_1 = {"reference": {"project_id": PROJECT_ID}, "placement":{"cluster_name":CLUSTER_NAME}, "pyspark_job":{"main_python_file_uri":GCS_JOB_FILE_1}}

ARGS = {
    "owner": "RACHI HULI",
    "start_date": datetime(2026, 1, 1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "pyspark_dag",
    schedule_interval = None,
    default_args = ARGS,
    tags = ["pyspark", "dataproc", "etl"]
) as dag:
    
    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id = "pyspark_task_1",
        job = PYSPARK_JOB_1,
        region = REGION,
        project_id = PROJECT_ID
    )

pyspark_task_1
