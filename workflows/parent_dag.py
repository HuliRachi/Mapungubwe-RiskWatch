# Step 1 - Import all modules

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Steep 2 - Define Default Arguments

ARGS ={
    "owner":"RACHI HULI",
    "start_date":days_ago(1),
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":["***@gmail.com"],
    "email_on_success":False,
    "retries":1,
    "retry_delay":timedelta(minutes=5)
}

# Step 3 - Instantiate the DAG

with DAG (
    dag_id = "parent_dag",
    schedule_interval = "0 5 * * *",
    description = "Parent DAG to trigger Pyspark and BigQuery DAGS",
    default_args = ARGS,
    tags = ["parent", "orchestration", "etl"]

)as dag:
    
    # Step 4 - Define tasks
    
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id = "trigger_pyspark_dag",
        trigger_dag_id = "pyspark_dag",
        wait_for_completion = True,
    )
    
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id = "trigger_bigquery_dag",
        trigger_dag_id = "bigquery_dag",
        wait_for_completion = True,
    )

# Step 5 - Define Dependencies

trigger_pyspark_dag >> trigger_bigquery_dag