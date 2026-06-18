import os
import datetime
import json
import pandas as pd
from pyspark.sql import SparkSession

# Initialize dynamic local Spark engine instance
spark = SparkSession.builder.appName("KrugerMysqlToLanding").getOrCreate()

IS_LOCAL = os.getenv("RUNNING_ENV") == "LOCAL_DOCKER"

GCS_BUCKET = "mapungubwe-bucket" 
SANPARK_NAME = "mapungubwe"
MYSQL_DB = "mapungubwe_db"

if IS_LOCAL:
    # --- Local Container Path Frameworks ---
    LOCAL_BASE = "/home/airflow/gcs/data"
    LANDING_PATH = f"{LOCAL_BASE}/landing/{SANPARK_NAME}/"
    ARCHIVE_PATH = f"{LOCAL_BASE}/landing/{SANPARK_NAME}/archive/"
    CONFIG_FILE_PATH = "/opt/airflow/config/config.csv"
    
    MYSQL_URL = f"jdbc:mysql://cloudsql-source:3306/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=convertToNull"
else:
    # --- Authentic Production GCP Infrastructure Targets ---
    from google.cloud import storage, bigquery
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{SANPARK_NAME}/"
    ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{SANPARK_NAME}/archive/"
    CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/config/config.csv"

    BQ_PROJECT = "project-a2ce378b-71f9-4087-95b" 
    BQ_CONFIG_TABLE = f"{BQ_PROJECT}.temp_dataset.config"
    BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
    BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"
    
    MYSQL_URL = f"jdbc:mysql://34.35.134.22:3306/{MYSQL_DB}?useSSL=true&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=convertToNull"

MYSQL_CONFIG = { 
    "url": MYSQL_URL, 
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Rachyhuly@98"
}

log_entries = []

def log_event(event_type, message, table=None):
    """Log tracking event details during workflow runs."""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")


def move_existing_files_to_archive(table):
    """Archives old ingestion datasets using native OS or cloud blobs."""
    if IS_LOCAL:
        table_dir = os.path.join(LANDING_PATH, table)
        if not os.path.exists(table_dir):
            log_event("INFO", f"No existing file structures for table {table}")
            return
            
        local_files = [f for f in os.listdir(table_dir) if f.endswith(".json")]
        if not local_files:
            log_event("INFO", f"No old tracking data logs found inside layout: {table}")
            return
            
        for file in local_files:
            date_part = file.split("_")[-1].split(".")[0]
            
            day, month, year = date_part[:2], date_part[2:4], date_part[-4:]
            
            target_directory = os.path.join(ARCHIVE_PATH, table, year, month, day)
            os.makedirs(target_directory, exist_ok=True)
            
            os.rename(os.path.join(table_dir, file), os.path.join(target_directory, file))
            log_event("INFO", f"Moved local data log {file} to partitioned disk backup folder.", table=table)
    else:
        blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{SANPARK_NAME}/{table}/"))
        existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

        if not existing_files:
            log_event("INFO", f"No existing files for table {table}")
            return
        
        for file in existing_files:
            source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
            date_part = file.split("_")[-1].split(".")[0]
            day, month, year = date_part[:2], date_part[2:4], date_part[-4:]

            archive_path = f"landing/{SANPARK_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
            destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

            storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
            source_blob.delete()
            log_event("INFO", f"Moved cloud storage object blob {file} to GCS target {archive_path}", table=table)


def get_latest_watermark(table_name): 
    """Fetches high-watermark boundaries to guarantee delta ingestion control."""
    if IS_LOCAL:
        return "1900-01-01 00:00:00"
    else:
        query = f"""
            SELECT MAX(load_timestamp) AS latest_timestamp
            FROM `{BQ_CONFIG_TABLE}`
            WHERE tablename = '{table_name}'
        """
        query_job = bq_client.query(query)
        result = list(query_job.result())
        
        if not result or result[0].latest_timestamp is None:
            return "1900-01-01 00:00:00"
            
        return str(result[0].latest_timestamp)


def extract_and_save_to_landing(table, loadtype, watermark_col):
    """Pulls row logs from relational endpoints and dumps them into JSON lines."""
    try:
        last_watermark = get_latest_watermark(table) if loadtype.lower() == "increment" else None
        log_event("INFO", f"Latest operational watermark for {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if loadtype.lower() == "full" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())

        log_event("SUCCESS", f"Successfully extracted data from relational database table {table}", table=table)
        today = datetime.datetime.today().strftime('%d%m%Y')

        if IS_LOCAL:
            target_output_dir = os.path.join(LANDING_PATH, table)
            os.makedirs(target_output_dir, exist_ok=True)
            target_output_file = os.path.join(target_output_dir, f"{table}_{today}.json")
            
            df.toPandas().to_json(target_output_file, orient="records", lines=True)
            log_event("SUCCESS", f"JSON lines dataset dumped straight to local folder path: {target_output_file}", table=table)
        else:
            JSON_FILE_PATH = f"landing/{SANPARK_NAME}/{table}/{table}_{today}.json"
            bucket = storage_client.bucket(GCS_BUCKET)
            blob = bucket.blob(JSON_FILE_PATH)
            blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")
            log_event("SUCCESS", f"JSON cloud object safely written to GCS at gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
    except Exception as e:
        log_event("ERROR", f"Error caught processing extraction loop parameters for {table}: {str(e)}", table=table)


def read_config_file():
    """Reads configuration spreadsheet matrices to manage dynamic loop processing."""
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "Successfully opened pipeline master configuration matrices.")
    return df

config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1':
        db, table, loadtype, watermark, _, target_path = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, loadtype, watermark)
