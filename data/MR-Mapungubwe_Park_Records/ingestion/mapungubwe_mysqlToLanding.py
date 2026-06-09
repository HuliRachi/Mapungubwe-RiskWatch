from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

spark = SparkSession.builder.appName("KrugerMysqlToLanding").getOrCreate()

# initialise GCS and BigQuery clients
storage_client = storage.Client()
bq_client = bigquery.Client()

#GCP configuration
GCS_BUCKET = "mapungubwe-bucket" 
SANPARK_NAME = "mapungubwe"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{SANPARK_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{SANPARK_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/config.csv"

#BigQuery configuration
BQ_PROJECT = "project-a2ce378b-71f9-4087-95b" 
BQ_CONFIG_TABLE = f"{BQ_PROJECT}.temp_dataset.config"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

#mysql configuration
MYSQL_CONFIG ={ 
    # Added &zeroDateTimeBehavior=convertToNull to the connection URL string
    "url": "jdbc:mysql://34.35.134.22:3306/mapungubwe-db?useSSL=true&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=convertToNull", 
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Rachyhuly@98"
}


#step 2 - initialize logging mechanism
log_entries = []

def log_event(event_type, message, table=None):
    """log event and store it in the log list"""
    log_entry ={
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table":table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}]{event_type} - {message}")

#step 3 - save logging to GCS
def save_logs_to_gcs():
    """save logs to a json file and upload to gcs"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"
    json_data = json.dumps(log_entries, indent=4)

    #get gcs bucket
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    #upload json data as a file
    blob.upload_from_string(json_data, content_type="application/json")

    print(f" Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")


def save_logs_to_bigquery():
    """save logs to bigquery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery").option("table", BQ_LOG_TABLE).option("temporaryGcsBucket", BQ_TEMP_PATH).mode("append").save()
        print("Logs stored in Bigquery for future analysis")

#step 6 - function to move existing files to archive

def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{SANPARK_NAME}/{table}/")) ##step 6, 1
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return
    
    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extract Date from File Name e.g patients_24032025.json
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        # Move to Archive
        archive_path = f"landing/{SANPARK_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copy file to archive and delete original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

# function to get latest watermark
def get_latest_watermark(table_name): 
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_CONFIG_TABLE}`
        WHERE tablename = '{table_name}'
    """
    query_job = bq_client.query(query)
    result = list(query_job.result())
    
    # Check if results list is empty, or if the field value itself is None
    if not result or result[0].latest_timestamp is None:
        return "1900-01-01 00:00:00"
        
    return str(result[0].latest_timestamp)



# Step 7 Function to Extract Data from MySQL and Save to GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try: ## check first if the file is incremental or full
        last_watermark = get_latest_watermark(table) if load_type.lower() == "increment" else None ##if is incremental go to audit table/config table, get the timestamp using select
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t" ##if is full just query it, if is incremental query it using latest date

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())

        log_event("SUCCESS", f" Successfully extracted data from {table}", table=table)

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{SANPARK_NAME}/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")

        log_event("SUCCESS", f" JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
        # Insert Audit Entry
        audit_df = spark.createDataFrame([ 
            (table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")], 
            ["tablename", "loadtype", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_CONFIG_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f" Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)



#step 1 - function to read config file from GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "Successfully read the config file")
    return df

config_df = read_config_file()

#show config_df

#loop config file
for row in config_df.collect():
    if row["is_active"] == '1' :
        db, table, load_type, watermark, _ , target_path = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()
