import os
import csv
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

# =========================================================================
# 🐳 1. CONNECTION MANAGEMENT (Your exact PostgresHook style)
# =========================================================================
def get_conn_cursor():
    """Establishes connection utilizing Airflow's built-in hooks system."""
    # Matches the connection ID you established in your .env file
    hook = PostgresHook(postgres_conn_id="POSTGRES_DB_YT_ELT")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur

def close_conn_cursor(conn, cur):
    """Safely closes connections."""
    cur.close()
    conn.close()

# =========================================================================
# 🚀 2. DDL INITIALIZATION LAYERS (Your exact schema logic)
# =========================================================================

# create schema
def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    print(f"✔️ Schema checked/created: {schema}")

# create tables
def create_table(schema, table):
    conn, cur = get_conn_cursor()

    if table == "dim_date":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                date_id     int          not null,
                date        TIMESTAMP    not null,
                month       int          not null,
                season      varchar(20)  not null,
                is_weekend  boolean      not null,
                constraint pk_dim_date_{schema} primary key (date_id)
            );
        """
    elif table == "dim_ranger":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                ranger_id               int         not null,
                ranger_name             varchar(50) not null,
                years_of_experience     int         not null,
                constraint pk_dim_ranger_{schema} primary key (ranger_id)
            );
        """
    elif table == "dim_zone":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                zone_id                     int         not null,
                zone_name                   varchar(50) not null,
                habitat_type                varchar(50) not null,
                distance_from_gate          int         not null,
                historical_poaching_count   int         not null,
                constraint pk_dim_zone_{schema} primary key (zone_id)
            );
        """
    elif table == "fact_incidents":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                incident_id         int         not null,
                date_id             int         not null,
                date                TIMESTAMP   not null,
                zone_id             int         not null,
                incident_type       varchar(50) not null,
                animals_involved    int         not null,
                outcome             varchar(20) not null,
                animal             varchar(20) not null,
                constraint pk_fact_incidents_{schema} primary key (incident_id)
            );
        """
    elif table == "fact_patrol":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                patrol_id       int             not null,
                date_id         int             not null,
                date            TIMESTAMP       not null,
                zone_id         int             not null,
                ranger_id       int             not null,
                hours_patrolled int             not null,
                sightings_count int             not null,
                risk_score      decimal(10,2)   not null,
                constraint pk_fact_patrol_{schema} primary key (patrol_id)
            );
        """

    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    print(f"   ✔️ Table checked/created: {schema}.{table}")

# =========================================================================
# 🔄 3. CSV EXTRACTION & SEED LOGIC (Bridging your pipeline files)
# =========================================================================
def load_csv_to_layer(schema, table, csv_file_path):
    """Parses raw CSV logs into structured data warehouse layers."""
    conn, cur = get_conn_cursor()
    
    # Clears old records to enforce primary key consistency across re-runs
    cur.execute(f"TRUNCATE TABLE {schema}.{table} CASCADE;")
    
    with open(csv_file_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        placeholders = ", ".join(["%s"] * len(header))
        columns = ", ".join(header)
        insert_sql = f"INSERT INTO {schema}.{table} ({columns}) VALUES ({placeholders})"
        
        for row in reader:
            cur.execute(insert_sql, row)
            
    conn.commit()
    close_conn_cursor(conn, cur)
    print(f"   🚀 Data populated successfully into: {schema}.{table}")
