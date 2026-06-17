## 🧳 For Recruiters: 2-Minute Local Quick Start

If you have cloned this repository to evaluate my engineering framework, follow these rapid steps to spin up the full architecture and see all pipeline tracks go green instantly.

### 🔌 Step 1: Clone & Verify the Filesystem

Ensure you are in the project's root folder where the `docker-compose.yml` sits, and verify that your local Docker Desktop application is currently running.

### 🚀 Step 2: One-Command

Run this command to spin up the docker

```bash
docker compose up -d
```

### ⏳ Step 3: Wait 15 Seconds for Health Check Verification

Give the multi-node container grid a few moments to finish running its background entry handshakes. Run this verification command:

```bash
docker ps
```

_Make sure all 6 core components (`airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `postgres`, `redis`, and `mapungubwe_mock_cloud_sql`) list an **Up** status._

### 🖥️ Step 4: Access the Command Center UI

Open your desktop web browser and navigate straight to the Airflow control dashboard:
👉 **[http://localhost:8080](http://localhost:8080)**

- **Username:** `airflow`
- **Password:** `airflow1234`

### 🟢 Step 5: Trigger the Ingestion Framework

1. Locate the **`parent_dag`** on your screen.
2. Click the blue toggle box on the left of all DAGS to turn them **Active**.
3. Click the **Trigger / Play** button on the far right of that row.
4. _Watch the automation sequence execute:_ `parent_dag` ➡️ `pyspark_dag` ➡️ `bigquery_dag`. Every single task lane ring will light up as a flawless solid **Green** 🟢!

### 🗄️ Step 6: Verify the Target Warehouse Rows

To prove that the pipeline successfully streamed the mock logs straight into the warehouse, run this single command in your laptop's terminal to inspect the internal Postgres storage layer:

```bash
docker exec -it postgres psql -U rachuhuli -d mapungubwe_db -c "SELECT * FROM bronze_dataset.fact_incidents LIMIT 2;"
```
