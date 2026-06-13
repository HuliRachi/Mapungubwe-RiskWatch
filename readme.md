# 🐾 Mapungubwe RiskWatch: End-to-End Pipeline on GCP

<code>I dedicate this project to my role model <i>JOHN DEVAR</i> who is a surgeon, who inspired me to work hard since i was in my final year.</code>

`Outside my professional work i love camping in SANParks`

## 📍 What is this project?

This project helps **Mapungubwe National Park** solve and prevent poaching activities across its borders and eco-zones. I built an end-to-end data pipeline on **Google Cloud Platform (GCP)** that integrates ranger patrol metrics, incident indicators, and environmental attributes. This system transforms raw field logs into actionable risk scores, allowing park management to optimize ranger deployments and proactively secure high-risk zones.

---

## ⚠️ The Problem

Mapungubwe's unique open geography, river borders, and dense wildlife make it a continuous target for illegal trespassing and poaching.

- **Reactive Deployment:** Ranger patrols are often scheduled blindly without data-driven insights, leaving vulnerable habitats exposed.
- **Unquantified Risk Levels:** Park managers lacked a unified platform to cross-reference environmental variables (like seasons or distance from gates) with historical poaching activity to measure immediate threat levels.

---

## 💡 The Solution

I built a scalable "Data Pipeline" that automates the collection, cleaning, and structured loading of operational anti-poaching data.

The core data warehousing engine separates historical structural reference data (Dimension tables loaded via Full Load) from dynamic, incoming operational events (Fact tables loaded via Incremental Load). This allows the system to scale efficiently as field data grows daily.

### 📊 PowerBI Dashboard Analysis

<img src="images/Mapungubwe RiskWatch_Dasboard-Analysis.png" alt="PowerBI_Dashboard" width="700" height="350">

1. **Area Chart** - Displays the percentage risk levels recorded across the different conservation zones.
2. **Pie Chart** - Breaks down total anti-poaching incident classifications
3. **Stacked Bar Graph** - Shows poaching and trespassing cases arrested vs incidents still investigated.
4. **Matrix Table** - Shows most common months which these incidents happen.

### 🛠️ Tech Stack & Pipeline Engine

- **Orchestration:** Cloud Composer (Apache Airflow) for end-to-end workflow automation, task dependency management, and scheduling.
- **Storage & Data Lake:** Google Cloud Storage (GCS) structured into Landing, Bronze, Silver, and Gold storage tiers.
- **Relational Database:** Cloud SQL for storing CSV files from Mapungubwe National Park.
- **Data Processing:** Dataproc (Apache Spark) handling distributed big data cleaning, schema validation, and transformations.
- **Data Warehouse:** BigQuery for high-performance spatial analytics, star-schema optimization, and gold-layer aggregations.
- **CI/CD Automation:** Cloud Build integrated with GitHub for continuous integration of Airflow DAGs and Spark assets.
- **Security & Governance:** IAM roles and Service Accounts strictly configured with the principle of least privilege.

---

### 🗺️ Data Pipeline Architecture Diagram

<img src="images/Mapungubwe RiskWatch_Architecture-Diagram.png" alt="Architecture_Diagram" width="700" height="350">

- Raw CSV files (`dim_zone`, `dim_ranger`,`dim_date`, `fact_patrol`, `fact_incidents`) enter Google Cloud Storage in the Landing layer.
- Apache Airflow orchestrates the pipeline, moving the data to the Bronze layer using external tables.
- PySpark on Cloud Dataproc performs schema validation and quality checks (`is_quarantine`) to move clean data into the Silver layer.
- Finally, data is aggregated into business analytics models in the Gold layer inside BigQuery, feeding the Power BI application directly.

### 🗄️ Core Star Schema Architecture

<img src="images/Mapungubwe RiskWatch_ER-Diagram.png" alt="Schema_ER_Diagram" width="700" height="350">

The data lake models 5 distinct structural tables organized to map out field operations efficiently:

#### 1. Dimension Tables (Full Load Strategy)

- **`dim_zone`:** Manages 20 core conservation areas including specific habitats (Riverine, Savannah), gate distances, and baseline poaching counts.
- **`dim_ranger`:** Profiles 40 active field rangers along with their specific years of tactical experience.
- **`dim_date`:** Houses 3 full years (2023 - 2025) of chronological indicators, adjusting for South African seasonal cycles and weekends.

#### 2. Fact Tables (Incremental Load Strategy)

- **`fact_patrol`:** Scaled to 20,000 operational rows tracking total patrol hours, immediate wildlife/track sightings, and calculated risk scores.
- **`fact_incidents`:** Logs live poaching attempts, fence breaches, or snare findings, explicitly tracking outcomes and targeted animal species.

---

## 🚀 What this project shows

- **Data-Driven Security:** It transitions anti-poaching operations from reactive tracking to proactive risk forecasting.
- **Optimized Resource Deployment:** It identifies exactly which zones require senior rangers based on calculated threat metrics.
- **Habitat Intelligence:** It highlights correlation trends between specific terrain styles (like riverine boundaries) and seasonal spikes in illegal entry.

---

## 📈 Results

- **Targeted Patrols:** Rangers can be strategically deployed to zones with high risk scores rather than relying on random routing.
- **Protected Wildlife:** Swift visual tracking of snare and trespass incidents helps teams intercept threats before animal casualties occur.
- **Strategic Management:** Park directors can use long-term trend data to allocate budget resources to the most vulnerable geographic sectors.

---

Built and designed by Hulisani Ratshiedana  
**Contact:** rachyhuly17@gmail.com
