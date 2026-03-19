# Sale-ETL — README.md

---

```markdown
# 🚀 Sale-ETL — End-to-End Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.x-green)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange)
![BigQuery](https://img.shields.io/badge/BigQuery-Google%20Cloud-blue)
![Dataproc](https://img.shields.io/badge/Dataproc-Google%20Cloud-yellow)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Stages](#pipeline-stages)
- [Data Sources](#data-sources)
- [Transformations](#transformations)
- [BigQuery Schema](#bigquery-schema)
- [Analytics Views](#analytics-views)
- [Edge Cases Handled](#edge-cases-handled)
- [Prerequisites](#prerequisites)
- [Setup & Installation](#setup--installation)
- [Running the Pipeline](#running-the-pipeline)
- [Looker Studio Dashboards](#looker-studio-dashboards)
- [Troubleshooting](#troubleshooting)
- [Design Decisions](#design-decisions)
- [Lessons Learned](#lessons-learned)

---

## 📖 Project Overview

**Sale-ETL** is a fully automated, cloud-native, end-to-end data engineering
pipeline built on Apache Airflow, Google Cloud Platform (GCP), and Apache Spark.

The pipeline ingests raw e-commerce sales data from 8 CSV source files,
applies enterprise-grade transformations using Spark on Dataproc, loads
clean structured data into partitioned and clustered BigQuery tables, and
exposes 8 analytical views for business intelligence reporting via
Looker Studio.

### Business Problem

A mid-size e-commerce company generates daily transactional data across
orders, payments, reviews, customers, sellers, products, and geolocation
records. This data arrives as raw CSV files with:

- Mixed date formats and invalid timestamps
- Duplicate transaction records
- Missing or null foreign key references
- Special characters and null synonyms in string fields
- No unified analytical layer for business reporting

**Sale-ETL solves this by building a fully automated pipeline that
transforms raw messy CSV data into clean, partitioned, analytics-ready
BigQuery tables and views — refreshed daily with zero manual intervention.**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SALE-ETL ARCHITECTURE                        │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              LOCAL ENVIRONMENT (Docker)                   │  │
│  │  Apache Airflow — Webserver, Scheduler, Worker, Postgres  │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │ GCP API Calls                     │
│  ┌──────────────────────────▼───────────────────────────────┐  │
│  │                 GOOGLE CLOUD PLATFORM                     │  │
│  │                                                           │  │
│  │  ┌─────────────────────────────────────────────────┐    │  │
│  │  │           Google Cloud Storage (GCS)             │    │  │
│  │  │  sale-etl-source-bucket → source_zone/           │    │  │
│  │  │  sale-etl-main-bucket   → landing_zone/          │    │  │
│  │  │                         → processed_zone/        │    │  │
│  │  │                         → spark_jobs/            │    │  │
│  │  └─────────────────────────────────────────────────┘    │  │
│  │                                                           │  │
│  │  ┌──────────────────┐    ┌──────────────────────────┐   │  │
│  │  │  GCP Dataproc    │    │      BigQuery            │   │  │
│  │  │  Spark Cluster   │    │  sales_raw    (8 tables) │   │  │
│  │  │  Auto-created    │    │  sales_analytics (8 views│   │  │
│  │  │  Auto-deleted    │    └──────────────┬───────────┘   │  │
│  │  └──────────────────┘                   │               │  │
│  │                              ┌──────────▼──────────┐    │  │
│  │                              │   Looker Studio     │    │  │
│  │                              │  5 Dashboards       │    │  │
│  │                              └─────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
[8 CSV Files]
     │
     ▼
[source_staging_dag]  →  Validate + Upload to GCS source bucket
     │
     ▼ triggers
[etl_pipeline_dag]
  Stage 0 → Data freshness check
  Stage 1 → Ingest to GCS landing zone
  Stage 2 → Spark transform on Dataproc
  Stage 3 → Load to BigQuery (staging → final)
  Stage 4 → Create 8 analytics views
     │
     ▼
[Looker Studio]  →  5 Business Dashboards
```

---

## 🛠️ Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Orchestration | Apache Airflow | 3.x |
| Containerisation | Docker + Docker Compose | Latest |
| Compute | Apache Spark on GCP Dataproc | 3.x |
| Storage | Google Cloud Storage | — |
| Data Warehouse | Google BigQuery | — |
| Visualisation | Looker Studio | — |
| Language | Python | 3.12 |
| Query Language | BigQuery Standard SQL | — |
| Version Control | Git | — |

---

## 📁 Project Structure

```
Sale-ETL/
│
├── dags/
│   ├── config.py                    # All configuration constants
│   ├── etl_pipeline_dag.py          # Main ETL pipeline DAG (44+ tasks)
│   └── source_staging_dag.py        # Source file validation + upload DAG
│
├── spark_jobs/
│   ├── spark_utils.py               # Shared transformation utilities
│   ├── transform_orders.py
│   ├── transform_order_items.py
│   ├── transform_order_payments.py
│   ├── transform_order_reviews.py
│   ├── transform_customers.py
│   ├── transform_sellers.py
│   ├── transform_products.py
│   └── transform_geolocations.py
│
├── sql/
│   └── views/
│       ├── vw_order_summary.sql
│       ├── vw_daily_sales_performance.sql
│       ├── vw_customer_profile.sql
│       ├── vw_seller_performance.sql
│       ├── vw_product_performance.sql
│       ├── vw_payment_analysis.sql
│       ├── vw_review_sentiment.sql
│       └── vw_geo_sales_heatmap.sql
│
├── data/
│   └── *.csv                        # Source CSV files (not committed)
│
├── credentials/                     # GCP service account key (not committed)
├── logs/                            # Airflow task logs (not committed)
├── plugins/                         # Airflow plugins
├── docker-compose.yml               # Airflow local environment
├── .env                             # Environment variables (not committed)
├── .env.example                     # Environment variables template
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔄 Pipeline Stages

### DAG 1 — source_staging_dag

| Task | Description |
|------|-------------|
| validate_source_files | Validates all 8 CSV files exist and are non-empty |
| upload_{table}_to_gcs × 8 | Uploads each CSV to GCS source bucket (parallel) |
| update_watermark | Records successful upload timestamp |
| trigger_etl_pipeline | Triggers etl_pipeline_dag |

### DAG 2 — etl_pipeline_dag (44+ tasks)

| Stage | Tasks | Description |
|-------|-------|-------------|
| Stage 0 | 1 task | Data freshness check |
| Stage 1 | 8 tasks | Ingest CSV to GCS landing zone (parallel) |
| Stage 2a | 9 tasks | Upload Spark scripts to GCS (parallel) |
| Stage 2b | 1 task | Create Dataproc cluster |
| Stage 2c | 8 tasks | Run Spark transformation jobs (parallel) |
| Stage 2d | 1 task | Delete Dataproc cluster (ALL_DONE) |
| Stage 3a | 2 tasks | Create BQ datasets (parallel) |
| Stage 3b | 8 tasks | Create BQ tables (parallel) |
| Stage 3c | 8 tasks | Load staging tables (parallel) |
| Stage 3d | 12 tasks | Fact dedup insert + Dimension merge (parallel) |
| Stage 3e | 8 tasks | Drop staging tables (parallel) |
| Stage 3f | 1 task | BQ load complete checkpoint |
| Stage 4 | 8 tasks | Create analytics views (parallel) |
| Stage 4f | 1 task | Pipeline complete |

**Total: 44+ tasks | Fully idempotent | Safe to rerun**

---

## 📊 Data Sources

| Table | Description | Type | Partition | Cluster |
|-------|-------------|------|-----------|---------|
| orders | Order transactions | Fact | sale_date | — |
| order_items | Items per order | Fact | sale_date | — |
| order_payments | Payment records | Fact | sale_date | — |
| order_reviews | Customer reviews | Fact | sale_date | — |
| customers | Customer profiles | Dimension | — | country, gender |
| sellers | Seller profiles | Dimension | — | country, code |
| products | Product catalogue | Dimension | — | category |
| geolocations | Postal coordinates | Dimension | — | country, postal |

---

## ⚡ Transformations

### Common (All Tables — spark_utils.py)

- ✅ String cleaning — removes control characters, normalises whitespace
- ✅ Null synonym replacement — "null", "none", "na", "n/a", "nan" → NULL
- ✅ Type enforcement — STRING, FLOAT64, INT64, DATE, TIMESTAMP
- ✅ Postal codes preserved as STRING (leading zeros)
- ✅ ETL timestamp added to every table

### Table-Specific Highlights

| Table | Key Derived Columns |
|-------|-------------------|
| orders | sale_date, sale_year, sale_month, sale_day_of_week, is_delivered, is_canceled, delivery_delay_days, shipping_days, order_cycle_days |
| order_items | total_item_value (price + freight), sale_date from orders join |
| order_payments | sale_date from orders join |
| order_reviews | review_sentiment (positive/neutral/negative), sale_date from orders join |
| customers | age_group, customer_segment |
| sellers | seller_country (full name from code) |
| products | product_volume_cm3 (l × h × w) |
| geolocations | Deduplicated by AVG(lat/lon) per postal code |

### Output Format

```
Format    : Parquet (Snappy compressed)
Location  : gs://sale-etl-main-bucket/
            processed_zone/{table}/
            partition=YYYY-MM-DD/
            part-0000*.snappy.parquet
Benefits  : 60-70% smaller than CSV
            Columnar — faster BQ load
            Schema embedded in file
```

---

## 🗄️ BigQuery Schema

### sales_raw Dataset — Final Column Counts

| Table | Source Cols | Final Cols | New Derived |
|-------|------------|------------|-------------|
| orders | 8 | 23 | +15 |
| order_items | 7 | 10 | +3 |
| order_payments | 5 | 7 | +2 |
| order_reviews | 7 | 10 | +3 |
| customers | 8 | 11 | +3 |
| sellers | 4 | 8 | +4 |
| products | 6 | 8 | +2 |
| geolocations | 5 | 6 | +1 |
| **Total** | **50** | **83** | **+33** |

### Load Patterns

**Fact Tables (orders, order_items, order_payments, order_reviews)**
```sql
-- Step 1: Delete today's partition
DELETE FROM `sales_raw.{table}`
WHERE sale_date = DATE('{execution_date}')

-- Step 2: Insert deduplicated records
INSERT INTO `sales_raw.{table}`
SELECT ... FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY {business_key}
        ORDER BY etl_timestamp DESC
    ) AS _rn
    FROM `sales_raw.{table}_staging`
) WHERE _rn = 1
```

**Dimension Tables (customers, sellers, products, geolocations)**
```sql
MERGE `sales_raw.{table}` T
USING `sales_raw.{table}_staging` S
ON T.{business_key} = S.{business_key}
WHEN MATCHED     THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

---

## 📈 Analytics Views

All 8 views live in the `sales_analytics` dataset.
SQL files stored in `sql/views/` — dbt-style pattern.

| View | Tables Joined | Granularity | Business Use |
|------|--------------|-------------|--------------|
| vw_order_summary | 3 | Per order | Master order fact |
| vw_daily_sales_performance | 3 | Per day | Revenue trends |
| vw_customer_profile | 4 | Per customer | CRM + LTV |
| vw_seller_performance | 5 | Per seller | Seller scorecards |
| vw_product_performance | 4 | Per product | Catalogue analysis |
| vw_payment_analysis | 2 | Per type + month | Finance reporting |
| vw_review_sentiment | 3 | Per review | CX quality |
| vw_geo_sales_heatmap | 5 | Per city + month | Geo dashboard |

### SQL File Pattern

```python
# Views use placeholder replacement at runtime
# No hardcoded project/dataset names in SQL files

_load_sql("vw_order_summary.sql")
# Replaces:
#   {project}           → sale-etl
#   {raw_dataset}       → sales_raw
#   {analytics_dataset} → sales_analytics
```

---

## 🛡️ Edge Cases Handled

| Edge Case | Detection | Strategy |
|-----------|-----------|----------|
| Empty CSV files | df.count() == 0 | Skip + warn, don't fail DAG |
| Missing columns | Schema comparison | Add as NULL, log warning |
| Duplicate primary keys | ROW_NUMBER() window | Keep latest by etl_timestamp |
| Null foreign keys | isNull() check | Flag with boolean column |
| Invalid date formats | Multi-format parser | Set to NULL, log count |
| Special characters | Regex UDF | Clean or replace with NULL |
| Cluster creation failure | retries=2 | Retry + ALL_DONE cleanup |
| BigQuery load failure | GoogleAPICallError | Retry 3x + XCom error push |
| Stale source data | Watermark check | Halt pipeline gracefully |
| Geo join fan-out | CTE dedup | AVG lat/lon per postal code |
| Division by zero | SAFE_DIVIDE() | Returns NULL instead of error |
| Postal code leading zeros | STRING type | Never cast to INTEGER |

---

## ✅ Prerequisites

| Requirement | Version |
|-------------|---------|
| Docker Desktop | Latest |
| Docker Compose | v2.x+ |
| Python | 3.12+ |
| GCP Account + Project | — |
| gcloud CLI | Latest |
| Git | Latest |
| RAM | 8 GB minimum |
| Disk | 20 GB minimum |

---

## ⚙️ Setup & Installation

### Step 1 — Clone Repository

```bash
git clone https://github.com/{your-username}/sale-etl.git
cd sale-etl
```

### Step 2 — GCP Setup

```bash
# Enable required APIs
gcloud services enable \
  storage.googleapis.com \
  dataproc.googleapis.com \
  bigquery.googleapis.com \
  compute.googleapis.com

# Create service account
gcloud iam service-accounts create sale-etl-sa \
  --display-name="Sale ETL Service Account"

# Grant roles
for role in storage.admin dataproc.admin bigquery.admin compute.admin; do
  gcloud projects add-iam-policy-binding sale-etl \
    --member="serviceAccount:sale-etl-sa@sale-etl.iam.gserviceaccount.com" \
    --role="roles/${role}"
done

# Download key
gcloud iam service-accounts keys create \
  ./credentials/sale-etl-sa.json \
  --iam-account=sale-etl-sa@sale-etl.iam.gserviceaccount.com

# Create GCS buckets
gsutil mb -p sale-etl -l {YOUR_REGION} gs://sale-etl-source-bucket
gsutil mb -p sale-etl -l {YOUR_REGION} gs://sale-etl-main-bucket
```

### Step 3 — Configure Environment

```bash
# Copy template
cp .env.example .env

# Edit .env with your values
nano .env
```

```bash
# .env contents
AIRFLOW_UID=50000
GCP_PROJECT_ID=sale-etl
GCS_SOURCE_BUCKET=sale-etl-source-bucket
GCS_MAIN_BUCKET=sale-etl-main-bucket
BQ_RAW_DATASET=sales_raw
BQ_ANALYTICS_DATASET=sales_analytics
DATAPROC_REGION=your-region
CLUSTER_NAME=sale-etl-cluster
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/credentials/sale-etl-sa.json
```

### Step 4 — Create Required Folders

```bash
mkdir -p credentials logs plugins sql/views data
```

### Step 5 — Start Airflow

```bash
# Initialise database
docker compose up airflow-init

# Start all services
docker compose up -d

# Verify containers
docker compose ps
```

### Step 6 — Configure GCP Connection in Airflow

```
1. Open http://localhost:8080
   Username: airflow | Password: airflow

2. Admin → Connections → + Add

3. Fill in:
   Connection Id   : google_cloud_default
   Connection Type : Google Cloud
   Project Id      : sale-etl
   Keyfile Path    : /opt/airflow/credentials/sale-etl-sa.json

4. Save → Test Connection ✅
```

---

## ▶️ Running the Pipeline

### Step 1 — Add Source CSV Files

```bash
cp /path/to/your/csvs/*.csv ./data/
```

Required files:
```
data/orders.csv
data/order_items.csv
data/order_payments.csv
data/order_reviews.csv
data/customers.csv
data/sellers.csv
data/products.csv
data/geolocations.csv
```

### Step 2 — Trigger source_staging_dag

```
Airflow UI → DAGs → source_staging_dag
→ Toggle ON → Click ▶ Trigger DAG
→ Wait for all tasks ✅
```

### Step 3 — Monitor etl_pipeline_dag

```
Triggers automatically after source_staging_dag.

Expected runtime:
  Stage 0 → ~10 seconds
  Stage 1 → ~1 minute
  Stage 2 → ~8-10 minutes (Spark)
  Stage 3 → ~3-4 minutes (BigQuery)
  Stage 4 → ~1 minute (Views)
  Total   → ~15 minutes
```

### Step 4 — Verify in BigQuery

```sql
-- Verify all tables loaded
SELECT 'orders'        , COUNT(*) FROM `sale-etl.sales_raw.orders`
UNION ALL
SELECT 'order_items'   , COUNT(*) FROM `sale-etl.sales_raw.order_items`
UNION ALL
SELECT 'order_payments', COUNT(*) FROM `sale-etl.sales_raw.order_payments`
UNION ALL
SELECT 'order_reviews' , COUNT(*) FROM `sale-etl.sales_raw.order_reviews`
UNION ALL
SELECT 'customers'     , COUNT(*) FROM `sale-etl.sales_raw.customers`
UNION ALL
SELECT 'sellers'       , COUNT(*) FROM `sale-etl.sales_raw.sellers`
UNION ALL
SELECT 'products'      , COUNT(*) FROM `sale-etl.sales_raw.products`
UNION ALL
SELECT 'geolocations'  , COUNT(*) FROM `sale-etl.sales_raw.geolocations`;

-- Verify all views exist
SELECT table_name
FROM `sale-etl.sales_analytics.INFORMATION_SCHEMA.VIEWS`
ORDER BY table_name;
```

---

## 📊 Looker Studio Dashboards

Connected to `sales_analytics` dataset views.

| Dashboard | Connected View | Key Charts |
|-----------|---------------|------------|
| Sales Overview | vw_daily_sales_performance | Revenue trend, order status, AOV |
| Customer Analysis | vw_customer_profile + vw_geo_heatmap | LTV, segments, geo map |
| Seller Performance | vw_seller_performance | Top sellers, review scores, delays |
| Product Analysis | vw_product_performance | Top categories, freight analysis |
| Payment & Sentiment | vw_payment_analysis + vw_review_sentiment | Payment mix, sentiment trend |

---

## 🔧 Troubleshooting

| Problem | Cause | Solution |
|---------|-------|----------|
| DAG not in UI | Import error | `docker logs {scheduler}` |
| GCP auth error | Wrong credentials | Check volume mount + connection |
| Spark job fails | Script not in GCS | Check upload_spark_script tasks |
| Parquet not found | Spark failed | Check Dataproc logs in GCP console |
| BQ table not found | Dataset missing | Check create_bq_raw_dataset task |
| View fails | Column mismatch | Run SQL in BQ console manually |
| Cluster not deleted | ALL_DONE missing | Delete manually via GCP console |
| DAG import timeout | Large config | Set DAGBAG_IMPORT_TIMEOUT=120 |
| sale_date NULL | Missing orders JOIN | Check dedup INSERT SQL |
| Docker OOM | Low memory | Increase Docker memory to 8GB |

---

## 💡 Design Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| Orchestration | Airflow (Docker) | Industry standard, free, full control |
| Transformation | Spark on Dataproc | Scalable, distributed, GCS native |
| Storage format | Parquet + Snappy | 60-70% smaller, faster BQ load |
| Fact load pattern | DELETE + INSERT dedup | Idempotent, handles reruns safely |
| Dimension load | MERGE on business key | Upsert — handles new + updated |
| Staging tables | Load staging first | Protects final tables from partial loads |
| SQL files | sql/views/ folder | dbt-style, debuggable, maintainable |
| Two datasets | sales_raw + sales_analytics | Separation of concerns |
| Cluster lifecycle | Create per run + delete | No idle cluster costs |
| Two DAGs | source_staging + etl | Single responsibility principle |

---

## 📚 Lessons Learned

1. Always use explicit column lists in BigQuery INSERT — never positional
2. Test Spark jobs locally before deploying to Dataproc
3. Cleanup tasks must use `TriggerRule.ALL_DONE` — not `ALL_SUCCESS`
4. Postal codes must always be STRING — never INTEGER
5. Check join cardinality before writing views
6. Mount all required folders as Docker volumes
7. Use BigQuery Standard SQL types — FLOAT64, INT64 not FLOAT, INTEGER
8. Staging pattern protects final tables — always worth the extra step
9. Separation of concerns (SQL files, config.py, spark_utils.py) saves debugging time
10. Draw dependency diagram before writing DAG code

---

## 📋 Environment Variables Reference

| Variable | Example | Purpose |
|----------|---------|---------|
| AIRFLOW_UID | 50000 | Airflow container user ID |
| GCP_PROJECT_ID | sale-etl | GCP project name |
| GCS_SOURCE_BUCKET | sale-etl-source-bucket | Source CSV bucket |
| GCS_MAIN_BUCKET | sale-etl-main-bucket | Main pipeline bucket |
| BQ_RAW_DATASET | sales_raw | Raw tables dataset |
| BQ_ANALYTICS_DATASET | sales_analytics | Views dataset |
| DATAPROC_REGION | us-central1 | Dataproc cluster region |
| CLUSTER_NAME | sale-etl-cluster | Dataproc cluster name |
| GOOGLE_APPLICATION_CREDENTIALS | /opt/airflow/credentials/sale-etl-sa.json | GCP auth key |

---

## 🚫 .gitignore

```
# Credentials — never commit
credentials/
*.json

# Environment variables — never commit
.env

# Data files — too large for git
data/

# Airflow logs
logs/

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/

# Docker
.docker/

# IDE
.vscode/
.idea/
*.DS_Store
```

---

## 📄 License

```
MIT License — free to use, modify and distribute.
```

---

## 👤 Author
Ankit kumar singh, Data Engineering Intern at Rakuten India

```
Built as part of Data Engineering Assignment 2
End-to-End ETL Pipeline — GCP + Airflow + Spark + BigQuery
```

---

*Built using Apache Airflow, Apache Spark,
Google Cloud Platform, and BigQuery*
```

---

