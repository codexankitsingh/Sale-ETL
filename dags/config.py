# dags/config.py
"""
Central configuration for the Sale-ETL pipeline.
All values read from environment variables with sensible defaults.
"""

import os

# ── GCP Core ───────────────────────────────────────────────
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "sale-etl")
GCP_CONN_ID    = os.environ.get("GCP_CONN_ID",    "google_cloud_default")

# ── GCS Buckets ────────────────────────────────────────────
GCS_SOURCE_BUCKET = os.environ.get("GCS_SOURCE_BUCKET", "sale-etl-source-bucket")
GCS_MAIN_BUCKET   = os.environ.get("GCS_MAIN_BUCKET",   "sale-etl-main-bucket")

# ── GCS Zone Prefixes ──────────────────────────────────────
LANDING_ZONE_PREFIX   = os.environ.get("LANDING_ZONE_PREFIX",   "landing_zone")
PROCESSED_ZONE_PREFIX = os.environ.get("PROCESSED_ZONE_PREFIX", "processed_zone")

# ── Local Paths ────────────────────────────────────────────
LOCAL_DATA_PATH       = os.environ.get("LOCAL_DATA_PATH",       "/opt/airflow/data")
SPARK_JOBS_LOCAL_PATH = os.environ.get("SPARK_JOBS_LOCAL_PATH", "/opt/airflow/spark_jobs")

# ── GCS Spark Jobs Prefix ──────────────────────────────────
SPARK_JOBS_GCS_PREFIX = os.environ.get("SPARK_JOBS_GCS_PREFIX", "spark_jobs")

# ── Tables ─────────────────────────────────────────────────
TABLE_NAMES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "geolocations",
    "customers",
    "sellers",
]

# ── Spark Scripts ──────────────────────────────────────────
SPARK_SCRIPTS = [
    "utils.py",
    "transform_orders.py",
    "transform_order_items.py",
    "transform_order_payments.py",
    "transform_order_reviews.py",
    "transform_products.py",
    "transform_geolocations.py",
    "transform_customers.py",
    "transform_sellers.py",
]

# ── Dataproc ───────────────────────────────────────────────
DATAPROC_CLUSTER_NAME        = os.environ.get("DATAPROC_CLUSTER_NAME",        "sale-etl-spark-cluster")
DATAPROC_REGION              = os.environ.get("DATAPROC_REGION",              "us-east1")
DATAPROC_NUM_WORKERS         = int(os.environ.get("DATAPROC_NUM_WORKERS",     "0"))
DATAPROC_MASTER_MACHINE_TYPE = os.environ.get("DATAPROC_MASTER_MACHINE_TYPE", "e2-standard-4")
DATAPROC_WORKER_MACHINE_TYPE = os.environ.get("DATAPROC_WORKER_MACHINE_TYPE", "e2-standard-4")
DATAPROC_IMAGE_VERSION       = os.environ.get("DATAPROC_IMAGE_VERSION",       "2.1-debian11")

# ── BigQuery Datasets ──────────────────────────────────────
BQ_RAW_DATASET       = os.environ.get("BQ_RAW_DATASET",       "sales_raw")
BQ_ANALYTICS_DATASET = os.environ.get("BQ_ANALYTICS_DATASET", "sales_analytics")
BQ_LOCATION          = os.environ.get("BIGQUERY_LOCATION",     "US")

# ── BigQuery Table Names ───────────────────────────────────
BQ_TABLES = {
    "orders"        : f"{GCP_PROJECT_ID}.sales_raw.orders",
    "order_items"   : f"{GCP_PROJECT_ID}.sales_raw.order_items",
    "order_payments": f"{GCP_PROJECT_ID}.sales_raw.order_payments",
    "order_reviews" : f"{GCP_PROJECT_ID}.sales_raw.order_reviews",
    "customers"     : f"{GCP_PROJECT_ID}.sales_raw.customers",
    "sellers"       : f"{GCP_PROJECT_ID}.sales_raw.sellers",
    "products"      : f"{GCP_PROJECT_ID}.sales_raw.products",
    "geolocations"  : f"{GCP_PROJECT_ID}.sales_raw.geolocations",
}

# ── BigQuery Staging Table Names ───────────────────────────
BQ_STAGING_TABLES = {
    table: f"{GCP_PROJECT_ID}.sales_raw.{table}_staging"
    for table in BQ_TABLES
}

# ── BigQuery Business Keys (for deduplication) ─────────────
BQ_BUSINESS_KEYS = {
    "orders"        : ["order_id"],
    "order_items"   : ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews" : ["review_id"],
    "customers"     : ["customer_trx_id"],
    "sellers"       : ["seller_id"],
    "products"      : ["product_id"],
    "geolocations"  : ["geo_postal_code"],
}

# ── Fact vs Dimension classification ──────────────────────
# Fact tables: partitioned by sale_date
# Dimension tables: clustered by business key
BQ_FACT_TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
]
BQ_DIMENSION_TABLES = [
    "customers",
    "sellers",
    "products",
    "geolocations",
]

# ── Watermark ──────────────────────────────────────────────
BQ_WATERMARK_DATASET = os.environ.get("BQ_WATERMARK_DATASET", "sales_raw")
BQ_WATERMARK_TABLE   = os.environ.get("BQ_WATERMARK_TABLE",   "etl_watermark")
BQ_WATERMARK_FULL    = f"{GCP_PROJECT_ID}.{BQ_WATERMARK_DATASET}.{BQ_WATERMARK_TABLE}"
MAX_FRESHNESS_HOURS  = int(os.environ.get("MAX_FRESHNESS_HOURS", "25"))


# ── SQL Files ──────────────────────────────────────────────
SQL_VIEWS_PATH = os.environ.get(
    "SQL_VIEWS_PATH",
    "/opt/airflow/sql/views"
)

# ── Analytics Views ────────────────────────────────────────
ANALYTICS_VIEWS = [
    "vw_order_summary",
    "vw_daily_sales_performance",
    "vw_customer_profile",
    "vw_seller_performance",
    "vw_product_performance",
    "vw_payment_analysis",
    "vw_review_sentiment",
    "vw_geo_sales_heatmap",
]
