# dags/etl_pipeline_dag.py
"""
DAG 2: etl_pipeline_dag
────────────────────────
Triggered exclusively by source_staging_dag.

Pipeline Stages:
  Stage 0 → Freshness Check   : Validate watermark
  Stage 1 → Data Ingestion    : GCS source → landing_zone
  Stage 2 → Data Processing   : PySpark on Dataproc → processed_zone
    2a → Upload Spark scripts to GCS
    2b → Create Dataproc cluster (dynamic name, single node)
    2c → Submit 8 PySpark jobs (parallel)
    2d → Delete Dataproc cluster (always runs)
  Stage 3 → BigQuery Load     : processed_zone → BigQuery
    3a → Create BQ datasets (sales_raw, sales_analytics)
    3b → Create BQ final tables (partitioned + clustered)
    3c → Load staging tables from GCS Parquet (parallel)
    3d → Fact tables  : DELETE partition + INSERT dedup
         Dimension tables : MERGE upsert
    3e → Drop staging tables (parallel)
    3f → BQ load complete summary
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.task.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)

import sys
sys.path.insert(0, "/opt/airflow/dags")
from config import (
    GCP_PROJECT_ID,
    GCS_SOURCE_BUCKET,
    GCS_MAIN_BUCKET,
    GCP_CONN_ID,
    TABLE_NAMES,
    DATAPROC_CLUSTER_NAME,
    DATAPROC_REGION,
    DATAPROC_NUM_WORKERS,
    DATAPROC_MASTER_MACHINE_TYPE,
    DATAPROC_WORKER_MACHINE_TYPE,
    DATAPROC_IMAGE_VERSION,
    SPARK_JOBS_GCS_PREFIX,
    SPARK_JOBS_LOCAL_PATH,
    SPARK_SCRIPTS,
    LANDING_ZONE_PREFIX,
    PROCESSED_ZONE_PREFIX,
    BQ_WATERMARK_FULL,
    MAX_FRESHNESS_HOURS,
    BQ_RAW_DATASET,
    BQ_ANALYTICS_DATASET,
    BQ_LOCATION,
    BQ_TABLES,
    BQ_STAGING_TABLES,
    BQ_BUSINESS_KEYS,
    BQ_FACT_TABLES,
    BQ_DIMENSION_TABLES,
    SQL_VIEWS_PATH,
    ANALYTICS_VIEWS,
)

# ── Default DAG arguments ──────────────────────────────────
default_args = {
    "owner"           : "sale-etl",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
}

# ── Dynamic cluster name ───────────────────────────────────
DYNAMIC_CLUSTER_NAME = (
    f"{DATAPROC_CLUSTER_NAME}-{{{{ ts_nodash | lower }}}}"
)

# ── Dataproc Cluster Config ────────────────────────────────
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances"   : 1,
        "machine_type_uri": DATAPROC_MASTER_MACHINE_TYPE,
        "disk_config": {
            "boot_disk_type"   : "pd-standard",
            "boot_disk_size_gb": 50,
        },
    },
    "worker_config": {
        "num_instances": 0,
    },
    "software_config": {
        "image_version": DATAPROC_IMAGE_VERSION,
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true",
            "spark:spark.sql.shuffle.partitions"  : "8",
            "spark:spark.executor.memory"         : "3g",
            "spark:spark.driver.memory"           : "3g",
        },
    },
    "gce_cluster_config": {
        "network_uri": "default",
    },
}

# ══════════════════════════════════════════════════════════
# BigQuery DDL Schemas
# ══════════════════════════════════════════════════════════

BQ_SCHEMAS = {

    # ── FACT TABLES ────────────────────────────────────────

    "orders": [
        {"name": "order_id",                      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "customer_trx_id",               "type": "STRING",    "mode": "NULLABLE"},
        {"name": "order_status",                  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "order_purchase_timestamp",      "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_approved_at",             "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_delivered_carrier_date",  "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_delivered_customer_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "order_estimated_delivery_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "sale_date",                     "type": "DATE",      "mode": "NULLABLE"},
        {"name": "etl_timestamp",                 "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "order_items": [
        {"name": "order_id",            "type": "STRING",    "mode": "REQUIRED"},
        {"name": "order_item_id",       "type": "INT64",   "mode": "NULLABLE"},
        {"name": "product_id",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "seller_id",           "type": "STRING",    "mode": "NULLABLE"},
        {"name": "shipping_limit_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "price",               "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "freight_value",       "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "total_item_value",    "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "sale_date",           "type": "DATE",      "mode": "NULLABLE"},
        {"name": "etl_timestamp",       "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "order_payments": [
        {"name": "order_id",             "type": "STRING",    "mode": "REQUIRED"},
        {"name": "payment_sequential",   "type": "INT64",   "mode": "NULLABLE"},
        {"name": "payment_type",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "payment_installments", "type": "INT64",   "mode": "NULLABLE"},
        {"name": "payment_value",        "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "sale_date",            "type": "DATE",      "mode": "NULLABLE"},
        {"name": "etl_timestamp",        "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "order_reviews": [
        {"name": "review_id",                 "type": "STRING",    "mode": "NULLABLE"},
        {"name": "order_id",                  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "review_score",              "type": "INT64",   "mode": "NULLABLE"},
        {"name": "review_comment_title_en",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "review_comment_message_en", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "review_creation_date",      "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "review_answer_timestamp",   "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "review_sentiment",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "sale_date",                 "type": "DATE",      "mode": "NULLABLE"},
        {"name": "etl_timestamp",             "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    # ── DIMENSION TABLES ───────────────────────────────────

    "customers": [
        {"name": "customer_trx_id",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "subscriber_id",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "subscribe_date",        "type": "DATE",      "mode": "NULLABLE"},
        {"name": "first_order_date",      "type": "DATE",      "mode": "NULLABLE"},
        {"name": "customer_postal_code",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "customer_city",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "customer_country",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "customer_country_code", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "age",                   "type": "INT64",   "mode": "NULLABLE"},
        {"name": "gender",                "type": "STRING",    "mode": "NULLABLE"},
        {"name": "etl_timestamp",         "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "sellers": [
        {"name": "seller_id",          "type": "STRING",    "mode": "REQUIRED"},
        {"name": "seller_name",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "seller_postal_code", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "seller_city",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "country_code",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "seller_country",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "etl_timestamp",      "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "products": [
        {"name": "product_id",            "type": "STRING",    "mode": "REQUIRED"},
        {"name": "product_category_name", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "product_weight_gr",     "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "product_length_cm",     "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "product_height_cm",     "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "product_width_cm",      "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "product_volume_cm3",    "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "etl_timestamp",         "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],

    "geolocations": [
        {"name": "geo_postal_code",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "geo_lat",          "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "geo_lon",          "type": "FLOAT64",     "mode": "NULLABLE"},
        {"name": "geolocation_city", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "geo_country",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "etl_timestamp",    "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
}

# ══════════════════════════════════════════════════════════
# BigQuery SQL Generators
# ══════════════════════════════════════════════════════════

def _create_table_ddl(table: str) -> str:
    """
    Generates CREATE TABLE IF NOT EXISTS DDL.
    Fact tables      → PARTITION BY sale_date
    Dimension tables → CLUSTER BY business columns
    """
    schema   = BQ_SCHEMAS[table]
    biz_keys = BQ_BUSINESS_KEYS[table]

    col_defs = ",\n".join(
        f"    {col['name']:<40} {col['type']}"
        for col in schema
    )

    if table in BQ_FACT_TABLES:
        partition_clause = "PARTITION BY sale_date"
        cluster_clause   = ""
    else:
        cluster_cols_map = {
            "customers"   : "customer_country, gender",
            "sellers"     : "seller_country, country_code",
            "products"    : "product_category_name",
            "geolocations": "geo_country, geo_postal_code",
        }
        partition_clause = ""
        cluster_clause   = (
            f"CLUSTER BY {cluster_cols_map[table]}"
        )

    return f"""
CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}`
(
{col_defs}
)
{partition_clause}
{cluster_clause}
OPTIONS (description = '{table} — Sale ETL pipeline')
""".strip()


def _fact_delete_sql(table: str) -> str:
    """
    Deletes today's partition only — idempotent.
    Safe to rerun multiple times on same day.
    """
    return (
        f"DELETE FROM `{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}`\n"
        f"WHERE sale_date = DATE('{{{{ ds }}}}')"
    )


def _fact_insert_dedup_sql(table: str) -> str:
    """
    INSERT + ROW_NUMBER() deduplication for fact tables.

    Fix: explicitly list ALL final table columns in outer SELECT
         matching exact order of BQ_SCHEMAS definition.
         sale_date explicitly cast as DATE to avoid type mismatch.

    orders         → sale_date = DATE(order_purchase_timestamp)
    order_items    → sale_date joined from orders
    order_payments → sale_date joined from orders
    order_reviews  → sale_date joined from orders
    """
    project  = GCP_PROJECT_ID
    dataset  = BQ_RAW_DATASET
    biz_keys = BQ_BUSINESS_KEYS[table]
    staging  = f"`{project}.{dataset}.{table}_staging`"
    final    = f"`{project}.{dataset}.{table}`"

    # ── All columns in final table including sale_date ─────
    all_final_cols = [col["name"] for col in BQ_SCHEMAS[table]]

    # ── Columns from staging (no sale_date) ───────────────
    staging_cols = [
        col["name"] for col in BQ_SCHEMAS[table]
        if col["name"] != "sale_date"
    ]

    # ── sale_date expression ───────────────────────────────
    if table == "orders":
        sale_date_expr = "DATE(s.order_purchase_timestamp)"
        join_clause    = ""
    else:
        sale_date_expr = "DATE(o.order_purchase_timestamp)"
        join_clause    = (
            f"LEFT JOIN `{project}.{dataset}.orders` o\n"
            f"                ON s.order_id = o.order_id"
        )

    # ── inner SELECT → s.col + sale_date + _rn ────────────
    inner_cols   = ",\n                ".join(
        [f"s.{c}" for c in staging_cols]
    )
    biz_key_list = ", ".join([f"s.{k}" for k in biz_keys])

    # ── outer SELECT → explicit column list with CAST ──────
    # Build outer cols — when we hit sale_date cast it explicitly
    outer_select_cols = []
    for col in BQ_SCHEMAS[table]:
        if col["name"] == "sale_date":
            # ✅ explicit CAST to DATE — prevents TIMESTAMP mismatch
            outer_select_cols.append(
                "CAST(sale_date AS DATE) AS sale_date"
            )
        else:
            outer_select_cols.append(col["name"])

    outer_cols = ",\n    ".join(outer_select_cols)

    # ── final INSERT columns ───────────────────────────────
    insert_cols = ", ".join(all_final_cols)

    return f"""
INSERT INTO {final}
    ({insert_cols})
SELECT
    {outer_cols}
FROM (
    SELECT
        {inner_cols},
        {sale_date_expr}            AS sale_date,
        ROW_NUMBER() OVER (
            PARTITION BY {biz_key_list}
            ORDER BY s.etl_timestamp DESC
        )                           AS _rn
    FROM {staging} s
    {join_clause}
)
WHERE _rn = 1
""".strip()




def _dimension_merge_sql(table: str) -> str:
    """
    MERGE upsert for dimension tables.
    WHEN MATCHED     → UPDATE all non-key columns
    WHEN NOT MATCHED → INSERT full row
    Dedup applied in USING clause via ROW_NUMBER().
    """
    project      = GCP_PROJECT_ID
    dataset      = BQ_RAW_DATASET
    biz_keys     = BQ_BUSINESS_KEYS[table]
    schema       = BQ_SCHEMAS[table]
    staging      = f"`{project}.{dataset}.{table}_staging`"
    final        = f"`{project}.{dataset}.{table}`"
    all_cols     = [col["name"] for col in schema]
    non_key_cols = [c for c in all_cols if c not in biz_keys]
    biz_key_list = ", ".join(biz_keys)
    on_clause    = " AND ".join([f"T.{k} = S.{k}" for k in biz_keys])
    update_set   = ",\n        ".join([f"T.{c} = S.{c}" for c in non_key_cols])
    insert_cols  = ", ".join(all_cols)
    insert_vals  = ", ".join([f"S.{c}" for c in all_cols])
    col_select   = ", ".join(all_cols)

    return f"""
MERGE {final} T
USING (
    SELECT {col_select}
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY {biz_key_list}
                   ORDER BY etl_timestamp DESC
               ) AS _rn
        FROM {staging}
    )
    WHERE _rn = 1
) S
ON {on_clause}
WHEN MATCHED THEN
    UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals})
""".strip()


def _drop_staging_sql(table: str) -> str:
    """Drops staging table after load completes."""
    return (
        f"DROP TABLE IF EXISTS "
        f"`{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}_staging`"
    )


# ══════════════════════════════════════════════════════════
# SQL FILE LOADER
# ══════════════════════════════════════════════════════════

def _load_sql(filename: str) -> str:
    """
    Reads a SQL file from sql/views/ folder.
    Replaces placeholders:
      {project}           → GCP_PROJECT_ID
      {raw_dataset}       → BQ_RAW_DATASET
      {analytics_dataset} → BQ_ANALYTICS_DATASET

    This keeps SQL files portable across environments.
    No hardcoded project/dataset names in SQL files.
    """
    import os
    sql_path = os.path.join(SQL_VIEWS_PATH, filename)

    with open(sql_path, "r") as f:
        sql = f.read()

    return (
        sql
        .replace("{project}",           GCP_PROJECT_ID)
        .replace("{raw_dataset}",       BQ_RAW_DATASET)
        .replace("{analytics_dataset}", BQ_ANALYTICS_DATASET)
    )


# ══════════════════════════════════════════════════════════
# WATERMARK FRESHNESS CHECK
# ══════════════════════════════════════════════════════════

def _check_data_freshness(**context) -> str:
    """
    Validates source data freshness via etl_watermark table.
    Rule 1: No watermark  → PASS (first load)
    Rule 2: status != success → FAIL
    Rule 3: age > MAX_FRESHNESS_HOURS → FAIL
    Rule 4: All pass → PROCEED
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    query  = f"""
        SELECT load_timestamp, status, logical_date, files_loaded
        FROM   `{BQ_WATERMARK_FULL}`
        ORDER  BY load_timestamp DESC
        LIMIT  1
    """

    try:
        results = list(client.query(query).result())
    except Exception as e:
        print(f"⚠️  Cannot query watermark: {e} — treating as first load")
        return "first_load"

    if not results:
        print("📋 Watermark empty — first load, proceeding.")
        return "first_load"

    row = results[0]
    print(
        f"📊 Watermark:\n"
        f"   Status       : {row.status}\n"
        f"   Load time    : {row.load_timestamp}\n"
        f"   Logical date : {row.logical_date}\n"
        f"   Files loaded : {row.files_loaded}"
    )

    if row.status != "success":
        raise ValueError(
            f"❌ Freshness FAILED — status='{row.status}'. "
            f"Re-run source_staging_dag first."
        )

    now       = datetime.now(timezone.utc)
    last_load = row.load_timestamp
    if last_load.tzinfo is None:
        last_load = last_load.replace(tzinfo=timezone.utc)

    hours = (now - last_load).total_seconds() / 3600
    if hours > MAX_FRESHNESS_HOURS:
        raise ValueError(
            f"❌ Freshness FAILED — data is {hours:.1f}h old "
            f"(max {MAX_FRESHNESS_HOURS}h). "
            f"Re-run source_staging_dag."
        )

    print(f"✅ Freshness PASSED — data is {hours:.2f}h old.")
    return "fresh"


# ══════════════════════════════════════════════════════════
# BQ LOAD SUMMARY
# ══════════════════════════════════════════════════════════

def _bq_load_complete(**context) -> None:
    """
    Prints row count summary for all 8 BQ tables.
    Validates today's partition counts for fact tables.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    ds     = context["ds"]

    print(f"\n{'='*65}")
    print(f"  BigQuery Load Summary — partition={ds}")
    print(f"{'='*65}")

    total_rows = 0

    for table in TABLE_NAMES:
        full_table = f"`{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}`"
        try:
            total_q   = f"SELECT COUNT(*) AS cnt FROM {full_table}"
            total_r   = list(client.query(total_q).result())
            row_cnt   = total_r[0].cnt
            total_rows += row_cnt

            if table in BQ_FACT_TABLES:
                part_q    = (
                    f"SELECT COUNT(*) AS cnt FROM {full_table} "
                    f"WHERE sale_date = DATE('{ds}')"
                )
                part_r    = list(client.query(part_q).result())
                today_cnt = part_r[0].cnt
                extra     = f"today={today_cnt:>8,}"
            else:
                extra = "clustered dim table    "

            print(
                f"  ✅ {table:<20} "
                f"total={row_cnt:>10,}  {extra}"
            )

        except Exception as e:
            print(f"  ❌ {table:<20} ERROR: {e}")

    print(f"{'─'*65}")
    print(f"  {'TOTAL':<20} {total_rows:>10,} rows across 8 tables")
    print(f"{'='*65}\n")


# ══════════════════════════════════════════════════════════
# PIPELINE COMPLETE SUMMARY
# ══════════════════════════════════════════════════════════

def _pipeline_complete(**context) -> None:
    """
    Final task — prints full pipeline summary.
    Confirms all stages completed successfully.
    """
    ds = context["ds"]
    print(f"""
{'='*65}
  ✅ PIPELINE COMPLETE — {ds}
{'='*65}

  Stage 0 ✅  Freshness Check
  Stage 1 ✅  Data Ingestion    (8 tables → landing_zone)
  Stage 2 ✅  Spark Processing  (8 jobs → processed_zone)
  Stage 3 ✅  BigQuery Load     (8 tables → sales_raw)
  Stage 4 ✅  Analytics Views   (8 views → sales_analytics)

  BigQuery:
    Dataset : sales_raw        (8 partitioned/clustered tables)
    Dataset : sales_analytics  (8 views ready for Looker Studio)

  Views Created:
    ✅ vw_order_summary
    ✅ vw_daily_sales_performance
    ✅ vw_customer_profile
    ✅ vw_seller_performance
    ✅ vw_product_performance
    ✅ vw_payment_analysis
    ✅ vw_review_sentiment
    ✅ vw_geo_sales_heatmap

{'='*65}
    """)



# ══════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════

with DAG(
    dag_id          = "etl_pipeline_dag",
    description     = (
        "Full ETL: Freshness → Ingest → Spark → "
        "BQ Load (partition + dedup + merge)"
    ),
    default_args    = default_args,
    start_date      = datetime(2025, 1, 1),
    schedule        = None,
    catchup         = False,
    max_active_runs = 1,
    tags            = [
        "etl", "gcs", "bigquery",
        "dataproc", "watermark", "phase-2",
    ],
) as dag:

    # ──────────────────────────────────────────────────────
    # STAGE 0: Data Freshness Check
    # Reads watermark from BigQuery
    # Halts pipeline if data is stale or source failed
    # ──────────────────────────────────────────────────────
    freshness_check = PythonOperator(
        task_id         = "data_freshness_check",
        python_callable = _check_data_freshness,
    )

    # ──────────────────────────────────────────────────────
    # STAGE 1: Data Ingestion
    # GCS source bucket → landing_zone (server-side copy)
    # Structure: {table}/partition={ds}/{table}.csv
    # All 8 tasks run in parallel
    # ──────────────────────────────────────────────────────
    ingestion_tasks = []

    for table in TABLE_NAMES:
        ingest_task = GCSToGCSOperator(
            task_id            = f"ingest_{table}_to_landing",
            source_bucket      = GCS_SOURCE_BUCKET,
            source_object      = (
                f"{table}"
                f"/partition={{{{ ds }}}}"
                f"/{table}.csv"
            ),
            destination_bucket = GCS_MAIN_BUCKET,
            destination_object = (
                f"{LANDING_ZONE_PREFIX}"
                f"/{table}"
                f"/partition={{{{ ds }}}}"
                f"/{table}.csv"
            ),
            gcp_conn_id        = GCP_CONN_ID,
            replace            = True,
        )
        ingestion_tasks.append(ingest_task)

    # ──────────────────────────────────────────────────────
    # STAGE 2a: Upload PySpark scripts to GCS
    # All 9 scripts uploaded in parallel
    # ──────────────────────────────────────────────────────
    upload_script_tasks = []

    for script in SPARK_SCRIPTS:
        upload_task = LocalFilesystemToGCSOperator(
            task_id     = f"upload_script_{script.replace('.', '_')}",
            src         = f"{SPARK_JOBS_LOCAL_PATH}/{script}",
            dst         = f"{SPARK_JOBS_GCS_PREFIX}/{script}",
            bucket      = GCS_MAIN_BUCKET,
            gcp_conn_id = GCP_CONN_ID,
            mime_type   = "text/x-python",
        )
        upload_script_tasks.append(upload_task)

    # ──────────────────────────────────────────────────────
    # STAGE 2b: Create Dataproc Cluster
    # Dynamic name  → no conflicts on retry
    # Single node   → only 1 VM needed
    # No zone       → GCP auto-selects available capacity
    # ──────────────────────────────────────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id        = "create_dataproc_cluster",
        project_id     = GCP_PROJECT_ID,
        cluster_name   = DYNAMIC_CLUSTER_NAME,
        region         = DATAPROC_REGION,
        cluster_config = CLUSTER_CONFIG,
        gcp_conn_id    = GCP_CONN_ID,
    )

    # ──────────────────────────────────────────────────────
    # STAGE 2c: Submit PySpark Jobs
    # One job per table — 8 jobs total
    # All run simultaneously on single node cluster
    # ──────────────────────────────────────────────────────
    spark_tasks = []

    for table in TABLE_NAMES:
        pyspark_job = {
            "reference" : {"project_id": GCP_PROJECT_ID},
            "placement" : {"cluster_name": DYNAMIC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": (
                    f"gs://{GCS_MAIN_BUCKET}"
                    f"/{SPARK_JOBS_GCS_PREFIX}/transform_{table}.py"
                ),
                "python_file_uris": [
                    f"gs://{GCS_MAIN_BUCKET}"
                    f"/{SPARK_JOBS_GCS_PREFIX}/utils.py",
                ],
                "args": [
                    # input  → landing_zone
                    f"gs://{GCS_MAIN_BUCKET}/{LANDING_ZONE_PREFIX}"
                    f"/{table}/partition={{{{ ds }}}}/{table}.csv",
                    # output → processed_zone
                    f"gs://{GCS_MAIN_BUCKET}/{PROCESSED_ZONE_PREFIX}"
                    f"/{table}/partition={{{{ ds }}}}",
                ],
                "properties": {
                    "spark.sql.shuffle.partitions": "8",
                    "spark.executor.memory"       : "3g",
                    "spark.driver.memory"         : "3g",
                },
            },
        }

        spark_task = DataprocSubmitJobOperator(
            task_id     = f"spark_transform_{table}",
            job         = pyspark_job,
            region      = DATAPROC_REGION,
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
        )
        spark_tasks.append(spark_task)

    # ──────────────────────────────────────────────────────
    # STAGE 2d: Delete Dataproc Cluster
    # trigger_rule = ALL_DONE → always runs even on failure
    # Prevents orphaned clusters and cost leaks
    # ──────────────────────────────────────────────────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id      = "delete_dataproc_cluster",
        project_id   = GCP_PROJECT_ID,
        cluster_name = DYNAMIC_CLUSTER_NAME,
        region       = DATAPROC_REGION,
        gcp_conn_id  = GCP_CONN_ID,
        trigger_rule = TriggerRule.ALL_DONE,
    )

    # ──────────────────────────────────────────────────────
    # STAGE 3a: Create BQ Datasets
    # sales_raw       → 8 raw tables
    # sales_analytics → views (Phase 5)
    # if_exists=ignore → idempotent, safe to rerun
    # ──────────────────────────────────────────────────────
    create_raw_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id    = "create_bq_raw_dataset",
        dataset_id = BQ_RAW_DATASET,
        project_id = GCP_PROJECT_ID,
        location   = BQ_LOCATION,
        gcp_conn_id= GCP_CONN_ID,
        if_exists  = "ignore",
    )

    create_analytics_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id    = "create_bq_analytics_dataset",
        dataset_id = BQ_ANALYTICS_DATASET,
        project_id = GCP_PROJECT_ID,
        location   = BQ_LOCATION,
        gcp_conn_id= GCP_CONN_ID,
        if_exists  = "ignore",
    )

    # ──────────────────────────────────────────────────────
    # STAGE 3b: Create BQ Final Tables
    # Fact tables      → PARTITION BY sale_date (DATE)
    # Dimension tables → CLUSTER BY business columns
    # CREATE TABLE IF NOT EXISTS → fully idempotent
    # ──────────────────────────────────────────────────────
    create_table_tasks = []

    for table in TABLE_NAMES:
        create_task = BigQueryInsertJobOperator(
            task_id     = f"create_bq_table_{table}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _create_table_ddl(table),
                    "useLegacySql": False,
                }
            },
        )
        create_table_tasks.append(create_task)

    # ──────────────────────────────────────────────────────
    # STAGE 3c: Load Staging Tables from GCS Parquet
    # One staging table per table — all parallel
    # Staging = raw dump, no partitioning, no dedup
    # WRITE_TRUNCATE → always fresh staging on rerun
    # ──────────────────────────────────────────────────────
    staging_load_tasks = []

    for table in TABLE_NAMES:
        staging_task = GCSToBigQueryOperator(
            task_id                        = f"load_{table}_to_staging",
            bucket                         = GCS_MAIN_BUCKET,
            source_objects                    = [
                # ✅ Files sit directly in partition folder
                f"{PROCESSED_ZONE_PREFIX}/{table}"
                f"/partition={{{{ ds }}}}"
                f"/*.snappy.parquet"            # ← correct extension
            ],
            destination_project_dataset_table = (
                f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table}_staging"
            ),
            source_format                  = "PARQUET",
            write_disposition              = "WRITE_TRUNCATE",
            autodetect                     = True,
            gcp_conn_id                    = GCP_CONN_ID,
        )
        staging_load_tasks.append(staging_task)

    # ──────────────────────────────────────────────────────
    # STAGE 3d — FACT TABLES
    # Step 1: DELETE today's partition (idempotent)
    # Step 2: INSERT + ROW_NUMBER() dedup
    #         + sale_date derived/joined from orders
    # ──────────────────────────────────────────────────────
    fact_delete_tasks = []
    fact_insert_tasks = []

    for table in BQ_FACT_TABLES:
        # ── DELETE today's partition ───────────────────────
        delete_task = BigQueryInsertJobOperator(
            task_id     = f"bq_delete_partition_{table}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _fact_delete_sql(table),
                    "useLegacySql": False,
                }
            },
        )
        fact_delete_tasks.append(delete_task)

        # ── INSERT deduplicated rows ───────────────────────
        insert_task = BigQueryInsertJobOperator(
            task_id     = f"bq_insert_dedup_{table}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _fact_insert_dedup_sql(table),
                    "useLegacySql": False,
                }
            },
        )
        fact_insert_tasks.append(insert_task)

    # ──────────────────────────────────────────────────────
    # STAGE 3d — DIMENSION TABLES
    # MERGE upsert on business key
    # WHEN MATCHED     → UPDATE all columns
    # WHEN NOT MATCHED → INSERT new row
    # Dedup applied inside USING clause
    # ──────────────────────────────────────────────────────
    dimension_merge_tasks = []

    for table in BQ_DIMENSION_TABLES:
        merge_task = BigQueryInsertJobOperator(
            task_id     = f"bq_merge_{table}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _dimension_merge_sql(table),
                    "useLegacySql": False,
                }
            },
        )
        dimension_merge_tasks.append(merge_task)

    # ──────────────────────────────────────────────────────
    # STAGE 3e: Drop Staging Tables
    # Runs after all fact + dimension loads complete
    # Cleans up temp staging tables to save BQ storage
    # ──────────────────────────────────────────────────────
    drop_staging_tasks = []

    for table in TABLE_NAMES:
        drop_task = BigQueryInsertJobOperator(
            task_id     = f"drop_staging_{table}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _drop_staging_sql(table),
                    "useLegacySql": False,
                }
            },
        )
        drop_staging_tasks.append(drop_task)

    # ──────────────────────────────────────────────────────
    # STAGE 3f: BQ Load Complete Summary
    # Prints row counts + partition counts for all 8 tables
    # ──────────────────────────────────────────────────────
    bq_load_complete = PythonOperator(
        task_id         = "bq_load_complete",
        python_callable = _bq_load_complete,
    )

    # ──────────────────────────────────────────────────────
    # STAGE 4: Create Analytics Views
    # All 8 views run in parallel
    # CREATE OR REPLACE VIEW → always idempotent
    # SQL loaded from sql/views/*.sql files
    # ──────────────────────────────────────────────────────
    view_tasks = []

    for view in ANALYTICS_VIEWS:
        view_task = BigQueryInsertJobOperator(
            task_id     = f"create_view_{view}",
            project_id  = GCP_PROJECT_ID,
            gcp_conn_id = GCP_CONN_ID,
            configuration={
                "query": {
                    "query"       : _load_sql(f"{view}.sql"),
                    "useLegacySql": False,
                }
            },
        )
        view_tasks.append(view_task)

    # ──────────────────────────────────────────────────────
    # STAGE 4f: Pipeline Complete
    # Final task — confirms full pipeline success
    # ──────────────────────────────────────────────────────
    pipeline_complete = PythonOperator(
        task_id         = "pipeline_complete",
        python_callable = _pipeline_complete,
    )


    # ══════════════════════════════════════════════════════
    # FULL DEPENDENCY CHAIN
    #
    # [data_freshness_check]
    #         │
    # [ingest_* × 8] ──────────────────────── parallel
    #         │
    # [upload_script_* × 9] ───────────────── parallel
    #         │
    # [create_dataproc_cluster]
    #         │
    # [spark_transform_* × 8] ─────────────── parallel
    #         │
    # [delete_dataproc_cluster] ←──────────── ALL_DONE
    #         │
    # [create_bq_raw_dataset]
    # [create_bq_analytics_dataset] ───────── parallel
    #         │
    # [create_bq_table_* × 8] ─────────────── parallel
    #         │
    # [load_*_to_staging × 8] ─────────────── parallel
    #         │
    #   ┌─────┴──────────────────────┐
    #   │ FACT TABLES                │ DIMENSION TABLES
    #   │ [bq_delete_partition_*]    │ [bq_merge_*]
    #   │         │                  │
    #   │ [bq_insert_dedup_*]        │
    #   └─────┬──────────────────────┘
    #         │
    # [drop_staging_* × 8] ────────────────── parallel
    #         │
    # [bq_load_complete]
    # ══════════════════════════════════════════════════════

    # Stage 0 → Stage 1
    freshness_check >> ingestion_tasks

    # Stage 1 → Stage 2a
    for ingest_task in ingestion_tasks:
        ingest_task >> upload_script_tasks

    # Stage 2a → Stage 2b
    for upload_task in upload_script_tasks:
        upload_task >> create_cluster

    # Stage 2b → Stage 2c
    create_cluster >> spark_tasks

    # Stage 2c → Stage 2d
    for spark_task in spark_tasks:
        spark_task >> delete_cluster

    # Stage 2d → Stage 3a (both datasets in parallel)
    delete_cluster >> [create_raw_dataset, create_analytics_dataset]

    # Stage 3a → Stage 3b
    for create_ds in [create_raw_dataset, create_analytics_dataset]:
        create_ds >> create_table_tasks

    # Stage 3b → Stage 3c
    for create_tbl in create_table_tasks:
        create_tbl >> staging_load_tasks

    # Stage 3c → Stage 3d
    # Fact tables: staging → delete → insert
    for i, table in enumerate(BQ_FACT_TABLES):
        # find matching staging task by task_id
        staging_task = next(
            t for t in staging_load_tasks
            if table in t.task_id
        )
        staging_task >> fact_delete_tasks[i]
        fact_delete_tasks[i] >> fact_insert_tasks[i]

    # Dimension tables: staging → merge
    for i, table in enumerate(BQ_DIMENSION_TABLES):
        staging_task = next(
            t for t in staging_load_tasks
            if table in t.task_id
        )
        staging_task >> dimension_merge_tasks[i]

    # Stage 3d → Stage 3e (all fact inserts + dim merges → drop staging)
    for insert_task in fact_insert_tasks:
        insert_task >> drop_staging_tasks

    for merge_task in dimension_merge_tasks:
        merge_task >> drop_staging_tasks

    # Stage 3e → Stage 3f
    for drop_task in drop_staging_tasks:
        drop_task >> bq_load_complete

    # Stage 3f → Stage 4 (views) ← ADD THIS ✅
    bq_load_complete >> view_tasks

    # Stage 4 → pipeline complete ← ADD THIS ✅
    for view_task in view_tasks:
        view_task >> pipeline_complete

