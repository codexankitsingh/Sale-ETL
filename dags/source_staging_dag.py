# dags/source_staging_dag.py
"""
DAG 1: source_staging_dag
─────────────────────────
Uploads 8 raw CSV files from local /data/ folder
to GCS source bucket with date-based partitioning.

Watermark Pattern:
  - Creates etl_watermark table in BigQuery if not exists
  - Writes a watermark record after every successful load
  - Stores: load_timestamp, prev_load_timestamp, status, files_loaded
  - etl_pipeline_dag reads this watermark to validate freshness

GCS Structure:
  gs://sale-etl-source-bucket/partition=YYYY-MM-DD/<table>.csv

BigQuery Watermark Table:
  sale-etl.sales_raw.etl_watermark
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow/dags")
from config import (
    GCP_PROJECT_ID,
    GCS_SOURCE_BUCKET,
    GCP_CONN_ID,
    TABLE_NAMES,
    LOCAL_DATA_PATH,
    BQ_WATERMARK_DATASET,
    BQ_WATERMARK_TABLE,
    BQ_WATERMARK_FULL,
)

# ── Default DAG arguments ──────────────────────────────────
default_args = {
    "owner"           : "sale-etl",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry"  : False,
}

# ══════════════════════════════════════════════════════════
# WATERMARK UTILITIES — Embedded in DAG 1
# ══════════════════════════════════════════════════════════

WATERMARK_SCHEMA = [
    {"name": "dag_id",              "type": "STRING",    "mode": "REQUIRED"},
    {"name": "run_id",              "type": "STRING",    "mode": "REQUIRED"},
    {"name": "logical_date",        "type": "DATE",      "mode": "REQUIRED"},
    {"name": "load_timestamp",      "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "prev_load_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "files_loaded",        "type": "INTEGER",   "mode": "NULLABLE"},
    {"name": "status",              "type": "STRING",    "mode": "REQUIRED"},
    {"name": "notes",               "type": "STRING",    "mode": "NULLABLE"},
]


def _ensure_watermark_table(**context) -> None:
    """
    Creates the etl_watermark table in BigQuery if it doesn't exist.
    Safe to call on every DAG run — fully idempotent.

    Table     : sale-etl.sales_raw.etl_watermark
    Partitioned: load_timestamp (DAY)
    """
    from google.cloud import bigquery

    client    = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = BQ_WATERMARK_FULL

    try:
        client.get_table(table_ref)
        print(f"✅ Watermark table already exists: {table_ref}")

    except Exception:
        print(f"📋 Watermark table not found. Creating: {table_ref}")

        schema = [
            bigquery.SchemaField(
                f["name"], f["type"], mode=f["mode"]
            )
            for f in WATERMARK_SCHEMA
        ]

        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_ = bigquery.TimePartitioningType.DAY,
            field = "load_timestamp",
        )

        client.create_table(table)
        print(f"✅ Watermark table created: {table_ref}")


def _write_success_watermark(**context) -> None:
    """
    Writes a success watermark record to BigQuery
    after all 8 CSV uploads complete successfully.

    Automatically fetches the previous load_timestamp
    from the last successful record for full audit trail.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID)
    now    = datetime.now(timezone.utc)

    # ── Step 1: Get previous load timestamp ───────────────
    prev_load_timestamp = None
    try:
        query = f"""
            SELECT load_timestamp
            FROM   `{BQ_WATERMARK_FULL}`
            WHERE  status = 'success'
            ORDER  BY load_timestamp DESC
            LIMIT  1
        """
        results = list(client.query(query).result())
        if results:
            prev_ts = results[0].load_timestamp
            if prev_ts.tzinfo is None:
                prev_ts = prev_ts.replace(tzinfo=timezone.utc)
            prev_load_timestamp = prev_ts.isoformat()
            print(f"📋 Previous load timestamp: {prev_load_timestamp}")
        else:
            print("📋 No previous watermark — this is the first load")

    except Exception as e:
        print(f"⚠️  Could not fetch previous watermark: {e}")

    # ── Step 2: Write new watermark record ────────────────
    row = {
        "dag_id"             : context["dag"].dag_id,
        "run_id"             : context["run_id"],
        "logical_date"       : str(context["ds"]),
        "load_timestamp"     : now.isoformat(),
        "prev_load_timestamp": prev_load_timestamp,
        "files_loaded"       : len(TABLE_NAMES),
        "status"             : "success",
        "notes"              : (
            f"Loaded {len(TABLE_NAMES)} files "
            f"for partition={context['ds']} "
            f"at {now.strftime('%Y-%m-%d %H:%M:%S')} UTC"
        ),
    }

    errors = client.insert_rows_json(BQ_WATERMARK_FULL, [row])

    if errors:
        raise RuntimeError(
            f"❌ Failed to write watermark to BigQuery: {errors}"
        )

    print(
        f"✅ Watermark written successfully!\n"
        f"   Status       : success\n"
        f"   Load time    : {now.isoformat()}\n"
        f"   Prev load    : {prev_load_timestamp or 'N/A (first load)'}\n"
        f"   Files loaded : {len(TABLE_NAMES)}\n"
        f"   Partition    : {context['ds']}"
    )


# ── DAG Definition ─────────────────────────────────────────
with DAG(
    dag_id          = "source_staging_dag",
    description     = "Upload CSVs to GCS → Write watermark → Trigger ETL pipeline",
    default_args    = default_args,
    start_date      = datetime(2025, 1, 1),
    schedule        = "@daily",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["source", "gcs", "ingestion", "watermark", "phase-1"],
) as dag:

    # ──────────────────────────────────────────────────────
    # TASK 0: Ensure watermark table exists in BigQuery
    # Runs first — creates table if missing (idempotent)
    # ──────────────────────────────────────────────────────
    ensure_watermark_table = PythonOperator(
        task_id         = "ensure_watermark_table",
        python_callable = _ensure_watermark_table,
    )

    # ──────────────────────────────────────────────────────
    # TASK 1-8: Upload CSVs to GCS source bucket (parallel)
    # ──────────────────────────────────────────────────────
    upload_tasks = []

    for table in TABLE_NAMES:
        upload_task = LocalFilesystemToGCSOperator(
            task_id     = f"upload_{table}_to_source",
            src         = f"{LOCAL_DATA_PATH}/{table}.csv",
            dst         = (
                f"{table}"                       # ✅ table folder first
                f"/partition={{{{ ds }}}}"
                f"/{table}.csv"
            ),
            bucket      = GCS_SOURCE_BUCKET,
            gcp_conn_id = GCP_CONN_ID,
            mime_type   = "text/csv",
        )
        upload_tasks.append(upload_task)

    # ──────────────────────────────────────────────────────
    # TASK 9: Write success watermark to BigQuery
    # Only runs after ALL 8 uploads succeed
    # ──────────────────────────────────────────────────────
    write_watermark = PythonOperator(
        task_id         = "write_success_watermark",
        python_callable = _write_success_watermark,
    )

    # ──────────────────────────────────────────────────────
    # TASK 10: Trigger ETL pipeline DAG
    # Fires etl_pipeline_dag with same logical_date
    # ──────────────────────────────────────────────────────
    trigger_etl_pipeline = TriggerDagRunOperator(
        task_id             = "trigger_etl_pipeline_dag",
        trigger_dag_id      = "etl_pipeline_dag",
        logical_date        = "{{ logical_date }}",
        wait_for_completion = False,
        reset_dag_run       = True,
        poke_interval       = 60,
    )

    # ──────────────────────────────────────────────────────
    # TASK DEPENDENCY CHAIN
    #
    # [ensure_watermark_table]
    #          │
    # [upload_* × 8] ← parallel
    #          │
    # [write_success_watermark]
    #          │
    # [trigger_etl_pipeline_dag]
    # ──────────────────────────────────────────────────────
    ensure_watermark_table >> upload_tasks >> \
    write_watermark >> trigger_etl_pipeline
