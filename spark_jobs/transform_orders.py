# spark_jobs/transform_orders.py
"""
Transforms: orders table
Actual columns (after clean):
  order_id, customer_trx_id, order_status,
  order_purchase_timestamp, order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto   # ✅ no safe_to_timestamp


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_orders")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[orders] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast timestamp columns ─────────────────────────────
    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in ts_cols:
        df = df.withColumn(
            col,
            F.to_timestamp(F.col(col))   # ✅ standard PySpark — no safe_to_timestamp
        )

    # ── Standardize order_status to lowercase ──────────────
    df = df.withColumn(
        "order_status",
        F.lower(F.trim(F.col("order_status")))
    )

    print(f"[orders] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[orders] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
