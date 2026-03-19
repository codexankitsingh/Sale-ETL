# spark_jobs/transform_order_payments.py
"""
Transforms: order_payments table
Actual columns (after clean):
  order_id, payment_sequential,
  payment_type, payment_installments,
  payment_value
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_order_payments")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[order_payments] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast numeric columns ───────────────────────────────
    df = df.withColumn(
        "payment_sequential",
        F.col("payment_sequential").cast("integer")
    )
    df = df.withColumn(
        "payment_installments",
        F.col("payment_installments").cast("integer")
    )
    df = df.withColumn(
        "payment_value",
        F.col("payment_value").cast("double")
    )

    # ── Standardize payment_type to lowercase ──────────────
    df = df.withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )

    print(f"[order_payments] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[order_payments] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
