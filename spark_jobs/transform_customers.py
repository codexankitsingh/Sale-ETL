# spark_jobs/transform_customers.py
"""
Transforms: customers table
Actual columns (after clean):
  customer_trx_id, subscriber_id,
  subscribe_date, first_order_date,
  customer_postal_code, customer_city,
  customer_country, customer_country_code,
  age, gender
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_customers")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[customers] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Preserve postal code as STRING (leading zeros) ─────
    df = df.withColumn(
        "customer_postal_code",
        F.lpad(F.col("customer_postal_code").cast("string"), 5, "0")
    )

    # ── Cast date columns ──────────────────────────────────
    df = df.withColumn(
        "subscribe_date",
        F.to_date(F.col("subscribe_date"))
    )
    df = df.withColumn(
        "first_order_date",
        F.to_date(F.col("first_order_date"))
    )

    # ── Cast age to integer ────────────────────────────────
    df = df.withColumn("age", F.col("age").cast("integer"))

    # ── Standardize city to title case ────────────────────
    df = df.withColumn(
        "customer_city",
        F.initcap(F.lower(F.trim(F.col("customer_city"))))
    )

    # ── Standardize country to uppercase ──────────────────
    df = df.withColumn(
        "customer_country",
        F.upper(F.trim(F.col("customer_country")))
    )

    # ── Standardize gender to title case ──────────────────
    df = df.withColumn(
        "gender",
        F.initcap(F.lower(F.trim(F.col("gender"))))
    )

    print(f"[customers] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[customers] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
