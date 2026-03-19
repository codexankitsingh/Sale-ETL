# spark_jobs/transform_sellers.py
"""
Transforms: sellers table
Actual columns (after clean):
  seller_id, seller_name,
  seller_postal_code, seller_city,
  country_code, seller_country
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_sellers")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[sellers] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Preserve postal code as STRING (leading zeros) ─────
    df = df.withColumn(
        "seller_postal_code",
        F.lpad(F.col("seller_postal_code").cast("string"), 5, "0")
    )

    # ── Standardize city to title case ────────────────────
    df = df.withColumn(
        "seller_city",
        F.initcap(F.lower(F.trim(F.col("seller_city"))))
    )

    # ── Standardize country to uppercase ──────────────────
    df = df.withColumn(
        "seller_country",
        F.upper(F.trim(F.col("seller_country")))
    )

    # ── Standardize country_code to uppercase ─────────────
    df = df.withColumn(
        "country_code",
        F.upper(F.trim(F.col("country_code")))
    )

    print(f"[sellers] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[sellers] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
