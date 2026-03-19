# spark_jobs/transform_products.py
"""
Transforms: products table
Actual columns (after clean):
  product_id, product_category_name,
  product_weight_gr, product_length_cm,
  product_height_cm, product_width_cm
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_products")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[products] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast numeric columns ───────────────────────────────
    numeric_cols = [
        "product_weight_gr",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for col in numeric_cols:
        df = df.withColumn(col, F.col(col).cast("double"))

    # ── Standardize category name to lowercase ─────────────
    df = df.withColumn(
        "product_category_name",
        F.lower(F.trim(F.col("product_category_name")))
    )

    # ── Add volumetric weight ──────────────────────────────
    df = df.withColumn(
        "product_volume_cm3",
        F.round(
            F.col("product_length_cm") *
            F.col("product_height_cm") *
            F.col("product_width_cm"),
            2
        )
    )

    print(f"[products] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[products] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
