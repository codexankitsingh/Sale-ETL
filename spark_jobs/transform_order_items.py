# spark_jobs/transform_order_items.py
"""
Transforms: order_items table
Actual columns (after clean):
  order_id, order_item_id, product_id,
  seller_id, shipping_limit_date,
  price, freight_value
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_order_items")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[order_items] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast numeric columns ───────────────────────────────
    df = df.withColumn("price",         F.col("price").cast("double"))
    df = df.withColumn("freight_value", F.col("freight_value").cast("double"))
    df = df.withColumn("order_item_id", F.col("order_item_id").cast("integer"))

    # ── Cast timestamp ─────────────────────────────────────
    df = df.withColumn(
        "shipping_limit_date",
        F.to_timestamp(F.col("shipping_limit_date"))
    )

    # ── Add total item value ───────────────────────────────
    df = df.withColumn(
        "total_item_value",
        F.round(F.col("price") + F.col("freight_value"), 2)
    )

    print(f"[order_items] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[order_items] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
