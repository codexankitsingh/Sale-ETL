# spark_jobs/transform_order_reviews.py
"""
Transforms: order_reviews table
Actual columns (after clean):
  review_id, order_id, review_score,
  review_comment_title_en,
  review_comment_message_en,
  review_creation_date,
  review_answer_timestamp
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_order_reviews")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[order_reviews] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast review_score to integer ───────────────────────
    df = df.withColumn(
        "review_score",
        F.col("review_score").cast("integer")
    )

    # ── Cast timestamp columns ─────────────────────────────
    df = df.withColumn(
        "review_creation_date",
        F.to_timestamp(F.col("review_creation_date"))
    )
    df = df.withColumn(
        "review_answer_timestamp",
        F.to_timestamp(F.col("review_answer_timestamp"))
    )

    # ── Add sentiment bucket based on review_score ─────────
    df = df.withColumn(
        "review_sentiment",
        F.when(F.col("review_score") >= 4, "positive")
         .when(F.col("review_score") == 3, "neutral")
         .otherwise("negative")
    )

    print(f"[order_reviews] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[order_reviews] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
