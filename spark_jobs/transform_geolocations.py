# spark_jobs/transform_geolocations.py
"""
Transforms: geolocations table
Actual columns (after clean):
  geo_postal_code, geo_lat, geo_lon,
  geolocation_city, geo_country
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import apply_common_transformations, read_csv_auto


def main(input_path: str, output_path: str):

    spark = (
        SparkSession.builder
        .appName("transform_geolocations")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[geolocations] Reading from: {input_path}")
    df = read_csv_auto(spark, input_path)
    df = apply_common_transformations(df)

    # ── Cast lat/lon to double ─────────────────────────────
    df = df.withColumn("geo_lat", F.col("geo_lat").cast("double"))
    df = df.withColumn("geo_lon", F.col("geo_lon").cast("double"))

    # ── Standardize city to title case ────────────────────
    df = df.withColumn(
        "geolocation_city",
        F.initcap(F.lower(F.trim(F.col("geolocation_city"))))
    )

    # ── Standardize country to uppercase ──────────────────
    df = df.withColumn(
        "geo_country",
        F.upper(F.trim(F.col("geo_country")))
    )

    # ── Deduplicate on postal code ─────────────────────────
    df = df.dropDuplicates(["geo_postal_code"])

    print(f"[geolocations] Writing to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[geolocations] ✅ Done. Rows: {df.count()}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
