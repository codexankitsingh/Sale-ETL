# spark_jobs/utils.py
"""
Shared PySpark utility functions for the Sale-ETL pipeline.

Handles:
  - Auto-detects CSV delimiter (comma, semicolon, tab, pipe)
  - Strips BOM character \ufeff from first column header
  - Strips carriage return \r from last column header
  - Applies common transformations to all tables
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def detect_delimiter(spark: SparkSession, path: str) -> str:
    """
    Auto-detects the delimiter of a CSV file by reading
    the first line and counting delimiter occurrences.

    Supports: comma (,), semicolon (;), tab (\t), pipe (|)
    Returns the most likely delimiter character.
    """
    raw_line = (
        spark.sparkContext
        .textFile(path)
        .first()
    )

    candidates = {
        ","  : raw_line.count(","),
        ";"  : raw_line.count(";"),
        "\t" : raw_line.count("\t"),
        "|"  : raw_line.count("|"),
    }

    detected = max(candidates, key=candidates.get)

    print(
        f"[utils] Delimiter detection for: {path}\n"
        f"        Counts → comma={candidates[',']}, "
        f"semicolon={candidates[';']}, "
        f"tab={candidates[chr(9)]}, "
        f"pipe={candidates['|']}\n"
        f"        Detected delimiter: {repr(detected)}"
    )

    return detected


def read_csv_auto(
    spark       : SparkSession,
    path        : str,
    infer_schema: bool = False,
) -> DataFrame:
    """
    Reads a CSV file with:
      - Auto-detected delimiter
      - BOM character stripped from first column
      - Carriage return stripped from last column
      - All column names lowercased + spaces→underscores
      - header=True, inferSchema=False by default
    """
    delimiter = detect_delimiter(spark, path)

    df = (
        spark.read
        .option("header",      "true")
        .option("inferSchema", str(infer_schema).lower())
        .option("sep",         delimiter)
        .option("encoding",    "UTF-8")
        # ── Handles \r\n line endings (Windows CSV) ────────
        .option("lineSep",     "\n")
        # ── Strips BOM automatically ───────────────────────
        .option("charset",     "UTF-8-BOM")
        .csv(path)
    )

    # ── Clean column names ─────────────────────────────────
    # Strips: BOM \ufeff, carriage return \r,
    #         leading/trailing spaces
    # Converts to: lowercase with underscores
    clean_cols = []
    for col in df.columns:
        clean = (
            col
            .replace("\ufeff", "")   # strip BOM
            .replace("\r",     "")   # strip carriage return
            .replace("\n",     "")   # strip newline
            .strip()                 # strip spaces
            .lower()                 # lowercase
            .replace(" ", "_")       # spaces to underscores
            .replace("-", "_")       # hyphens to underscores
        )
        clean_cols.append(clean)

    # Rename all columns at once
    df = df.toDF(*clean_cols)

    print(
        f"[utils] Read CSV  : {path}\n"
        f"        Delimiter : {repr(delimiter)}\n"
        f"        Columns   : {df.columns}\n"
        f"        Row count : {df.count()}"
    )

    return df


def apply_common_transformations(df: DataFrame) -> DataFrame:
    """
    Applies standard transformations to all tables:
      1. Strip whitespace from all string columns
      2. Replace null-like values with 'NA'
      3. Add etl_timestamp column
    """
    # ── Step 1: Trim whitespace from all string columns ───
    string_cols = [
        col for col, dtype in df.dtypes
        if dtype == "string"
    ]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    # ── Step 2: Replace null-like strings with 'NA' ───────
    null_like = ["", "null", "none", "na", "n/a", "nan"]
    for col in string_cols:
        df = df.withColumn(
            col,
            F.when(
                F.lower(F.col(col)).isin(null_like),
                F.lit("NA")
            ).otherwise(F.col(col))
        )

    # ── Step 3: Add ETL timestamp ─────────────────────────
    df = df.withColumn(
        "etl_timestamp",
        F.current_timestamp().cast("timestamp")
    )

    return df
