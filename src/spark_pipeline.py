import setuptools  # Fix for "No module named 'distutils'" in Python 3.12
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, lag, sum as _sum, when, lit, count, max as _max, min as _min,
    concat_ws, hour, date_format
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import shutil
from delta import *

# ---------------------------------------------------------------------------
# Spark Session initialization with Delta support
# ---------------------------------------------------------------------------
import sys

# 1. Unset SPARK_HOME if it's set to a broken path
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# 2. Force Spark to bind to localhost to avoid "Connection refused" on macOS
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# 3. Configure Spark builder
builder = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .master("local[*]")

# 4. Use configure_spark_with_delta_pip to handle Delta Lake dependencies automatically
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.getcwd()
RAW_DATA_PATH = os.path.join(BASE_DIR, "data/clickstream_data.json")
BRONZE_PATH = os.path.join(BASE_DIR, "data/delta/bronze")
SILVER_PATH = os.path.join(BASE_DIR, "data/delta/silver")
GOLD_PATH = os.path.join(BASE_DIR, "data/delta/gold")
INTERMEDIATE_PATH = os.path.join(BASE_DIR, "data/delta/intermediate")

# ---------------------------------------------------------------------------
# Helper: robust schema validation and error handling for raw JSON
# ---------------------------------------------------------------------------
raw_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("current_page", StringType(), False),
    StructField("url", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("device", StringType(), True)
])

def read_raw_json(path):
    """Read raw JSON with schema validation."""
    df = spark.read.schema(raw_schema).option("mode", "PERMISSIVE").json(path)
    # Separate bad records
    bad_records = df.filter(
        col("user_id").isNull() |
        col("event_time").isNull() |
        col("current_page").isNull()
    )
    if bad_records.count() > 0:
        bad_path = os.path.join(BASE_DIR, "data/delta/bad_records")
        bad_records.write.format("delta").mode("overwrite").save(bad_path)
        print(f"Found {bad_records.count()} malformed records – saved to {bad_path}")
    # Return only good records
    good_df = df.filter(
        col("user_id").isNotNull() &
        col("event_time").isNotNull() &
        col("current_page").isNotNull()
    )
    return good_df

# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------
def process_bronze():
    print("Processing Bronze Layer...")
    df = read_raw_json(RAW_DATA_PATH)
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"Bronze layer written to {BRONZE_PATH}")

    # Export to CSV for user verification
    csv_path = os.path.join(BASE_DIR, "data/output/bronze.csv")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"Exported Bronze data to {csv_path}")

# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------
def process_silver():
    print("Processing Silver Layer (Sessionisation)...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    # Convert timestamp string to proper Timestamp type
    df = df.withColumn("event_timestamp", to_timestamp(col("event_time")))
    # Window for each user ordered by timestamp
    window_spec = Window.partitionBy("user_id").orderBy("event_timestamp")
    df = df.withColumn("prev_time", lag("event_timestamp").over(window_spec))
    df = df.withColumn(
        "time_diff_seconds",
        (col("event_timestamp").cast("long") - col("prev_time").cast("long"))
    )
    # Refined session timeout: 30 minutes OR inactivity > 15 minutes
    df = df.withColumn(
        "is_new_session",
        when(
            (col("time_diff_seconds") > 1800) |
            (col("time_diff_seconds") > 900) |
            col("time_diff_seconds").isNull(),
            1
        ).otherwise(0)
    )
    df = df.withColumn("session_id", _sum("is_new_session").over(window_spec))
    df = df.withColumn(
        "unique_session_id",
        concat_ws("_", col("user_id"), col("session_id"))
    )
    # Persist intermediate session metrics
    df.select(
        "unique_session_id", "user_id", "session_id", "event_timestamp", "time_diff_seconds", "is_new_session"
    ).write.format("delta").mode("overwrite").save(os.path.join(INTERMEDIATE_PATH, "session_metrics"))
    # Write final silver layer
    df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"Silver layer written to {SILVER_PATH}")
    
    # Export to CSV for user verification
    csv_path = os.path.join(BASE_DIR, "data/output/silver.csv")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"Exported Silver data to {csv_path}")

# ---------------------------------------------------------------------------
# Visualization Helper
# ---------------------------------------------------------------------------
def generate_plots(gold_df):
    """Generate Funnel Chart and Session Duration Distribution using Matplotlib/Seaborn."""
    print("Generating visualizations...")
    import matplotlib.pyplot as plt
    import seaborn as sns
    import pandas as pd

    # Convert Spark DataFrame to Pandas (Gold data is aggregated and small)
    # Cast timestamps to string to avoid Pandas 2.x compatibility issues (TypeError: Casting to unit-less dtype 'datetime64' is not supported)
    plot_df = gold_df.withColumn("end_time", col("end_time").cast("string")) \
                     .withColumn("start_time", col("start_time").cast("string"))
    pdf = plot_df.toPandas()

    # 1. Funnel Chart
    # Identify funnel columns that exist in the dataframe
    possible_stages = ["home", "search", "product_view", "add_to_cart", "checkout", "confirmation", "order_placed", "payment_failed"]
    stages = [c for c in possible_stages if c in pdf.columns]
    
    # Sum up sessions that hit each stage (assuming > 0 means visited)
    funnel_counts = {stage: pdf[pdf[stage] > 0].shape[0] for stage in stages}
    funnel_df = pd.DataFrame(list(funnel_counts.items()), columns=['Stage', 'Sessions'])
    
    plt.figure(figsize=(10, 6))
    sns.barplot(data=funnel_df, x='Stage', y='Sessions', palette='rocket')
    plt.title('Conversion Funnel')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, "data/funnel_chart.png"))
    plt.close()
    print("Saved funnel_chart.png")

    # 2. Session Duration Distribution
    plt.figure(figsize=(10, 6))
    sns.histplot(pdf['duration_seconds'], bins=30, kde=True, color='coral')
    plt.title('Session Duration Distribution')
    plt.xlabel('Duration (seconds)')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(os.path.join(BASE_DIR, "data/session_duration.png"))
    plt.close()
    print("Saved session_duration.png")

# ---------------------------------------------------------------------------
# Gold layer
# ---------------------------------------------------------------------------
def process_gold():
    print("Processing Gold Layer (Analytics)...")
    df = spark.read.format("delta").load(SILVER_PATH)
    
    # Session level aggregates
    session_stats = df.groupBy("unique_session_id").agg(
        _max("event_timestamp").alias("end_time"),
        _min("event_timestamp").alias("start_time"),
        count("current_page").alias("page_views"),
        count(when(col("device") == "Mobile", True)).alias("mobile_sessions"),
        count(when(col("browser") == "Chrome", True)).alias("chrome_sessions")
    )
    session_stats = session_stats.withColumn(
        "duration_seconds",
        col("end_time").cast("long") - col("start_time").cast("long")
    )
    
    # Funnel pivot
    funnel = df.groupBy("unique_session_id").pivot("current_page").count().na.fill(0)
    
    # Ensure extra columns exist
    for extra in ["payment_failed", "order_placed"]:
        if extra not in funnel.columns:
            funnel = funnel.withColumn(extra, lit(0))
            
    gold_df = session_stats.join(funnel, "unique_session_id")
    # Use overwriteSchema option to handle new columns (e.g. payment_failed, order_placed)
    gold_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PATH)
    print(f"Gold layer written to {GOLD_PATH}")
    
    # Export results
    export_path_csv = os.path.join(BASE_DIR, "data/output/gold_report.csv")
    export_path_parquet = os.path.join(BASE_DIR, "data/output/gold_report.parquet")
    gold_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(export_path_csv)
    gold_df.write.mode("overwrite").parquet(export_path_parquet)
    print(f"Exported Gold results to {export_path_csv}")

    # Generate Plots
    generate_plots(gold_df)

    # Sample output
    print("\n--- Gold Layer Sample ---")
    gold_df.show(5)

def run():
    process_bronze()
    print("\n--- Verifying Bronze Layer Data ---")
    spark.read.format("delta").load(BRONZE_PATH).show(5)
    
    process_silver()
    print("\n--- Verifying Silver Layer Data ---")
    spark.read.format("delta").load(SILVER_PATH).show(5)
    
    process_gold()
    spark.stop()

if __name__ == "__main__":
    run()
