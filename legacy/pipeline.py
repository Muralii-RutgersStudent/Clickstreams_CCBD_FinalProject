from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lag, sum as _sum, when, lit, count, max as _max, min as _min, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
import os
import shutil
from delta import *

# Initialize Spark Session with Delta support
builder = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Paths
BASE_DIR = os.getcwd()
RAW_DATA_PATH = os.path.join(BASE_DIR, "clickstream_data.json")
BRONZE_PATH = os.path.join(BASE_DIR, "delta/bronze")
SILVER_PATH = os.path.join(BASE_DIR, "delta/silver")
GOLD_PATH = os.path.join(BASE_DIR, "delta/gold")

# Clean up previous runs (for demo purposes)
if os.path.exists(os.path.join(BASE_DIR, "delta")):
    shutil.rmtree(os.path.join(BASE_DIR, "delta"))

def process_bronze():
    print("Processing Bronze Layer...")
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("current_page", StringType(), True),
        StructField("url", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("device", StringType(), True)
    ])
    df = spark.read.json(RAW_DATA_PATH, schema=schema)
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"Bronze layer written to {BRONZE_PATH}")

def process_silver():
    print("Processing Silver Layer (Sessionization)...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    df = df.withColumn("event_timestamp", to_timestamp(col("event_time")))
    window_spec = Window.partitionBy("user_id").orderBy("event_timestamp")
    df = df.withColumn("prev_time", lag("event_timestamp").over(window_spec))
    df = df.withColumn("time_diff_seconds", (col("event_timestamp").cast("long") - col("prev_time").cast("long")))
    df = df.withColumn("is_new_session", when((col("time_diff_seconds") > 1800) | (col("time_diff_seconds").isNull()), 1).otherwise(0))
    df = df.withColumn("session_id", _sum("is_new_session").over(window_spec))
    df = df.withColumn("unique_session_id", concat_ws("_", col("user_id"), col("session_id")))
    df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"Silver layer written to {SILVER_PATH}")

def process_gold():
    print("Processing Gold Layer (Funnel & Aggregations)...")
    df = spark.read.format("delta").load(SILVER_PATH)
    session_stats = df.groupBy("unique_session_id").agg(
        _max("event_timestamp").alias("end_time"),
        _min("event_timestamp").alias("start_time"),
        count("current_page").alias("page_views")
    )
    session_stats = session_stats.withColumn("duration_seconds", col("end_time").cast("long") - col("start_time").cast("long"))
    funnel = df.groupBy("unique_session_id").pivot("current_page").count().na.fill(0)
    gold_df = session_stats.join(funnel, "unique_session_id")
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print(f"Gold layer written to {GOLD_PATH}")
    print("\n--- Gold Layer Sample ---")
    gold_df.show(5)
    print("\n--- Funnel Summary ---")
    total_sessions = gold_df.count()
    print(f"Total Sessions: {total_sessions}")
    stages = ["home", "search", "product_view", "add_to_cart", "checkout", "confirmation"]
    for stage in stages:
        if stage in gold_df.columns:
            count_stage = gold_df.filter(col(stage) > 0).count()
            print(f"{stage}: {count_stage} ({count_stage/total_sessions*100:.1f}%)")

def run():
    process_bronze()
    process_silver()
    process_gold()
    spark.stop()

if __name__ == "__main__":
    run()

from pyspark.sql.functions import col, from_json, to_timestamp, window, lag, sum as _sum, when, lit, count, avg, max as _max
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import os
import shutil
from delta import *

# Initialize Spark Session
builder = SparkSession.builder \
    .appName("ClickstreamAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Paths
BASE_DIR = os.getcwd()
RAW_DATA_PATH = os.path.join(BASE_DIR, "clickstream_data.json")
BRONZE_PATH = os.path.join(BASE_DIR, "delta/bronze")
SILVER_PATH = os.path.join(BASE_DIR, "delta/silver")
GOLD_PATH = os.path.join(BASE_DIR, "delta/gold")

# Clean up previous runs (for demo purposes)
if os.path.exists(os.path.join(BASE_DIR, "delta")):
    shutil.rmtree(os.path.join(BASE_DIR, "delta"))

def process_bronze():
    print("Processing Bronze Layer...")
    # Schema for raw data
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("current_page", StringType(), True),
        StructField("url", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("device", StringType(), True)
    ])

    # Read raw JSON
    df = spark.read.json(RAW_DATA_PATH, schema=schema)
    
    # Write to Bronze (Delta)
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"Bronze layer written to {BRONZE_PATH}")

def process_silver():
    print("Processing Silver Layer (Sessionization)...")
    df = spark.read.format("delta").load(BRONZE_PATH)
    
    # Convert timestamp
    df = df.withColumn("event_timestamp", to_timestamp(col("event_time")))
    
    # Sessionization Logic
    # 1. Calculate time difference between events for each user
    window_spec = Window.partitionBy("user_id").orderBy("event_timestamp")
    
    df = df.withColumn("prev_time", lag("event_timestamp").over(window_spec))
    df = df.withColumn("time_diff_seconds", 
                       (col("event_timestamp").cast("long") - col("prev_time").cast("long")))
    
    # 2. Flag new session if time diff > 30 mins (1800 seconds) or null (first event)
    df = df.withColumn("is_new_session", 
                       when((col("time_diff_seconds") > 1800) | (col("time_diff_seconds").isNull()), 1).otherwise(0))
    
    # 3. Generate Session ID (cumulative sum of is_new_session)
    df = df.withColumn("session_id", 
                       _sum("is_new_session").over(window_spec))
    
    # Create a unique session ID string
    # Better unique session ID
    from pyspark.sql.functions import concat_ws
    df = df.withColumn("unique_session_id", concat_ws("_", col("user_id"), col("session_id")))

    # Write to Silver
    df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"Silver layer written to {SILVER_PATH}")

def process_gold():
    print("Processing Gold Layer (Funnel & Aggregations)...")
    df = spark.read.format("delta").load(SILVER_PATH)
    
    # 1. Session Metrics
    session_stats = df.groupBy("unique_session_id").agg(
        _max("event_timestamp").alias("end_time"),
        pyspark.sql.functions.min("event_timestamp").alias("start_time"),
        count("current_page").alias("page_views")
    )
    
    session_stats = session_stats.withColumn("duration_seconds", 
                                             col("end_time").cast("long") - col("start_time").cast("long"))
    
    # 2. Funnel Analysis
    # Check if session reached specific stages
    funnel = df.groupBy("unique_session_id").pivot("current_page").count().na.fill(0)
    
    # Join metrics with funnel
    gold_df = session_stats.join(funnel, "unique_session_id")
    
    # Write to Gold
    gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
    print(f"Gold layer written to {GOLD_PATH}")
    
    # Show some results
    print("\n--- Gold Layer Sample ---")
    gold_df.show(5)
    
    print("\n--- Funnel Summary ---")
    # Calculate conversion rates
    total_sessions = gold_df.count()
    print(f"Total Sessions: {total_sessions}")
    
    # Simple funnel counts (assuming if they hit the page, they are in that stage)
    # Note: Real funnel logic might require strict ordering, but this is a good approximation for the project
    stages = ["home", "search", "product_view", "add_to_cart", "checkout", "confirmation"]
    for stage in stages:
        if stage in gold_df.columns:
            count_stage = gold_df.filter(col(stage) > 0).count()
            print(f"{stage}: {count_stage} ({count_stage/total_sessions*100:.1f}%)")

import pyspark.sql.functions

if __name__ == "__main__":
    process_bronze()
    process_silver()
    process_gold()
    spark.stop()
