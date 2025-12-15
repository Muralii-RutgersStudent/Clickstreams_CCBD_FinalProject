import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import pandas as pd
import os
from delta import *

# Initialize Spark (needed to read Delta tables)
builder = SparkSession.builder \
    .appName("ClickstreamViz") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

BASE_DIR = os.getcwd()
GOLD_PATH = os.path.join(BASE_DIR, "delta/gold")

def create_visualizations():
    print("Reading Gold Layer data...")
    if not os.path.exists(GOLD_PATH):
        print("Gold layer not found. Run pipeline.py first.")
        return

    df = spark.read.format("delta").load(GOLD_PATH)
    
    # Convert to Pandas for plotting (data should be aggregated enough)
    pdf = df.toPandas()
    
    # 1. Funnel Visualization
    stages = ["home", "search", "product_view", "add_to_cart", "checkout", "confirmation"]
    counts = []
    
    # Calculate counts for each stage (users who visited at least once)
    # Note: In a real funnel, we might want strict order, but this is a simplified view
    for stage in stages:
        if stage in pdf.columns:
            count = pdf[pdf[stage] > 0].shape[0]
            counts.append(count)
        else:
            counts.append(0)
            
    plt.figure(figsize=(10, 6))
    bars = plt.bar(stages, counts, color='skyblue')
    
    # Add value labels
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                 f'{int(height)}',
                 ha='center', va='bottom')
                 
    plt.title('Conversion Funnel')
    plt.xlabel('Stage')
    plt.ylabel('Number of Sessions')
    plt.savefig('funnel_chart.png')
    print("Saved funnel_chart.png")
    
    # 2. Session Duration Distribution
    plt.figure(figsize=(10, 6))
    plt.hist(pdf['duration_seconds'] / 60, bins=30, color='lightgreen', edgecolor='black')
    plt.title('Session Duration Distribution')
    plt.xlabel('Duration (minutes)')
    plt.ylabel('Frequency')
    plt.savefig('session_duration.png')
    print("Saved session_duration.png")

if __name__ == "__main__":
    create_visualizations()
    spark.stop()
