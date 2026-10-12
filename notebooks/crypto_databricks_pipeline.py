"""
========================================================================================
Project: Azure Databricks Crypto Market Analytics & Medallion Pipeline
Author: Mohamed Channa (Data Science, Big Data & AI - ENSA Tétouan)
Description: End-to-end PySpark ETL pipeline implementing Bronze -> Silver -> Gold
             (Medallion Architecture) with advanced Window Functions and Star Schema
             dimensional modeling for Power BI Executive Dashboards.
========================================================================================
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType
)

def create_spark_session(app_name="Crypto_Market_Analytics_Pipeline"):
    """
    Initializes a production-configured SparkSession.
    Compatible with Databricks Runtime and local Spark cluster.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

def run_pipeline(bronze_dir="data/bronze", silver_dir="data/silver", gold_dir="data/gold"):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("=" * 80)
    print(">>> [STEP 0] Spark Session Initialized successfully.")
    print("=" * 80)

    # ----------------------------------------------------------------------------------
    # 1. BRONZE LAYER -> SILVER LAYER (Extraction, Flattening, Cleaning)
    # ----------------------------------------------------------------------------------
    print("\n>>> [STEP 1] Ingesting Bronze JSON files & Transforming to Silver Layer...")
    
    # CoinGecko market_chart schema: arrays of [unix_timestamp_ms, value]
    point_schema = ArrayType(DoubleType())
    chart_schema = StructType([
        StructField("prices", ArrayType(point_schema), True),
        StructField("market_caps", ArrayType(point_schema), True),
        StructField("total_volumes", ArrayType(point_schema), True)
    ])

    json_files = [f for f in os.listdir(bronze_dir) if f.endswith(".json")]
    print(f"    Found {len(json_files)} raw JSON files to ingest from Bronze.")

    silver_dfs = []
    for json_file in json_files:
        coin_name = json_file.replace(".json", "")
        file_path = os.path.join(bronze_dir, json_file)

        raw_df = spark.read.schema(chart_schema).json(file_path)
        
        # Explode prices
        prices_df = (
            raw_df.select(F.posexplode("prices").alias("pos", "price_point"))
            .select(
                F.col("pos"),
                (F.col("price_point")[0] / 1000).cast(LongType()).alias("timestamp_sec"),
                F.col("price_point")[1].cast(DoubleType()).alias("price_usd")
            )
        )

        # Explode market_caps
        caps_df = (
            raw_df.select(F.posexplode("market_caps").alias("pos", "cap_point"))
            .select(
                F.col("pos"),
                F.col("cap_point")[1].cast(DoubleType()).alias("market_cap")
            )
        )

        # Explode volumes
        vols_df = (
            raw_df.select(F.posexplode("total_volumes").alias("pos", "vol_point"))
            .select(
                F.col("pos"),
                F.col("vol_point")[1].cast(DoubleType()).alias("volume")
            )
        )

        # Harmonize on position index
        combined_df = (
            prices_df
            .join(caps_df, on="pos", how="inner")
            .join(vols_df, on="pos", how="inner")
            .withColumn("coin_name", F.lit(coin_name))
            .withColumn("Date", F.to_date(F.from_unixtime("timestamp_sec")))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .select("coin_name", "price_usd", "market_cap", "volume", "Date", "ingestion_timestamp")
        )

        # Deduplicate per coin and date
        clean_coin_df = combined_df.dropDuplicates(["coin_name", "Date"])
        silver_dfs.append(clean_coin_df)

    # Union all coins
    silver_df = silver_dfs[0]
    for df in silver_dfs[1:]:
        silver_df = silver_df.union(df)

    total_silver_records = silver_df.count()
    print(f"    Silver Layer complete: {total_silver_records} cleaned observations.")

    # Save to Silver (Delta format in Databricks, Parquet fallback)
    silver_output_path = os.path.join(silver_dir, "crypto_silver_clean.parquet")
    silver_df.write.mode("overwrite").parquet(silver_output_path)
    print(f"    Saved Silver dataset to: {silver_output_path}")

    # ----------------------------------------------------------------------------------
    # 2. SILVER LAYER -> GOLD LAYER (Dimensional Modeling & Window Functions)
    # ----------------------------------------------------------------------------------
    print("\n>>> [STEP 2] Building Star Schema Dimension & Fact Tables (Gold Layer)...")

    # A. Dimension Table: dim_coin
    distinct_coins = silver_df.select("coin_name").distinct().sort("coin_name")
    coin_window = Window.orderBy("coin_name")
    dim_coin = distinct_coins.withColumn("coin_id", F.row_number().over(coin_window))
    
    os.makedirs(gold_dir, exist_ok=True)
    dim_coin_pd = dim_coin.toPandas()
    dim_coin_pd.to_csv(os.path.join(gold_dir, "dim_coin.csv"), index=False)
    print(f"    dim_coin created: {len(dim_coin_pd)} distinct cryptocurrencies.")

    # B. Dimension Table: dim_date
    distinct_dates = silver_df.select("Date").distinct().sort("Date")
    dim_date = (
        distinct_dates
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Day", F.dayofmonth("Date"))
        .withColumn("DayOfWeek", F.dayofweek("Date")) # 1=Sun, 7=Sat
        .withColumn("is_Weekend", F.when(F.col("DayOfWeek").isin([1, 7]), True).otherwise(False))
    )
    dim_date_pd = dim_date.toPandas()
    dim_date_pd.to_csv(os.path.join(gold_dir, "dim_date.csv"), index=False)
    print(f"    dim_date created: {len(dim_date_pd)} distinct calendar days.")

    # C. Fact Table: fact_marketdata with PySpark Window Functions
    # Join coin_id
    enriched_df = silver_df.join(dim_coin, on="coin_name", how="inner")

    # Define Window Specifications
    w_coin = Window.partitionBy("coin_id").orderBy("Date")
    w_7d = Window.partitionBy("coin_id").orderBy("Date").rowsBetween(-6, 0)
    w_30d = Window.partitionBy("coin_id").orderBy("Date").rowsBetween(-29, 0)

    # Calculate financial analytical metrics
    fact_marketdata = (
        enriched_df
        .withColumn("lag_price", F.lag("price_usd", 1).over(w_coin))
        .withColumn(
            "daily_return_pct",
            F.when(F.col("lag_price").isNotNull() & (F.col("lag_price") > 0),
                   ((F.col("price_usd") - F.col("lag_price")) / F.col("lag_price")) * 100.0)
            .otherwise(None)
        )
        .withColumn("7_day_sma_price", F.avg("price_usd").over(w_7d))
        .withColumn("30_day_volatility", F.stddev("daily_return_pct").over(w_30d))
        .withColumn("7_day_sma_volume", F.avg("volume").over(w_7d))
        .select(
            "coin_id",
            "Date",
            "price_usd",
            "market_cap",
            "volume",
            "daily_return_pct",
            "7_day_sma_price",
            "30_day_volatility",
            "7_day_sma_volume"
        )
        .sort("coin_id", "Date")
    )

    fact_pd = fact_marketdata.toPandas()
    fact_csv_path = os.path.join(gold_dir, "fact_marketdata.csv")
    fact_pd.to_csv(fact_csv_path, index=False)
    print(f"    fact_marketdata created: {len(fact_pd)} rows with all analytical metrics.")

    print("\n" + "=" * 80)
    print(">>> [SUCCESS] Pipeline execution finished! Gold tables ready for Power BI.")
    print("=" * 80)

if __name__ == "__main__":
    run_pipeline()
