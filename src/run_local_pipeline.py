#!/usr/bin/env python3
"""
========================================================================================
Local Pipeline Runner: Bronze -> Silver -> Gold (Pandas/PyArrow Engine)
Author: Mohamed Channa (Data Science, Big Data & AI - ENSA Tétouan)
Description: Standalone executable pipeline that ingests raw Bronze JSON files,
             flattens and cleans them into Silver Parquet, computes Window-based
             financial indicators, and produces the Star Schema Gold CSVs.
========================================================================================
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

def run_local_pipeline():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    # Handle if executed from root or src/
    if os.path.basename(base_dir) == "src":
        repo_root = os.path.dirname(base_dir)
    else:
        repo_root = base_dir

    bronze_dir = os.path.join(repo_root, "data", "bronze")
    silver_dir = os.path.join(repo_root, "data", "silver")
    gold_dir = os.path.join(repo_root, "data", "gold")
    powerbi_dir = os.path.join(repo_root, "powerbi")

    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)
    os.makedirs(powerbi_dir, exist_ok=True)

    print("=" * 80)
    print(">>> [CRYPTO ANALYTICS ETL] Starting Local Pipeline Execution...")
    print("=" * 80)

    # 1. BRONZE INGESTION & SILVER TRANSFORMATION
    print("\n[STEP 1/3] Ingesting Bronze JSON files...")
    json_files = sorted([f for f in os.listdir(bronze_dir) if f.endswith(".json")])
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {bronze_dir}!")

    records = []
    ingestion_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for f in json_files:
        coin_name = f.replace(".json", "")
        file_path = os.path.join(bronze_dir, f)
        with open(file_path, "r", encoding="utf-8") as jf:
            data = json.load(jf)

        prices = data.get("prices", [])
        caps = data.get("market_caps", [])
        vols = data.get("total_volumes", [])

        # Build dictionary by date
        coin_days = {}
        for item in prices:
            ts_sec = item[0] / 1000.0
            dt = datetime.utcfromtimestamp(ts_sec).strftime("%Y-%m-%d")
            coin_days[dt] = {"price_usd": item[1], "market_cap": 0.0, "volume": 0.0}

        for item in caps:
            dt = datetime.utcfromtimestamp(item[0] / 1000.0).strftime("%Y-%m-%d")
            if dt in coin_days:
                coin_days[dt]["market_cap"] = item[1]

        for item in vols:
            dt = datetime.utcfromtimestamp(item[0] / 1000.0).strftime("%Y-%m-%d")
            if dt in coin_days:
                coin_days[dt]["volume"] = item[1]

        for dt, vals in coin_days.items():
            records.append({
                "coin_name": coin_name,
                "price_usd": vals["price_usd"],
                "market_cap": vals["market_cap"],
                "volume": vals["volume"],
                "Date": dt,
                "ingestion_timestamp": ingestion_ts
            })

    silver_df = pd.DataFrame(records)
    silver_df = silver_df.drop_duplicates(subset=["coin_name", "Date"]).sort_values(["coin_name", "Date"])
    
    silver_parquet_path = os.path.join(silver_dir, "crypto_silver_clean.parquet")
    silver_df.to_parquet(silver_parquet_path, index=False)
    print(f"    -> Silver Layer output saved: {len(silver_df)} records across {silver_df['coin_name'].nunique()} assets.")

    # 2. DIMENSION TABLES (GOLD)
    print("\n[STEP 2/3] Constructing Star Schema Dimensions...")
    
    # dim_coin
    coins = sorted(silver_df["coin_name"].unique())
    dim_coin = pd.DataFrame({
        "coin_name": coins,
        "coin_id": range(1, len(coins) + 1)
    })
    dim_coin.to_csv(os.path.join(gold_dir, "dim_coin.csv"), index=False)
    dim_coin.to_csv(os.path.join(powerbi_dir, "dim_coin.csv"), index=False)
    print(f"    -> dim_coin: {len(dim_coin)} cryptocurrencies with surrogate keys.")

    # dim_date
    dates = pd.to_datetime(sorted(silver_df["Date"].unique()))
    dim_date = pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"),
        "Year": dates.year,
        "Month": dates.month,
        "Day": dates.day,
        "DayOfWeek": dates.dayofweek + 1, # 1=Mon .. 7=Sun
        "is_Weekend": dates.dayofweek.isin([5, 6])
    })
    dim_date.to_csv(os.path.join(gold_dir, "dim_date.csv"), index=False)
    dim_date.to_csv(os.path.join(powerbi_dir, "dim_date.csv"), index=False)
    print(f"    -> dim_date: {len(dim_date)} days calendar dimension.")

    # 3. FACT TABLE WITH WINDOW FUNCTIONS (GOLD)
    print("\n[STEP 3/3] Generating Fact Table with Window Analytical Functions...")
    merged = silver_df.merge(dim_coin, on="coin_name", how="inner")
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged = merged.sort_values(["coin_id", "Date"])

    # Calculate rolling metrics per coin
    fact_records = []
    for cid, group in merged.groupby("coin_id"):
        group = group.sort_values("Date").copy()
        
        # 1-day lag return
        group["lag_price"] = group["price_usd"].shift(1)
        group["daily_return_pct"] = ((group["price_usd"] - group["lag_price"]) / group["lag_price"]) * 100.0
        
        # 7-day Simple Moving Average (Price)
        group["7_day_sma_price"] = group["price_usd"].rolling(window=7, min_periods=1).mean()
        
        # 30-day Rolling Volatility (Standard Deviation of daily returns)
        group["30_day_volatility"] = group["daily_return_pct"].rolling(window=30, min_periods=2).std()
        
        # 7-day Simple Moving Average (Volume)
        group["7_day_sma_volume"] = group["volume"].rolling(window=7, min_periods=1).mean()
        
        fact_records.append(group)

    fact_df = pd.concat(fact_records, ignore_index=True)
    fact_df["Date"] = fact_df["Date"].dt.strftime("%Y-%m-%d")

    fact_cols = [
        "coin_id",
        "Date",
        "price_usd",
        "market_cap",
        "volume",
        "daily_return_pct",
        "7_day_sma_price",
        "30_day_volatility",
        "7_day_sma_volume"
    ]
    fact_final = fact_df[fact_cols].sort_values(["coin_id", "Date"])

    fact_final.to_csv(os.path.join(gold_dir, "fact_marketdata.csv"), index=False)
    fact_final.to_csv(os.path.join(powerbi_dir, "fact_marketdata.csv"), index=False)
    print(f"    -> fact_marketdata: {len(fact_final)} rows generated with financial indicators.")

    print("\n" + "=" * 80)
    print(">>> [SUCCESS] All Star Schema Gold tables created successfully!")
    print(f"    Files exported to: {gold_dir}/ and {powerbi_dir}/")
    print("=" * 80)

if __name__ == "__main__":
    run_local_pipeline()
