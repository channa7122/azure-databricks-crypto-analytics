import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

def run_local_pipeline():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(base_dir) if os.path.basename(base_dir) == "src" else base_dir

    bronze_dir = os.path.join(repo_root, "data", "bronze")
    silver_dir = os.path.join(repo_root, "data", "silver")
    gold_dir = os.path.join(repo_root, "data", "gold")
    powerbi_dir = os.path.join(repo_root, "powerbi")

    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)
    os.makedirs(powerbi_dir, exist_ok=True)

    print("=" * 80)
    print(">>> [CRYPTO ANALYTICS ETL] Exécution du pipeline local Bronze -> Silver -> Gold...")
    print("=" * 80)

    # 1. BRONZE -> SILVER
    print("\n[1/3] Aplatissement et nettoyage Bronze vers Silver...")
    json_files = sorted([f for f in os.listdir(bronze_dir) if f.endswith(".json")])
    if not json_files:
        raise FileNotFoundError(f"Aucun fichier JSON trouvé dans {bronze_dir} !")

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

        coin_days = {}
        for item in prices:
            dt = datetime.utcfromtimestamp(item[0] / 1000.0).strftime("%Y-%m-%d")
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

    silver_df = pd.DataFrame(records).drop_duplicates(subset=["coin_name", "Date"]).sort_values(["coin_name", "Date"])
    silver_df.to_parquet(os.path.join(silver_dir, "crypto_silver_clean.parquet"), index=False)
    print(f"✅ Couche Silver créée ({len(silver_df)} lignes)")

    # 2. DIMENSIONS GOLD
    print("\n[2/3] Création des dimensions du Schéma en Étoile...")
    coins = sorted(silver_df["coin_name"].unique())
    dim_coin = pd.DataFrame({"coin_name": coins, "coin_id": range(1, len(coins) + 1)})
    dim_coin.to_csv(os.path.join(gold_dir, "dim_coin.csv"), index=False)
    dim_coin.to_csv(os.path.join(powerbi_dir, "dim_coin.csv"), index=False)

    dates = pd.to_datetime(sorted(silver_df["Date"].unique()))
    dim_date = pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"),
        "Year": dates.year,
        "Month": dates.month,
        "Day": dates.day,
        "DayOfWeek": dates.dayofweek + 1,
        "is_Weekend": dates.dayofweek.isin([5, 6])
    })
    dim_date.to_csv(os.path.join(gold_dir, "dim_date.csv"), index=False)
    dim_date.to_csv(os.path.join(powerbi_dir, "dim_date.csv"), index=False)
    print(f"✅ Dimensions créées : dim_coin ({len(dim_coin)} actifs) et dim_date ({len(dim_date)} dates)")

    # 3. TABLE DE FAITS GOLD (WINDOW FUNCTIONS)
    print("\n[3/3] Calcul des métriques financières glissantes (Window Functions)...")
    merged = silver_df.merge(dim_coin, on="coin_name", how="inner")
    merged["Date"] = pd.to_datetime(merged["Date"])
    merged = merged.sort_values(["coin_id", "Date"])

    fact_records = []
    for cid, group in merged.groupby("coin_id"):
        g = group.sort_values("Date").copy()
        g["lag_price"] = g["price_usd"].shift(1)
        g["daily_return_pct"] = ((g["price_usd"] - g["lag_price"]) / g["lag_price"]) * 100.0
        g["7_day_sma_price"] = g["price_usd"].rolling(window=7, min_periods=1).mean()
        g["30_day_volatility"] = g["daily_return_pct"].rolling(window=30, min_periods=2).std()
        g["7_day_sma_volume"] = g["volume"].rolling(window=7, min_periods=1).mean()
        fact_records.append(g)

    fact_df = pd.concat(fact_records, ignore_index=True)
    fact_df["Date"] = fact_df["Date"].dt.strftime("%Y-%m-%d")

    cols = ["coin_id", "Date", "price_usd", "market_cap", "volume", "daily_return_pct", "7_day_sma_price", "30_day_volatility", "7_day_sma_volume"]
    fact_final = fact_df[cols].sort_values(["coin_id", "Date"])

    fact_final.to_csv(os.path.join(gold_dir, "fact_marketdata.csv"), index=False)
    fact_final.to_csv(os.path.join(powerbi_dir, "fact_marketdata.csv"), index=False)
    print(f"✅ fact_marketdata.csv généré ({len(fact_final)} lignes)")

    print("\n" + "=" * 80)
    print("🎉 SUCCÈS : Données générées dans data/gold/ et powerbi/ !")
    print("=" * 80)

if __name__ == "__main__":
    run_local_pipeline()