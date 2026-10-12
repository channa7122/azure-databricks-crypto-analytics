#!/usr/bin/env python3
"""
========================================================================================
Executive Dashboard Generator: Market Overview & Risk Analytics
Author: Mohamed Channa (Data Science, Big Data & AI - ENSA Tétouan)
Description: Generates high-resolution executive dashboards (2400x1500 PNG) directly
             from Gold Star Schema tables using matplotlib & seaborn.
========================================================================================
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

def generate_dashboards():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(base_dir) if os.path.basename(base_dir) == "src" else base_dir
    gold_dir = os.path.join(repo_root, "data", "gold")
    out_dir = os.path.join(repo_root, "dashboards")
    os.makedirs(out_dir, exist_ok=True)

    print(">>> Loading Gold Star Schema tables...")
    dim_coin = pd.read_csv(os.path.join(gold_dir, "dim_coin.csv"))
    dim_date = pd.read_csv(os.path.join(gold_dir, "dim_date.csv"))
    fact = pd.read_csv(os.path.join(gold_dir, "fact_marketdata.csv"))

    df = fact.merge(dim_coin, on="coin_id").merge(dim_date, on="Date")
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values(["coin_name", "Date"])

    # Theme styling
    plt.rcParams["font.sans-serif"] = "DejaVu Sans"
    plt.rcParams["axes.edgecolor"] = "#E0E0E0"
    plt.rcParams["axes.linewidth"] = 0.8

    # ==============================================================================
    # DASHBOARD 1: MARKET OVERVIEW & TRENDS
    # ==============================================================================
    print(">>> Generating Dashboard 1: Market Overview & Trends...")
    fig, axes = plt.subplots(2, 2, figsize=(20, 12.5), dpi=120)
    fig.patch.set_facecolor("#F8F9FA")
    fig.suptitle("CRYPTO MARKET ANALYTICS | DASHBOARD 1: MARKET OVERVIEW & TRENDS",
                 fontsize=22, fontweight="bold", color="#1A202C", y=0.98)

    # 1. BTC Spot vs 7-Day SMA
    ax1 = axes[0, 0]
    ax1.set_facecolor("#FFFFFF")
    btc_df = df[df["coin_name"] == "bitcoin"].sort_values("Date")
    ax1.plot(btc_df["Date"], btc_df["price_usd"], label="Bitcoin Price ($)", color="#1E3A8A", linewidth=2.2)
    ax1.plot(btc_df["Date"], btc_df["7_day_sma_price"], label="7-Day SMA ($)", color="#F59E0B", linewidth=2.0, linestyle="--")
    ax1.set_title("Bitcoin: Spot Price vs. 7-Day Simple Moving Average (SMA)", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax1.set_ylabel("USD ($)", fontsize=11, fontweight="semibold")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax1.legend(loc="upper left", frameon=True, facecolor="#F8FAFC", edgecolor="#CBD5E1")
    ax1.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    # 2. Top Cryptos by Latest Market Cap
    ax2 = axes[0, 1]
    ax2.set_facecolor("#FFFFFF")
    latest_date = df["Date"].max()
    latest_df = df[df["Date"] == latest_date].sort_values("market_cap", ascending=True)
    top_cap = latest_df.tail(8)
    bars = ax2.barh(top_cap["coin_name"].str.capitalize(), top_cap["market_cap"] / 1e9, color="#2563EB", height=0.6)
    ax2.set_title(f"Top Cryptos by Market Capitalization ($ Billions) [{latest_date.strftime('%b %Y')}]", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax2.set_xlabel("Market Cap ($ Billions)", fontsize=11, fontweight="semibold")
    ax2.grid(True, axis="x", linestyle=":", alpha=0.6, color="#CBD5E1")
    for bar in bars:
        w = bar.get_width()
        ax2.text(w + 15, bar.get_y() + bar.get_height()/2, f"${w:.1f}B", va="center", ha="left", fontsize=9.5, fontweight="bold", color="#1E293B")

    # 3. 7-Day Rolling Volume Trends (Top 5 Coins)
    ax3 = axes[1, 0]
    ax3.set_facecolor("#FFFFFF")
    top5_coins = ["bitcoin", "ethereum", "solana", "ripple", "dogecoin"]
    colors = ["#1E3A8A", "#7C3AED", "#10B981", "#0284C7", "#D97706"]
    for coin, col in zip(top5_coins, colors):
        c_df = df[df["coin_name"] == coin]
        ax3.plot(c_df["Date"], c_df["7_day_sma_volume"] / 1e9, label=coin.capitalize(), color=col, linewidth=1.8)
    ax3.set_title("7-Day Rolling Volume Trends (Top 5 Cryptos in $ Billions)", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax3.set_ylabel("7-Day SMA Volume ($B)", fontsize=11, fontweight="semibold")
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax3.legend(loc="upper right", frameon=True, facecolor="#F8FAFC", edgecolor="#CBD5E1")
    ax3.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    # 4. Total Market Cap Evolution
    ax4 = axes[1, 1]
    ax4.set_facecolor("#FFFFFF")
    total_mcap = df.groupby("Date")["market_cap"].sum() / 1e12
    ax4.plot(total_mcap.index, total_mcap.values, color="#0D9488", linewidth=2.4)
    ax4.fill_between(total_mcap.index, total_mcap.values, color="#0D9488", alpha=0.15)
    ax4.set_title("Total Crypto Market Capitalization Evolution ($ Trillion)", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax4.set_ylabel("Total Market Cap ($ Trillion)", fontsize=11, fontweight="semibold")
    ax4.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax4.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    plt.tight_layout(rect=[0.02, 0.02, 0.98, 0.95])
    dash1_path = os.path.join(out_dir, "Dashboard1_Market_Overview.png")
    fig.savefig(dash1_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"    -> Saved Dashboard 1 to: {dash1_path}")

    # ==============================================================================
    # DASHBOARD 2: RISK & VOLATILITY ANALYTICS
    # ==============================================================================
    print(">>> Generating Dashboard 2: Risk & Volatility Analytics...")
    fig, axes = plt.subplots(2, 2, figsize=(20, 12.5), dpi=120)
    fig.patch.set_facecolor("#F8F9FA")
    fig.suptitle("CRYPTO MARKET ANALYTICS | DASHBOARD 2: RISK & VOLATILITY ANALYTICS",
                 fontsize=22, fontweight="bold", color="#1A202C", y=0.98)

    # 1. 30-Day Volatility Ranking
    ax1 = axes[0, 0]
    ax1.set_facecolor("#FFFFFF")
    vol_rank = latest_df.dropna(subset=["30_day_volatility"]).sort_values("30_day_volatility", ascending=True)
    colors_vol = ["#10B981" if v < 2.5 else "#F59E0B" if v < 3.5 else "#EF4444" for v in vol_rank["30_day_volatility"]]
    bars = ax1.barh(vol_rank["coin_name"].str.capitalize(), vol_rank["30_day_volatility"], color=colors_vol, height=0.65)
    ax1.set_title("30-Day Rolling Volatility Ranking (% StdDev) [Latest]", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax1.set_xlabel("Volatility (Rolling 30-Day Standard Deviation %)", fontsize=11, fontweight="semibold")
    ax1.grid(True, axis="x", linestyle=":", alpha=0.6, color="#CBD5E1")
    for bar in bars:
        w = bar.get_width()
        ax1.text(w + 0.05, bar.get_y() + bar.get_height()/2, f"{w:.2f}%", va="center", ha="left", fontsize=9, fontweight="bold", color="#1E293B")

    # 2. 30-Day Volatility Over Time: Blue-Chips vs High-Beta
    ax2 = axes[0, 1]
    ax2.set_facecolor("#FFFFFF")
    key_coins = [("bitcoin", "#1E3A8A"), ("ethereum", "#7C3AED"), ("solana", "#10B981"), ("polkadot", "#EC4899")]
    for coin, col in key_coins:
        c_df = df[df["coin_name"] == coin]
        ax2.plot(c_df["Date"], c_df["30_day_volatility"], label=coin.capitalize(), color=col, linewidth=2.0)
    ax2.set_title("30-Day Volatility Over Time: Blue-Chips vs High-Beta Coins", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax2.set_ylabel("Rolling Volatility (% StdDev)", fontsize=11, fontweight="semibold")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.legend(loc="upper right", frameon=True, facecolor="#F8FAFC", edgecolor="#CBD5E1")
    ax2.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    # 3. Daily Returns Distribution (Fat-Tail Risk: BTC vs SOL)
    ax3 = axes[1, 0]
    ax3.set_facecolor("#FFFFFF")
    btc_ret = df[df["coin_name"] == "bitcoin"]["daily_return_pct"].dropna()
    sol_ret = df[df["coin_name"] == "solana"]["daily_return_pct"].dropna()
    sns.kdeplot(btc_ret, ax=ax3, label="Bitcoin (Stable Blue-Chip)", color="#1E3A8A", linewidth=2.5, fill=True, alpha=0.25)
    sns.kdeplot(sol_ret, ax=ax3, label="Solana (High-Beta)", color="#10B981", linewidth=2.5, fill=True, alpha=0.25)
    ax3.axvline(0, color="#64748B", linestyle="--", linewidth=1.2)
    ax3.set_title("Daily Returns Distribution (%): Bitcoin vs Solana (Fat-Tail Risk)", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax3.set_xlabel("Daily Return (%)", fontsize=11, fontweight="semibold")
    ax3.set_ylabel("Density", fontsize=11, fontweight="semibold")
    ax3.set_xlim(-15, 15)
    ax3.legend(loc="upper right", frameon=True, facecolor="#F8FAFC", edgecolor="#CBD5E1")
    ax3.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    # 4. Risk-Return Profile (Mean Volatility vs Mean Daily Return)
    ax4 = axes[1, 1]
    ax4.set_facecolor("#FFFFFF")
    summary = df.groupby("coin_name").agg(
        mean_vol=("30_day_volatility", "mean"),
        mean_ret=("daily_return_pct", "mean"),
        mcap=("market_cap", "last")
    ).reset_index()
    ax4.scatter(summary["mean_vol"], summary["mean_ret"], s=summary["mcap"] / 3e9 + 80, c="#3B82F6", alpha=0.7, edgecolors="#1E3A8A", linewidths=1.5)
    for _, row in summary.iterrows():
        ax4.annotate(row["coin_name"].capitalize(), (row["mean_vol"], row["mean_ret"]),
                     textcoords="offset points", xytext=(6, 4), fontsize=9, fontweight="bold", color="#1E293B")
    ax4.axhline(0, color="#EF4444", linestyle=":", linewidth=1.2)
    ax4.set_title("Risk-Return Profile: Mean 30-Day Volatility vs Mean Daily Return", fontsize=14, fontweight="bold", pad=12, color="#2D3748")
    ax4.set_xlabel("Average 30-Day Volatility (%)", fontsize=11, fontweight="semibold")
    ax4.set_ylabel("Average Daily Return (%)", fontsize=11, fontweight="semibold")
    ax4.grid(True, linestyle=":", alpha=0.6, color="#CBD5E1")

    plt.tight_layout(rect=[0.02, 0.02, 0.98, 0.95])
    dash2_path = os.path.join(out_dir, "Dashboard2_Risk_Analytics.png")
    fig.savefig(dash2_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"    -> Saved Dashboard 2 to: {dash2_path}")

    print(">>> [SUCCESS] Both executive dashboards generated perfectly!")

if __name__ == "__main__":
    generate_dashboards()
