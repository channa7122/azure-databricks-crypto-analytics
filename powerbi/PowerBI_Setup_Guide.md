# Power BI Desktop Step-by-Step Setup Guide

Follow these 4 simple steps to open, configure, and visualize the Crypto Market Analytics dataset in Power BI Desktop in under 2 minutes:

---

### Step 1: Ingest Data Tables
1. Open **Power BI Desktop**.
2. Click **Get Data** -> **Folder** (or **Text/CSV**) and select this `powerbi/` directory containing:
   - `dim_coin.csv`
   - `dim_date.csv`
   - `fact_marketdata.csv`
3. Click **Load** (no transformation needed; the PySpark pipeline has already cleaned and validated all data types).

---

### Step 2: Validate Model Relationships
1. Go to the **Model View** tab (left-hand sidebar icon 🗂️).
2. Confirm the Star Schema relationships:
   - Drag `dim_coin[coin_id]` -> `fact_marketdata[coin_id]` (1 to Many `1:*`, Single direction).
   - Drag `dim_date[Date]` -> `fact_marketdata[Date]` (1 to Many `1:*`, Single direction).
3. Mark `dim_date` as a Date Table:
   - Right-click `dim_date` -> **Mark as date table** -> Select `Date` column.

---

### Step 3: Add Key DAX Measures
1. In the Fields pane, right-click `fact_marketdata` -> **New Measure**.
2. Copy and paste the DAX formulas from `DAX_Measures.dax`:
   - `[Total Market Cap]`
   - `[Total Volume 24h]`
   - `[7D SMA Price]`
   - `[30D Volatility]`
   - `[Daily Return %]`
   - `[Market Cap Dominance %]`
   - `[Risk Classification]`

---

### Step 4: Recreate Executive Dashboards
Refer to the visual reference mockups in `dashboards/`:

#### Page 1: Market Overview & Trends (`Dashboard1_Market_Overview.png`)
- **Top Line Chart:** X-axis = `dim_date[Date]`, Y-axis = `AVERAGE(fact_marketdata[price_usd])` and `[7D SMA Price]`. Slicer on `dim_coin[coin_name]` = "bitcoin".
- **Top Bar Chart:** Y-axis = `dim_coin[coin_name]`, X-axis = `[Total Market Cap]`. Filtered to latest date.
- **Bottom Area Chart:** X-axis = `dim_date[Date]`, Y-axis = `[7D SMA Volume]`, Legend = `dim_coin[coin_name]`.
- **Bottom Trend Chart:** X-axis = `dim_date[Date]`, Y-axis = `[Total Market Cap]`.

#### Page 2: Risk & Volatility Analytics (`Dashboard2_Risk_Analytics.png`)
- **Top Horizontal Bar:** Y-axis = `dim_coin[coin_name]`, X-axis = `[30D Volatility]`. Conditional formatting: Green (<2.5%), Orange (2.5-3.5%), Red (>3.5%).
- **Top Volatility Line Chart:** X-axis = `dim_date[Date]`, Y-axis = `[30D Volatility]`, Legend = Blue-chips vs High-Beta coins.
- **Bottom Density / Histogram:** Distribution of `fact_marketdata[daily_return_pct]` comparing Bitcoin vs Solana.
- **Bottom Scatter Plot:** X-axis = `[30D Volatility]`, Y-axis = `[Daily Return %]`, Values = `dim_coin[coin_name]`, Bubble Size = `[Total Market Cap]`.
