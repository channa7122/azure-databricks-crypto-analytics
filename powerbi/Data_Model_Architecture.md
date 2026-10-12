# Power BI Data Model Architecture & Star Schema Guide

## 1. Relational Model Overview
The analytical data model follows **Kimball Star Schema** methodology, decoupled from operational OLTP structures for high-performance interactive querying in Power BI VertiPaq engine:

```
      +------------------------+             +------------------------+
      |        dim_date        |             |        dim_coin        |
      +------------------------+             +------------------------+
      | Date (PK) [Date]       |             | coin_id (PK) [Int]     |
      | Year [Int]             |             | coin_name [String]     |
      | Month [Int]            |             +------------------------+
      | Day [Int]              |                         | 1
      | DayOfWeek [Int]        |                         |
      | is_Weekend [Boolean]   |                         |
      +------------------------+                         |
                  | 1                                    |
                  |                                      |
                  | *                                  * |
      +--------------------------------------------------+-----+
      |                    fact_marketdata                     |
      +--------------------------------------------------------+
      | coin_id (FK) [Int]                                     |
      | Date (FK) [Date]                                       |
      | price_usd [Decimal/Float]                              |
      | market_cap [Decimal/Float]                             |
      | volume [Decimal/Float]                                 |
      | daily_return_pct [Float] (Window Lag return)           |
      | 7_day_sma_price [Float] (Window 7D SMA)                |
      | 30_day_volatility [Float] (Window 30D Rolling StdDev)  |
      | 7_day_sma_volume [Float] (Window 7D SMA)               |
      +--------------------------------------------------------+
```

## 2. Table Relationships & Cardinality
1. **dim_date to fact_marketdata**:
   - **From Column:** `dim_date[Date]` (Type: Date)
   - **To Column:** `fact_marketdata[Date]` (Type: Date)
   - **Cardinality:** One-to-Many (`1 -> *`)
   - **Cross Filter Direction:** Single (dim_date filters fact_marketdata)

2. **dim_coin to fact_marketdata**:
   - **From Column:** `dim_coin[coin_id]` (Type: Whole Number)
   - **To Column:** `fact_marketdata[coin_id]` (Type: Whole Number)
   - **Cardinality:** One-to-Many (`1 -> *`)
   - **Cross Filter Direction:** Single (dim_coin filters fact_marketdata)

## 3. Data Dictionary
| Table | Column | Data Type | Role | Description |
| :--- | :--- | :--- | :--- | :--- |
| `dim_coin` | `coin_id` | Whole Number | Primary Key | Surrogate integer key for crypto asset |
| `dim_coin` | `coin_name` | Text | Attribute | Standardized name (e.g., bitcoin, solana, ethereum) |
| `dim_date` | `Date` | Date | Primary Key | Calendar date (YYYY-MM-DD) |
| `dim_date` | `Year` | Whole Number | Attribute | Calendar Year (2025, 2026) |
| `dim_date` | `Month` | Whole Number | Attribute | Calendar Month (1 to 12) |
| `dim_date` | `Day` | Whole Number | Attribute | Day of Month (1 to 31) |
| `dim_date` | `DayOfWeek` | Whole Number | Attribute | Day of week index (1 to 7) |
| `dim_date` | `is_Weekend` | True/False | Attribute | Weekend flag (Saturday/Sunday) |
| `fact_marketdata` | `coin_id` | Whole Number | Foreign Key | References `dim_coin[coin_id]` |
| `fact_marketdata` | `Date` | Date | Foreign Key | References `dim_date[Date]` |
| `fact_marketdata` | `price_usd` | Decimal Number | Fact Metric | End-of-day spot price in USD |
| `fact_marketdata` | `market_cap` | Decimal Number | Fact Metric | Market capitalization in USD |
| `fact_marketdata` | `volume` | Decimal Number | Fact Metric | 24-hour traded volume in USD |
| `fact_marketdata` | `daily_return_pct` | Decimal Number | Analytical Metric | 1-day percentage price change (%) |
| `fact_marketdata` | `7_day_sma_price` | Decimal Number | Analytical Metric | 7-day Simple Moving Average of price |
| `fact_marketdata` | `30_day_volatility` | Decimal Number | Analytical Metric | 30-day rolling standard deviation of returns (%) |
| `fact_marketdata` | `7_day_sma_volume` | Decimal Number | Analytical Metric | 7-day Simple Moving Average of volume |
