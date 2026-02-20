-- =============================================================
-- mart_daily_returns.sql
-- Mart model: calculates daily and cumulative returns per ticker
-- Source: int_stock_with_fundamentals
-- Materialized as TABLE (queried by dashboard)
-- =============================================================

WITH base AS (
    SELECT * FROM {{ ref('int_stock_with_fundamentals') }}
),

with_returns AS (
    SELECT
        ticker,
        company_name,
        sector,
        industry,
        market_cap_category,
        trade_date,
        trade_year,
        trade_month,
        trade_month_start,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        pct_of_52w_high,

        -- Previous day's closing price (per ticker)
        LAG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS prev_close_price,

        -- Daily return: percentage change from previous close
        ROUND(
            (close_price - LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            )) / NULLIF(LAG(close_price) OVER (
                PARTITION BY ticker ORDER BY trade_date
            ), 0) * 100,
        4) AS daily_return_pct,

        -- Daily price range
        ROUND(high_price - low_price, 4)    AS daily_range,

        -- Dollar volume traded
        ROUND(close_price * volume, 2)       AS dollar_volume

    FROM base
),

with_cumulative AS (
    SELECT
        *,

        -- Cumulative return from first trading day in dataset
        ROUND(
            (close_price / FIRST_VALUE(close_price) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) - 1) * 100,
        4) AS cumulative_return_pct,

        -- Row number for each ticker (1 = earliest date)
        ROW_NUMBER() OVER (
            PARTITION BY ticker ORDER BY trade_date
        ) AS trading_day_number

    FROM with_returns
)

SELECT
    ticker,
    company_name,
    sector,
    industry,
    market_cap_category,
    trade_date,
    trade_year,
    trade_month,
    trade_month_start,
    trading_day_number,
    open_price,
    high_price,
    low_price,
    close_price,
    prev_close_price,
    volume,
    dollar_volume,
    daily_range,
    daily_return_pct,
    cumulative_return_pct,
    pct_of_52w_high

FROM with_cumulative
ORDER BY ticker, trade_date