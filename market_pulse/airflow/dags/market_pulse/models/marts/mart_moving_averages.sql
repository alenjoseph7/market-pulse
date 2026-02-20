-- =============================================================
-- mart_moving_averages.sql
-- Mart model: calculates 7, 30, 90 day moving averages
-- Used for trend analysis in the dashboard
-- =============================================================

WITH base AS (
    SELECT * FROM {{ ref('mart_daily_returns') }}
),

moving_averages AS (
    SELECT
        ticker,
        company_name,
        sector,
        trade_date,
        close_price,
        daily_return_pct,

        -- 7-day moving average (short term trend)
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 4) AS ma_7_day,

        -- 30-day moving average (medium term trend)
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS ma_30_day,

        -- 90-day moving average (long term trend)
        ROUND(AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ), 4) AS ma_90_day,

        -- 30-day rolling volatility (standard deviation of daily returns)
        ROUND(STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 4) AS volatility_30_day,

        -- 30-day average volume
        ROUND(AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 0) AS avg_volume_30_day

    FROM base
),

with_signals AS (
    SELECT
        *,
        -- Golden cross signal: short term MA crosses above long term MA
        CASE
            WHEN ma_7_day > ma_30_day THEN 'BULLISH'
            WHEN ma_7_day < ma_30_day THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END AS trend_signal,

        -- Price vs moving averages
        ROUND(((close_price / NULLIF(ma_30_day, 0)) - 1) * 100, 2)
            AS pct_above_ma_30

    FROM moving_averages
)

SELECT * FROM with_signals
ORDER BY ticker, trade_date