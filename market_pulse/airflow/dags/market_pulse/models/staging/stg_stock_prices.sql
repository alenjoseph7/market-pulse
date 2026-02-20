-- =============================================================
-- stg_stock_prices.sql
-- Staging model: cleans, types, and deduplicates raw OHLCV data
-- Source: RAW.STOCK_PRICES.DAILY_OHLCV
-- =============================================================

WITH source AS (
    -- Pull everything from the raw table
    SELECT * FROM {{ source('raw_stock_prices', 'daily_ohlcv') }}
),

cleaned AS (
    SELECT
        -- Clean and cast all columns to proper types
        UPPER(TRIM(ticker))                     AS ticker,
        TRY_TO_DATE(trade_date, 'YYYY-MM-DD')   AS trade_date,
        TRY_TO_DOUBLE(open_price)               AS open_price,
        TRY_TO_DOUBLE(high_price)               AS high_price,
        TRY_TO_DOUBLE(low_price)                AS low_price,
        TRY_TO_DOUBLE(close_price)              AS close_price,
        TRY_TO_NUMBER(volume)                   AS volume,
        loaded_at

    FROM source

    -- Remove rows where critical fields are null after casting
    WHERE TRY_TO_DATE(trade_date, 'YYYY-MM-DD') IS NOT NULL
      AND TRY_TO_DOUBLE(close_price) IS NOT NULL
      AND TRIM(ticker) IS NOT NULL
),

deduplicated AS (
    -- Remove duplicates keeping the most recently loaded row
    -- This handles the duplicate rows we saw from multiple ingestion runs
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, trade_date  -- unique key
                ORDER BY loaded_at DESC          -- keep latest load
            ) AS row_num
        FROM cleaned
    )
    WHERE row_num = 1
)

SELECT
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,

    -- Derived columns useful downstream
    YEAR(trade_date)                        AS trade_year,
    MONTH(trade_date)                       AS trade_month,
    DAYOFWEEK(trade_date)                   AS day_of_week,
    DATE_TRUNC('month', trade_date)         AS trade_month_start

FROM deduplicated