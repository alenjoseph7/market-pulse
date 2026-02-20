-- =============================================================
-- stg_company_fundamentals.sql
-- Staging model: cleans and types raw fundamentals data
-- Source: RAW.COMPANY_FUNDAMENTALS.FUNDAMENTALS
-- =============================================================

WITH source AS (
    SELECT * FROM {{ source('raw_fundamentals', 'fundamentals') }}
),

cleaned AS (
    SELECT
        UPPER(TRIM(ticker))             AS ticker,
        TRIM(company_name)              AS company_name,
        TRIM(sector)                    AS sector,
        TRIM(industry)                  AS industry,
        TRY_TO_NUMBER(market_cap)       AS market_cap,
        TRY_TO_DOUBLE(pe_ratio)         AS pe_ratio,
        TRY_TO_DOUBLE(fifty_two_week_high) AS fifty_two_week_high,
        TRY_TO_DOUBLE(fifty_two_week_low)  AS fifty_two_week_low,
        loaded_at

    FROM source

    WHERE TRIM(ticker) IS NOT NULL
      AND TRIM(company_name) IS NOT NULL
),

deduplicated AS (
    -- Keep most recently loaded record per ticker
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker
                ORDER BY loaded_at DESC
            ) AS row_num
        FROM cleaned
    )
    WHERE row_num = 1
)

SELECT
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    pe_ratio,
    fifty_two_week_high,
    fifty_two_week_low,

    -- Derived: market cap category for easy grouping
    CASE
        WHEN market_cap >= 200000000000 THEN 'Mega Cap'   -- $200B+
        WHEN market_cap >= 10000000000  THEN 'Large Cap'  -- $10B+
        WHEN market_cap >= 2000000000   THEN 'Mid Cap'    -- $2B+
        ELSE 'Small Cap'
    END AS market_cap_category

FROM deduplicated