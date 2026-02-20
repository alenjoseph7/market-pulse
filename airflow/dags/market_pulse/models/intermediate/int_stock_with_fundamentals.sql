-- =============================================================
-- int_stock_with_fundamentals.sql
-- Intermediate model: joins stock prices with company details
-- This enriches every price row with sector, name, market cap
-- =============================================================

WITH stock_prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

fundamentals AS (
    SELECT * FROM {{ ref('stg_company_fundamentals') }}
),

joined AS (
    SELECT
        -- Price data
        sp.ticker,
        sp.trade_date,
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.volume,
        sp.trade_year,
        sp.trade_month,
        sp.trade_month_start,
        sp.day_of_week,

        -- Company data
        f.company_name,
        f.sector,
        f.industry,
        f.market_cap,
        f.market_cap_category,
        f.pe_ratio,
        f.fifty_two_week_high,
        f.fifty_two_week_low,

        -- Derived: is current price near 52 week high?
        ROUND(
            (sp.close_price / NULLIF(f.fifty_two_week_high, 0)) * 100, 2
        ) AS pct_of_52w_high

    FROM stock_prices sp
    LEFT JOIN fundamentals f ON sp.ticker = f.ticker
)

SELECT * FROM joined