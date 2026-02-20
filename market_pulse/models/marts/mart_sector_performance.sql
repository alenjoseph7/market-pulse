-- =============================================================
-- mart_sector_performance.sql
-- Mart model: aggregates stock performance by sector and month
-- Answers: which sector performed best? worst? most volatile?
-- =============================================================

WITH daily_returns AS (
    SELECT * FROM {{ ref('mart_daily_returns') }}
),

monthly_sector AS (
    SELECT
        sector,
        trade_month_start,
        trade_year,
        trade_month,

        -- Number of companies in this sector
        COUNT(DISTINCT ticker)                          AS company_count,

        -- Average monthly return across all stocks in sector
        ROUND(AVG(daily_return_pct), 4)                AS avg_daily_return_pct,

        -- Best performing stock in sector that month
        MAX(daily_return_pct)                          AS max_daily_return_pct,

        -- Worst performing stock in sector that month
        MIN(daily_return_pct)                          AS min_daily_return_pct,

        -- Average volatility (std dev of daily returns)
        ROUND(STDDEV(daily_return_pct), 4)             AS avg_volatility,

        -- Total dollar volume traded in sector
        ROUND(SUM(dollar_volume), 2)                   AS total_dollar_volume,

        -- Average cumulative return for sector
        ROUND(AVG(cumulative_return_pct), 4)           AS avg_cumulative_return_pct

    FROM daily_returns
    WHERE sector IS NOT NULL
    GROUP BY sector, trade_month_start, trade_year, trade_month
),

with_ranking AS (
    SELECT
        *,
        -- Rank sectors by average daily return each month
        RANK() OVER (
            PARTITION BY trade_month_start
            ORDER BY avg_daily_return_pct DESC
        ) AS sector_rank_by_return,

        -- Rank sectors by volatility each month
        RANK() OVER (
            PARTITION BY trade_month_start
            ORDER BY avg_volatility DESC
        ) AS sector_rank_by_volatility

    FROM monthly_sector
)

SELECT * FROM with_ranking
ORDER BY trade_month_start DESC, sector_rank_by_return