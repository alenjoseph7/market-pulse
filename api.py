"""
Market Pulse — FastAPI
----------------------
REST API serving stock analytics from Snowflake.

Run:
    uvicorn api:app --reload --port 8000

Endpoints:
    GET /health
    GET /tickers
    GET /ticker/{ticker}
    GET /sector/{sector}
    GET /top-gainers
    GET /top-losers
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import snowflake.connector
import os
from datetime import datetime
from functools import lru_cache

# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────
app = FastAPI(
    title="Market Pulse API",
    description="Stock analytics API powered by Snowflake + dbt",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# SNOWFLAKE CONNECTION
# ─────────────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account="EBSQBLV-NJ71987",
        user="ALENJOSEPH7",
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse="REPORTER_WH",
        database="ANALYTICS",
        schema="CORE",
        role="REPORTER"
    )

def run_query(sql: str) -> list[dict]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()

# ─────────────────────────────────────────────
# RESPONSE MODELS
# ─────────────────────────────────────────────
class TickerResponse(BaseModel):
    ticker: str
    company_name: Optional[str]
    sector: str
    trade_date: str
    close_price: float
    daily_return_pct: float
    cumulative_return_pct: float
    ma_7_day: Optional[float]
    ma_30_day: Optional[float]
    ma_90_day: Optional[float]
    volatility_30_day: Optional[float]
    trend_signal: Optional[str]
    pct_above_ma_30: Optional[float]

class SectorResponse(BaseModel):
    sector: str
    avg_daily_return_pct: float
    avg_volatility: Optional[float]
    company_count: int
    sector_rank_by_return: int
    trade_month_start: str

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str

# ─────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health_check():
    """Check API health and Snowflake connectivity."""
    try:
        run_query("SELECT 1")
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0"
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Snowflake unavailable: {str(e)}")


@app.get("/tickers", tags=["Stocks"])
def get_tickers():
    """Get list of all available tickers with their sectors."""
    rows = run_query("""
        SELECT DISTINCT TICKER, SECTOR
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS
        ORDER BY SECTOR, TICKER
    """)
    return {"tickers": rows, "count": len(rows)}


@app.get("/ticker/{ticker}", response_model=TickerResponse, tags=["Stocks"])
def get_ticker(ticker: str):
    """Get latest analytics for a specific ticker."""
    ticker = ticker.upper()
    rows = run_query(f"""
        SELECT
            r.TICKER,
            m.COMPANY_NAME,
            r.SECTOR,
            r.TRADE_DATE::VARCHAR AS TRADE_DATE,
            r.CLOSE_PRICE,
            r.DAILY_RETURN_PCT,
            r.CUMULATIVE_RETURN_PCT,
            m.MA_7_DAY,
            m.MA_30_DAY,
            m.MA_90_DAY,
            m.VOLATILITY_30_DAY,
            m.TREND_SIGNAL,
            m.PCT_ABOVE_MA_30
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS r
        LEFT JOIN ANALYTICS.CORE.MART_MOVING_AVERAGES m
            ON r.TICKER = m.TICKER AND r.TRADE_DATE = m.TRADE_DATE
        WHERE r.TICKER = '{ticker}'
        ORDER BY r.TRADE_DATE DESC
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")
    return rows[0]


@app.get("/ticker/{ticker}/history", tags=["Stocks"])
def get_ticker_history(ticker: str, days: int = 30):
    """Get price history for a ticker."""
    ticker = ticker.upper()
    if days > 365:
        raise HTTPException(status_code=400, detail="Max 365 days")
    rows = run_query(f"""
        SELECT
            r.TRADE_DATE::VARCHAR AS TRADE_DATE,
            r.CLOSE_PRICE,
            r.DAILY_RETURN_PCT,
            r.CUMULATIVE_RETURN_PCT,
            m.MA_7_DAY,
            m.MA_30_DAY,
            m.MA_90_DAY,
            m.VOLATILITY_30_DAY,
            m.TREND_SIGNAL
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS r
        LEFT JOIN ANALYTICS.CORE.MART_MOVING_AVERAGES m
            ON r.TICKER = m.TICKER AND r.TRADE_DATE = m.TRADE_DATE
        WHERE r.TICKER = '{ticker}'
        ORDER BY r.TRADE_DATE DESC
        LIMIT {days}
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Ticker '{ticker}' not found")
    return {"ticker": ticker, "days": days, "data": rows}


@app.get("/sector/{sector}", tags=["Sectors"])
def get_sector(sector: str):
    """Get latest performance for a sector."""
    rows = run_query(f"""
        SELECT
            SECTOR,
            TRADE_MONTH_START::VARCHAR AS TRADE_MONTH_START,
            AVG_DAILY_RETURN_PCT,
            AVG_VOLATILITY,
            COMPANY_COUNT,
            SECTOR_RANK_BY_RETURN
        FROM ANALYTICS.CORE.MART_SECTOR_PERFORMANCE
        WHERE UPPER(SECTOR) = UPPER('{sector}')
        ORDER BY TRADE_MONTH_START DESC
        LIMIT 1
    """)
    if not rows:
        raise HTTPException(status_code=404, detail=f"Sector '{sector}' not found")
    return rows[0]


@app.get("/sectors", tags=["Sectors"])
def get_sectors():
    """Get latest performance for all sectors."""
    rows = run_query("""
        SELECT
            SECTOR,
            TRADE_MONTH_START::VARCHAR AS TRADE_MONTH_START,
            AVG_DAILY_RETURN_PCT,
            AVG_VOLATILITY,
            COMPANY_COUNT,
            SECTOR_RANK_BY_RETURN
        FROM ANALYTICS.CORE.MART_SECTOR_PERFORMANCE
        WHERE TRADE_MONTH_START = (
            SELECT MAX(TRADE_MONTH_START)
            FROM ANALYTICS.CORE.MART_SECTOR_PERFORMANCE
        )
        ORDER BY SECTOR_RANK_BY_RETURN
    """)
    return {"sectors": rows, "count": len(rows)}


@app.get("/top-gainers", tags=["Market"])
def get_top_gainers(limit: int = 5):
    """Get top gaining stocks for the latest trading day."""
    rows = run_query(f"""
        SELECT TICKER, SECTOR, CLOSE_PRICE, DAILY_RETURN_PCT, CUMULATIVE_RETURN_PCT
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS
        WHERE TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.CORE.MART_DAILY_RETURNS)
        ORDER BY DAILY_RETURN_PCT DESC
        LIMIT {min(limit, 20)}
    """)
    return {"top_gainers": rows, "count": len(rows)}


@app.get("/top-losers", tags=["Market"])
def get_top_losers(limit: int = 5):
    """Get top losing stocks for the latest trading day."""
    rows = run_query(f"""
        SELECT TICKER, SECTOR, CLOSE_PRICE, DAILY_RETURN_PCT, CUMULATIVE_RETURN_PCT
        FROM ANALYTICS.CORE.MART_DAILY_RETURNS
        WHERE TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.CORE.MART_DAILY_RETURNS)
        ORDER BY DAILY_RETURN_PCT ASC
        LIMIT {min(limit, 20)}
    """)
    return {"top_losers": rows, "count": len(rows)}


@app.get("/most-volatile", tags=["Market"])
def get_most_volatile(limit: int = 5):
    """Get most volatile stocks based on 30-day volatility."""
    rows = run_query(f"""
        SELECT TICKER, SECTOR, CLOSE_PRICE, VOLATILITY_30_DAY, TREND_SIGNAL
        FROM ANALYTICS.CORE.MART_MOVING_AVERAGES
        WHERE TRADE_DATE = (SELECT MAX(TRADE_DATE) FROM ANALYTICS.CORE.MART_MOVING_AVERAGES)
        ORDER BY VOLATILITY_30_DAY DESC
        LIMIT {min(limit, 20)}
    """)
    return {"most_volatile": rows, "count": len(rows)}