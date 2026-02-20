"""
Market Pulse — Phase 1: Python Ingestion Script
------------------------------------------------
Pulls OHLCV stock data from Yahoo Finance and uploads to S3.
Run this daily (via Airflow in Phase 4, manually for now).

Install dependencies:
    pip install yfinance boto3 pandas
"""

import io
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf

# =============================================================
# CONFIGURATION — update these values
# =============================================================

S3_BUCKET = "market-pulse-raw-aj"  # your bucket name
S3_STOCK_PREFIX = "stock_prices/"
S3_FUNDAMENTALS_PREFIX = "fundamentals/"
AWS_REGION = "us-east-1"

# 30 tickers across different sectors — easy to explain in interviews
TICKERS = {
    # Technology
    "AAPL", "MSFT", "GOOGL", "META", "NVDA",
    # Finance
    "JPM", "BAC", "GS", "WFC", "V",
    # Healthcare
    "JNJ", "PFE", "UNH", "ABBV", "MRK",
    # Consumer
    "AMZN", "TSLA", "WMT", "HD", "MCD",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG",
    # Industrial
    "CAT", "BA", "GE", "MMM", "HON",
}

# =============================================================
# LOGGING SETUP
# =============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


# =============================================================
# S3 CLIENT
# =============================================================

def get_s3_client():
    """Returns a boto3 S3 client. Uses your AWS credentials from
    ~/.aws/credentials or environment variables."""
    return boto3.client("s3", region_name=AWS_REGION)


def upload_dataframe_to_s3(df: pd.DataFrame, bucket: str, key: str, s3_client) -> bool:
    """Uploads a pandas DataFrame as CSV to S3 without saving to disk."""
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="text/csv"
        )
        log.info(f"Uploaded {len(df)} rows to s3://{bucket}/{key}")
        return True
    except Exception as e:
        log.error(f"Failed to upload {key}: {e}")
        return False


# =============================================================
# STOCK PRICE INGESTION
# =============================================================

def fetch_ohlcv(ticker: str, start_date: str, end_date: str):
    """Fetches daily OHLCV data for a single ticker from Yahoo Finance."""
    try:
        data = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True   # adjusts for splits and dividends
        )
        if data.empty:
            log.warning(f"No data returned for {ticker}")
            return None

        data = data.reset_index()
        data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]
        data["ticker"] = ticker
        data = data.rename(columns={
            "Date":   "trade_date",
            "Open":   "open_price",
            "High":   "high_price",
            "Low":    "low_price",
            "Close":  "close_price",
            "Volume": "volume"
        })

        return data[["ticker", "trade_date", "open_price", "high_price",
                     "low_price", "close_price", "volume"]]

    except Exception as e:
        log.error(f"Error fetching {ticker}: {e}")
        return None


def ingest_stock_prices(s3_client, days_back: int = 1):
    """
    Fetches OHLCV for all tickers and uploads one CSV per ticker to S3.
    By default fetches yesterday's data (days_back=1).
    For backfill, set days_back to a larger number (e.g. 365 for 1 year).
    """
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    log.info(f"Fetching stock prices from {start_date} to {end_date}")

    success_count = 0
    for ticker in sorted(TICKERS):
        df = fetch_ohlcv(ticker, start_date, end_date)
        if df is None or df.empty:
            continue

        # One file per ticker per run — Snowpipe will pick up each file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3_key = f"{S3_STOCK_PREFIX}{ticker}_{timestamp}.csv"

        if upload_dataframe_to_s3(df, S3_BUCKET, s3_key, s3_client):
            success_count += 1

    log.info(f"Stock price ingestion complete: {success_count}/{len(TICKERS)} tickers uploaded")


# =============================================================
# FUNDAMENTALS INGESTION
# =============================================================

def fetch_fundamentals(ticker: str):
    """
    Fetches company fundamentals from Yahoo Finance.
    This is static reference data — run once or weekly.
    """
    try:
        info = yf.Ticker(ticker).info
        return {
            "ticker":             ticker,
            "company_name":       info.get("longName", ""),
            "sector":             info.get("sector", ""),
            "industry":           info.get("industry", ""),
            "market_cap":         info.get("marketCap", None),
            "pe_ratio":           info.get("trailingPE", None),
            "fifty_two_week_high": info.get("fiftyTwoWeekHigh", None),
            "fifty_two_week_low":  info.get("fiftyTwoWeekLow", None),
        }
    except Exception as e:
        log.error(f"Error fetching fundamentals for {ticker}: {e}")
        return None


def ingest_fundamentals(s3_client):
    """
    Fetches fundamentals for all tickers and uploads as a single CSV.
    Load this into Snowflake via COPY INTO (not Snowpipe) since it's static.
    """
    log.info("Fetching company fundamentals...")
    rows = []
    for ticker in sorted(TICKERS):
        data = fetch_fundamentals(ticker)
        if data:
            rows.append(data)
            log.info(f"  ✓ {ticker} — {data.get('company_name', 'N/A')}")

    if not rows:
        log.error("No fundamentals data fetched")
        return

    df = pd.DataFrame(rows)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    s3_key = f"{S3_FUNDAMENTALS_PREFIX}fundamentals_{timestamp}.csv"
    df.columns = ["ticker","company_name","sector","industry",
               "market_cap","pe_ratio","fifty_two_week_high","fifty_two_week_low"]
    upload_dataframe_to_s3(df, S3_BUCKET, s3_key, s3_client)
    log.info(f"Fundamentals uploaded: {len(df)} companies")


# =============================================================
# MAIN — Run ingestion
# =============================================================

if __name__ == "__main__":
    s3 = get_s3_client()

    # Daily run — active
    ingest_stock_prices(s3, days_back=1)

    # Backfill — done, keep commented
    # ingest_stock_prices(s3, days_back=365)

    # Fundamentals — done, keep commented
    # ingest_fundamentals(s3)