
Market Pulse 📈
End-to-End Stock Analytics Platform
A production-grade data engineering platform that automatically ingests daily stock market data for 30 tickers, transforms it through a multi-layer pipeline, and serves analytics through an interactive dashboard and REST API.

Architecture
Yahoo Finance API → AWS S3 → Snowpipe → Snowflake RAW → dbt (Staging → Intermediate → Marts) → Streamlit Dashboard + FastAPI REST API

Tech Stack



|Layer        |Technology                           |
|-------------|-------------------------------------|
|Ingestion    |Python, Yahoo Finance API            |
|Storage      |AWS S3                               |
|Auto-load    |Snowpipe + SQS                       |
|Warehouse    |Snowflake (Medallion Architecture)   |
|Transform    |dbt (6 models, 20 data quality tests)|
|Orchestration|Apache Airflow + Docker              |
|Dashboard    |Streamlit + Plotly                   |
|API          |FastAPI (9 endpoints)                |
|CI/CD        |GitHub Actions                       |

Key Features
	•	Fully automated pipeline — zero manual intervention
	•	30 stock tickers across multiple sectors
	•	7,500+ rows of daily data ingested
	•	20 dbt data quality tests enforcing schema integrity
	•	Production-grade RBAC security (3 roles, least privilege)
	•	CI/CD pipeline completing in under 60 seconds
	•	Bloomberg Terminal-themed dashboard with 6 pages
	•	REST API with 9 endpoints + auto-generated Swagger docs

Pipeline Flow
	1.	Ingestion — Python fetches OHLCV data from Yahoo Finance, uploads to S3
	2.	Auto-loading — S3 event triggers SQS, Snowpipe loads into Snowflake RAW
	3.	Transformation — dbt transforms across 3 layers (Staging → Intermediate → Marts)
	4.	Orchestration — Airflow DAG runs Mon-Fri at 10PM UTC with failure alerting
	5.	Consumption — Streamlit dashboard + FastAPI serve analytics

Snowflake Architecture



|Database |Contents                                                         |
|---------|-----------------------------------------------------------------|
|RAW      |Daily OHLCV stock prices as ingested                             |
|STAGING  |Cleaned, deduplicated, typed data                                |
|ANALYTICS|mart_daily_returns, mart_moving_averages, mart_sector_performance|

Dashboard Pages
	•	Overview — Top gainers, losers, most volatile, cumulative returns
	•	Price Charts — Price + MA7/30/90 per ticker
	•	Sector Performance — Monthly sector rankings + heatmap
	•	Daily Returns — Return distribution + ticker heatmap
	•	Volatility Rankings — Risk vs return scatter plot
	•	Stock Info — Full profile card per ticker

API Endpoints
GET /health — API + Snowflake connectivity check
GET /tickers — List all 30 tickers
GET /ticker/{ticker} — Latest price, return, signal
GET /ticker/{ticker}/history — Price history
GET /sectors — All sector rankings
GET /sector/{sector} — Specific sector performance
GET /top-gainers — Top gaining stocks
GET /top-losers — Top losing stocks
GET /most-volatile — Most volatile stocks

Key Achievements
	•	Fully automated end-to-end pipeline with zero manual intervention
	•	7,500+ rows of daily stock data ingested across 30 tickers
	•	20 data quality tests enforcing schema integrity
	•	Production-grade RBAC — least privilege access per layer
	•	CI/CD pipeline passing in under 60 seconds on every push
	•	Bloomberg Terminal-themed dashboard with 6 interactive pages
	•	REST API with 9 endpoints and auto-generated Swagger documentation
	•	Docker-based Airflow setup reproducible on any machine

Author
Alen Joseph James
MS Computer Science, Mercy University
alenjosephjames00@gmail.com | github.com/alenjoseph7​​​​​​​​​​​​​​​​
