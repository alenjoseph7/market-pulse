"""
Market Pulse — Airflow DAG (Simplified)
----------------------------------------
Orchestrates the dbt transformation pipeline daily.
Ingestion is handled separately by phase1_ingestion.py
running on the host machine or a Lambda function.

Pipeline:
1. Verify new data landed in Snowflake (from Snowpipe)
2. Run dbt transformations
3. Run dbt tests
4. Alert on failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "market_pulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="market_pulse_daily_pipeline",
    description="Daily Market Pulse dbt transformation pipeline",
    default_args=default_args,
    start_date=datetime(2026, 2, 19),
    schedule_interval="0 22 * * 1-5",
    catchup=False,
    tags=["market_pulse", "stocks", "snowflake", "dbt"],
) as dag:

    verify_task = SnowflakeOperator(
        task_id="verify_raw_data",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT COUNT(*) AS new_rows
            FROM RAW.STOCK_PRICES.DAILY_OHLCV
            WHERE LOADED_AT >= DATEADD(HOUR, -6, CURRENT_TIMESTAMP);
        """,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dags/market_pulse && \
            dbt run \
                --profiles-dir /opt/airflow/dags/market_pulse \
                --target prod
        """,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/dags/market_pulse && \
            dbt test \
                --profiles-dir /opt/airflow/dags/market_pulse \
                --target prod
        """,
    )

    success_task = BashOperator(
        task_id="pipeline_success",
        bash_command='echo "Market Pulse pipeline completed successfully on $(date)"',
    )

    failure_task = BashOperator(
        task_id="pipeline_failure_alert",
        bash_command='echo "ALERT: Market Pulse pipeline FAILED on $(date)"',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    verify_task >> dbt_run_task >> dbt_test_task >> success_task
    dbt_test_task >> failure_task