"""
reporting: a downstream DAG that depends on sales_etl outputs.
Demonstrates cascading failures - the agent should detect this isn't
a code problem but an upstream data problem.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

DATA_DIR = Path("/opt/airflow/data")


def build_daily_report(**context):
    agg_path = DATA_DIR / "sales_agg.csv"
    if not agg_path.exists():
        raise FileNotFoundError(
            f"Cannot find {agg_path}. Upstream sales_etl must run successfully first."
        )
    df = pd.read_csv(agg_path)
    if df.empty:
        raise ValueError("sales_agg.csv is empty - no data to report on")

    top = df.nlargest(5, "total_amount")
    print(f"Top 5 customers:\n{top}")
    out = DATA_DIR / "daily_report.csv"
    top.to_csv(out, index=False)


default_args = {
    "owner": "analytics",
    "retries": 0,
}

with DAG(
    dag_id="reporting",
    default_args=default_args,
    description="Daily report - depends on sales_etl",
    schedule="0 * * * *",  # hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reporting", "downstream"],
) as dag:

    wait_for_sales = ExternalTaskSensor(
        task_id="wait_for_sales",
        external_dag_id="sales_etl",
        external_task_id="aggregate_sales",
        timeout=300,
        mode="reschedule",
        poke_interval=60,
        soft_fail=False,
    )

    build_report = PythonOperator(
        task_id="build_daily_report",
        python_callable=build_daily_report,
    )

    wait_for_sales >> build_report
