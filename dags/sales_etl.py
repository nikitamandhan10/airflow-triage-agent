"""
sales_etl: simulates a CSV-based sales pipeline.
Designed to fail with schema drift (a column rename in the source).
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(exist_ok=True)


def generate_sales_csv(**context):
    """Simulate an upstream system producing a CSV.
    25% of the time, the schema 'drifts' (column renamed)."""
    rows = []
    drift = random.random() < 0.25
    for i in range(100):
        if drift:
            # The source system 'renamed' total_amount -> amount
            rows.append({
                "order_id": i,
                "customer_id": random.randint(1, 50),
                "amount": round(random.uniform(10, 500), 2),
                "order_date": datetime.utcnow().date().isoformat(),
            })
        else:
            rows.append({
                "order_id": i,
                "customer_id": random.randint(1, 50),
                "total_amount": round(random.uniform(10, 500), 2),
                "order_date": datetime.utcnow().date().isoformat(),
            })

    df = pd.DataFrame(rows)
    out = DATA_DIR / "sales_raw.csv"
    df.to_csv(out, index=False)
    print(f"Generated {len(df)} rows. Schema: {list(df.columns)} (drift={drift})")


def validate_sales(**context):
    """Validate expected schema. Fails on drift."""
    df = pd.read_csv(DATA_DIR / "sales_raw.csv")
    expected = {"order_id", "customer_id", "total_amount", "order_date"}
    actual = set(df.columns)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        raise ValueError(
            f"Schema validation failed for sales_raw.csv. "
            f"Missing columns: {missing}. Unexpected columns: {extra}. "
            f"Expected: {expected}. Got: {actual}."
        )
    print(f"Schema OK: {actual}")


def aggregate_sales(**context):
    df = pd.read_csv(DATA_DIR / "sales_raw.csv")
    agg = df.groupby("customer_id")["total_amount"].sum().reset_index()
    agg.to_csv(DATA_DIR / "sales_agg.csv", index=False)
    print(f"Aggregated to {len(agg)} customers")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="sales_etl",
    default_args=default_args,
    description="Sales pipeline that occasionally hits schema drift",
    schedule="*/20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "csv", "flaky"],
) as dag:

    generate = PythonOperator(task_id="generate_sales_csv", python_callable=generate_sales_csv)
    validate = PythonOperator(task_id="validate_sales", python_callable=validate_sales)
    aggregate = PythonOperator(task_id="aggregate_sales", python_callable=aggregate_sales)

    generate >> validate >> aggregate
