"""
weather_etl: pulls weather data from a public API.
Designed to occasionally fail with rate limits and timeouts.
"""
from __future__ import annotations

import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")
DATA_DIR.mkdir(exist_ok=True)

CITIES = ["London", "Tokyo", "New York", "Sydney", "Mumbai"]


def fetch_weather(**context):
    """Fetch weather. Occasionally simulates rate-limit failures."""
    # 30% chance of simulated rate limit (for the agent to find)
    if random.random() < 0.3:
        raise requests.exceptions.HTTPError(
            "429 Client Error: Too Many Requests for url: api.weather.example/v1/current. "
            "Rate limit exceeded: 60 requests/minute. Retry after 45s."
        )

    # 15% chance of timeout
    if random.random() < 0.15:
        raise requests.exceptions.Timeout(
            "HTTPSConnectionPool(host='api.weather.example', port=443): "
            "Read timed out. (read timeout=10)"
        )

    # Use a real free API for the happy path
    results = []
    for city in CITIES:
        # wttr.in is a free weather API
        try:
            r = requests.get(f"https://wttr.in/{city}?format=j1", timeout=10)
            r.raise_for_status()
            results.append({"city": city, "data": r.json()})
        except Exception:
            results.append({"city": city, "data": None})

    out = DATA_DIR / "weather_raw.json"
    out.write_text(json.dumps(results))
    print(f"Wrote {len(results)} cities to {out}")
    return str(out)


def transform_weather(**context):
    """Transform raw weather into a flat structure."""
    raw_path = Path("/opt/airflow/data/weather_raw.json")
    if not raw_path.exists():
        raise FileNotFoundError(
            f"Expected upstream output at {raw_path} but it doesn't exist. "
            "Did the fetch task fail?"
        )

    raw = json.loads(raw_path.read_text())
    flat = []
    for entry in raw:
        if entry["data"] is None:
            continue
        try:
            current = entry["data"]["current_condition"][0]
            flat.append({
                "city": entry["city"],
                "temp_c": current["temp_C"],
                "humidity": current["humidity"],
                "observed_at": datetime.utcnow().isoformat(),
            })
        except (KeyError, IndexError) as e:
            raise KeyError(
                f"Schema mismatch for {entry['city']}: expected current_condition[0].temp_C. "
                f"Got keys: {list(entry['data'].keys()) if entry['data'] else 'None'}. "
                f"Original error: {e}"
            )

    out = DATA_DIR / "weather_clean.json"
    out.write_text(json.dumps(flat))
    print(f"Transformed {len(flat)} records")


def load_weather(**context):
    """Pretend to load to a warehouse."""
    clean_path = Path("/opt/airflow/data/weather_clean.json")
    if not clean_path.exists():
        raise FileNotFoundError(f"No clean data at {clean_path}")
    data = json.loads(clean_path.read_text())
    print(f"Loaded {len(data)} rows to warehouse (simulated)")


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Pulls weather data; sometimes fails on rate limits or timeouts",
    schedule="*/15 * * * *",  # every 15 min
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "api", "flaky"],
) as dag:

    fetch = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather)
    transform = PythonOperator(task_id="transform_weather", python_callable=transform_weather)
    load = PythonOperator(task_id="load_weather", python_callable=load_weather)

    fetch >> transform >> load
