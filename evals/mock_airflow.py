"""
Mock AirflowClient for offline evals — no live Airflow instance needed.

Three canned scenarios:
  rate_limit       — weather_etl fetch task hits HTTP 429
  schema_drift     — sales_etl load task hits KeyError on renamed column
  upstream_failure — reporting DAG fails because sales_etl failed first
"""

from __future__ import annotations
from datetime import datetime, timezone
from agent.airflow_client import DagRun, TaskInstance

_NOW = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc).isoformat()

_LOGS = {
    "fetch_weather": """\
[2024-01-15 10:00:00,123] INFO - Executing task: fetch_weather
[2024-01-15 10:00:05,441] INFO - GET https://api.weather.example.com/v1/current
[2024-01-15 10:00:06,002] ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/weather_etl.py", line 45, in fetch_weather
    response.raise_for_status()
  File "/usr/local/lib/python3.10/site-packages/requests/models.py", line 943, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 429 Client Error: Too Many Requests for url: https://api.weather.example.com/v1/current
Retry-After: 3600
""",
    "load_csv": """\
[2024-01-15 10:20:00,010] INFO - Executing task: load_csv
[2024-01-15 10:20:05,300] INFO - Reading /data/sales_raw.csv — 1523 rows loaded
[2024-01-15 10:20:05,900] ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/sales_etl.py", line 67, in load_csv
    total = row['revenue']
KeyError: 'revenue'
""",
    "build_report": """\
[2024-01-15 11:00:00,000] INFO - Executing task: build_report
[2024-01-15 11:00:00,120] INFO - Loading sales summary from sales_etl output
[2024-01-15 11:00:00,300] ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/reporting.py", line 33, in build_report
    data = load_sales_summary()
  File "/opt/airflow/dags/reporting.py", line 18, in load_sales_summary
    return json.load(f)
FileNotFoundError: [Errno 2] No such file or directory: '/data/sales_summary.json'
""",
}

_DEPS = {
    "fetch_weather":     {"upstream": [],                "downstream": ["transform_weather"]},
    "transform_weather": {"upstream": ["fetch_weather"], "downstream": ["load_weather"]},
    "load_weather":      {"upstream": ["transform_weather"], "downstream": []},
    "extract_csv":       {"upstream": [],                "downstream": ["load_csv"]},
    "load_csv":          {"upstream": ["extract_csv"],   "downstream": ["validate"]},
    "validate":          {"upstream": ["load_csv"],      "downstream": []},
    "build_report":      {"upstream": [],                "downstream": ["send_report"]},
    "send_report":       {"upstream": ["build_report"],  "downstream": []},
}


class MockAirflowClient:
    """Canned responses for three failure scenarios."""

    def __init__(self, scenario: str):
        assert scenario in ("rate_limit", "schema_drift", "upstream_failure"), \
            f"Unknown scenario: {scenario}"
        self.scenario = scenario

    def get_dag_run(self, dag_id: str, run_id: str) -> DagRun:
        return DagRun(dag_id=dag_id, run_id=run_id, state="failed",
                      start_date=_NOW, end_date=_NOW, execution_date=_NOW)

    def get_task_instances(self, dag_id: str, run_id: str) -> list[TaskInstance]:
        def ti(task_id, state, duration=None):
            return TaskInstance(dag_id, run_id, task_id, state, 1, _NOW, _NOW, duration)

        if self.scenario == "rate_limit":
            return [
                ti("fetch_weather", "failed", 1.2),
                ti("transform_weather", "upstream_failed"),
                ti("load_weather", "upstream_failed"),
            ]
        if self.scenario == "schema_drift":
            return [
                ti("extract_csv", "success", 2.1),
                ti("load_csv", "failed", 0.3),
                ti("validate", "upstream_failed"),
            ]
        # upstream_failure
        return [
            ti("build_report", "failed", 0.2),
            ti("send_report", "upstream_failed"),
        ]

    def get_task_logs(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        return _LOGS.get(task_id, "[2024-01-15 10:00:00] INFO - No log captured for this task.")

    def get_task_dependencies(self, dag_id: str, task_id: str) -> dict:
        return _DEPS.get(task_id, {"upstream": [], "downstream": []})

    def get_recent_runs_for_dag(self, dag_id: str, n: int = 10) -> list[DagRun]:
        def run(i, state):
            return DagRun(dag_id, f"run_{i}", state, _NOW, _NOW, _NOW)

        if self.scenario == "rate_limit":
            # One prior failure → recent_failure_count = 1 → auto-fix eligible
            return [run(i, "failed" if i == 1 else "success") for i in range(n)]
        if self.scenario == "schema_drift":
            # Two prior failures → recurring
            return [run(i, "failed" if i < 2 else "success") for i in range(n)]
        # upstream_failure: DAG itself was healthy until now
        return [run(i, "success") for i in range(n)]

    def get_recent_failed_runs(self, hours: int = 24, limit: int = 20) -> list[DagRun]:
        if self.scenario == "upstream_failure":
            # sales_etl failed ~30 min ago — explains the reporting failure
            return [
                DagRun("sales_etl", "sales_failed_001", "failed", _NOW, _NOW, _NOW),
            ]
        return []

    def clear_task_instance(self, dag_id: str, run_id: str, task_id: str) -> dict:
        return {"task_instances": [{"task_id": task_id, "state": "queued"}]}

    def trigger_dag_run(self, dag_id: str) -> dict:
        return {"new_run_id": f"manual__{_NOW}", "state": "queued"}

    def get_xcom_value(self, dag_id: str, run_id: str, task_id: str, key: str = "return_value") -> dict:
        return {"key": key, "value": None}

    def get_task_instance_state(self, dag_id: str, run_id: str, task_id: str) -> str:
        return "success"
