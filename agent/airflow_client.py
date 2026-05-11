"""
Airflow REST API client.
This is the 'senses' of the agent - everything it learns about Airflow
goes through here. Keep it boring and well-tested.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from requests.auth import HTTPBasicAuth


@dataclass
class TaskInstance:
    dag_id: str
    run_id: str
    task_id: str
    state: Optional[str]
    try_number: int
    start_date: Optional[str]
    end_date: Optional[str]
    duration: Optional[float]


@dataclass
class DagRun:
    dag_id: str
    run_id: str
    state: str
    start_date: Optional[str]
    end_date: Optional[str]
    execution_date: Optional[str]


class AirflowClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.base_url = (base_url or os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")).rstrip("/")
        self.auth = HTTPBasicAuth(
            username or os.getenv("AIRFLOW_USERNAME", "airflow"),
            password or os.getenv("AIRFLOW_PASSWORD", "airflow"),
        )
        self.api = f"{self.base_url}/api/v1"

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        r = requests.get(f"{self.api}{path}", auth=self.auth, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # ---------- DAG Runs ----------

    def list_dags(self) -> list[dict]:
        return self._get("/dags", params={"limit": 100}).get("dags", [])

    def get_recent_failed_runs(self, hours: int = 24, limit: int = 20) -> list[DagRun]:
        """Find recent failed DAG runs across all DAGs."""
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        # Airflow's batch endpoint for dag runs
        body = {
            "states": ["failed"],
            "start_date_gte": since,
            "page_limit": limit,
        }
        r = requests.post(
            f"{self.api}/dags/~/dagRuns/list",
            auth=self.auth,
            json=body,
            timeout=30,
        )
        r.raise_for_status()
        return [
            DagRun(
                dag_id=d["dag_id"],
                run_id=d["dag_run_id"],
                state=d["state"],
                start_date=d.get("start_date"),
                end_date=d.get("end_date"),
                execution_date=d.get("execution_date"),
            )
            for d in r.json().get("dag_runs", [])
        ]

    def get_dag_run(self, dag_id: str, run_id: str) -> DagRun:
        d = self._get(f"/dags/{dag_id}/dagRuns/{run_id}")
        return DagRun(
            dag_id=d["dag_id"],
            run_id=d["dag_run_id"],
            state=d["state"],
            start_date=d.get("start_date"),
            end_date=d.get("end_date"),
            execution_date=d.get("execution_date"),
        )

    def get_recent_runs_for_dag(self, dag_id: str, n: int = 10) -> list[DagRun]:
        data = self._get(
            f"/dags/{dag_id}/dagRuns",
            params={"order_by": "-execution_date", "limit": n},
        )
        return [
            DagRun(
                dag_id=d["dag_id"],
                run_id=d["dag_run_id"],
                state=d["state"],
                start_date=d.get("start_date"),
                end_date=d.get("end_date"),
                execution_date=d.get("execution_date"),
            )
            for d in data.get("dag_runs", [])
        ]

    # ---------- Task Instances ----------

    def get_task_instances(self, dag_id: str, run_id: str) -> list[TaskInstance]:
        data = self._get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        return [
            TaskInstance(
                dag_id=t["dag_id"],
                run_id=t["dag_run_id"],
                task_id=t["task_id"],
                state=t.get("state"),
                try_number=t.get("try_number", 1),
                start_date=t.get("start_date"),
                end_date=t.get("end_date"),
                duration=t.get("duration"),
            )
            for t in data.get("task_instances", [])
        ]

    def get_failed_tasks(self, dag_id: str, run_id: str) -> list[TaskInstance]:
        return [t for t in self.get_task_instances(dag_id, run_id) if t.state == "failed"]

    def get_task_logs(
        self, dag_id: str, run_id: str, task_id: str, try_number: int = 1
    ) -> str:
        """Fetch raw log content for a task try."""
        url = (
            f"{self.api}/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{task_id}/logs/{try_number}"
        )
        r = requests.get(
            url,
            auth=self.auth,
            headers={"Accept": "text/plain"},
            params={"full_content": "true"},
            timeout=30,
        )
        r.raise_for_status()
        return r.text

    # ---------- DAG Structure ----------

    def get_task_dependencies(self, dag_id: str, task_id: str) -> dict[str, list[str]]:
        """Get upstream and downstream task IDs."""
        data = self._get(f"/dags/{dag_id}/tasks/{task_id}")
        return {
            "upstream": data.get("upstream_task_ids", []),
            "downstream": data.get("downstream_task_ids", []),
        }


if __name__ == "__main__":
    # Quick smoke test: python -m agent.airflow_client
    client = AirflowClient()
    print("DAGs:", [d["dag_id"] for d in client.list_dags()])
    print("Recent failures:", client.get_recent_failed_runs(hours=72))
