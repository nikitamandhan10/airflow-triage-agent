"""
Week 3 milestone: structured triage WITHOUT an LLM.

This module produces a clean JSON report describing a failure.
If you can build this reliably, the LLM part is easy. This is the
80% of the work that's actually data engineering.

Usage:
    python -m agent.triage_dumb --dag weather_etl --run-id manual__2024-01-15T10:00:00
"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from typing import Optional

from agent.airflow_client import AirflowClient


def extract_error_summary(logs: str, max_lines: int = 50) -> dict:
    """Pull the most useful slice of a log: the traceback and final error."""
    lines = logs.splitlines()

    # Find the last 'ERROR' or 'Traceback' marker
    error_start = None
    for i, line in enumerate(lines):
        if "Traceback (most recent call last)" in line or " ERROR " in line:
            error_start = i

    if error_start is None:
        # No clear error marker - return tail of logs
        excerpt = "\n".join(lines[-max_lines:])
        return {"excerpt": excerpt, "error_line": None}

    # Take from the error to end (or max_lines)
    end = min(len(lines), error_start + max_lines)
    excerpt = "\n".join(lines[error_start:end])

    # Try to find the actual exception line (last "ExceptionName: message")
    error_line = None
    for line in reversed(lines[error_start:end]):
        stripped = line.strip()
        if (
            stripped
            and ":" in stripped
            and not stripped.startswith(("File ", "[", "{"))
            and any(
                kw in stripped for kw in ["Error", "Exception", "Failed", "Timeout"]
            )
        ):
            error_line = stripped
            break

    return {"excerpt": excerpt, "error_line": error_line}


def detect_failure_pattern(client: AirflowClient, dag_id: str) -> str:
    """Is this a one-off or recurring failure?"""
    recent = client.get_recent_runs_for_dag(dag_id, n=10)
    states = [r.state for r in recent]
    fails = states.count("failed")
    if fails >= 5:
        return "chronic"  # Pipeline is broken
    if fails >= 2:
        return "recurring"
    return "first_failure"


def triage(
    dag_id: str,
    run_id: str,
    client: Optional[AirflowClient] = None,
) -> dict:
    """Build a structured failure report. No LLM involved."""
    client = client or AirflowClient()

    run = client.get_dag_run(dag_id, run_id)
    if run.state != "failed":
        return {"warning": f"Run state is {run.state}, not failed", "run": asdict(run)}

    all_tasks = client.get_task_instances(dag_id, run_id)
    failed_tasks = [t for t in all_tasks if t.state == "failed"]

    if not failed_tasks:
        return {"warning": "DAG run failed but no failed tasks found", "run": asdict(run)}

    # Focus on the first failed task (root of the failure cascade)
    primary = failed_tasks[0]

    # Get its logs
    try:
        logs = client.get_task_logs(dag_id, run_id, primary.task_id, primary.try_number)
        log_summary = extract_error_summary(logs)
    except Exception as e:
        log_summary = {"excerpt": f"Failed to fetch logs: {e}", "error_line": None}

    # Get upstream task states
    deps = client.get_task_dependencies(dag_id, primary.task_id)
    upstream_states = []
    for upstream_id in deps["upstream"]:
        match = next((t for t in all_tasks if t.task_id == upstream_id), None)
        if match:
            upstream_states.append({"task_id": upstream_id, "state": match.state})

    # Look at history
    pattern = detect_failure_pattern(client, dag_id)

    return {
        "dag_id": dag_id,
        "run_id": run_id,
        "run_state": run.state,
        "failed_task": primary.task_id,
        "try_number": primary.try_number,
        "duration_seconds": primary.duration,
        "error_line": log_summary["error_line"],
        "log_excerpt": log_summary["excerpt"],
        "upstream_states": upstream_states,
        "all_failed_tasks": [t.task_id for t in failed_tasks],
        "failure_pattern": pattern,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    report = triage(args.dag, args.run_id)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
