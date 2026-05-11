"""
Tools that the LLM agent can call during investigation.

Each tool is a small, well-defined function that returns structured data.
The agent decides which tools to call based on what it discovers.

Design principles:
- Read-only by default. No retries, clears, or DAG modifications.
- Each tool returns JSON-serializable data.
- Errors are returned, not raised - the agent should see and reason about them.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from agent.airflow_client import AirflowClient


def _safe(fn):
    """Decorator: catch exceptions and return them as data so the agent sees them."""
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}
    return wrapper


class TriageTools:
    """Bundle of tools bound to a single Airflow client."""

    def __init__(self, client: AirflowClient):
        self.client = client

    @_safe
    def get_dag_run_status(self, dag_id: str, run_id: str) -> dict:
        run = self.client.get_dag_run(dag_id, run_id)
        tasks = self.client.get_task_instances(dag_id, run_id)
        return {
            "run": asdict(run),
            "task_summary": {
                "total": len(tasks),
                "failed": [t.task_id for t in tasks if t.state == "failed"],
                "success": [t.task_id for t in tasks if t.state == "success"],
                "upstream_failed": [t.task_id for t in tasks if t.state == "upstream_failed"],
                "skipped": [t.task_id for t in tasks if t.state == "skipped"],
            },
        }

    @_safe
    def get_task_logs(
        self, dag_id: str, run_id: str, task_id: str, try_number: int = 1, tail_lines: int = 100
    ) -> dict:
        logs = self.client.get_task_logs(dag_id, run_id, task_id, try_number)
        lines = logs.splitlines()
        return {
            "task_id": task_id,
            "total_log_lines": len(lines),
            "tail": "\n".join(lines[-tail_lines:]),
        }

    @_safe
    def get_task_dependencies(self, dag_id: str, task_id: str) -> dict:
        return self.client.get_task_dependencies(dag_id, task_id)

    @_safe
    def get_recent_run_history(self, dag_id: str, n: int = 10) -> dict:
        runs = self.client.get_recent_runs_for_dag(dag_id, n=n)
        return {
            "dag_id": dag_id,
            "recent_runs": [asdict(r) for r in runs],
            "states_summary": {
                state: sum(1 for r in runs if r.state == state)
                for state in {r.state for r in runs}
            },
        }


# ---------- Anthropic tool schemas ----------
# These describe the tools to Claude. The names must match TriageTools methods.

TOOL_SCHEMAS = [
    {
        "name": "get_dag_run_status",
        "description": (
            "Get overall status of a DAG run, including which tasks failed, "
            "succeeded, or were skipped. Start here to understand the failure shape."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string"},
            },
            "required": ["dag_id", "run_id"],
        },
    },
    {
        "name": "get_task_logs",
        "description": (
            "Fetch the tail of logs for a specific task instance. Use this to read "
            "error messages, stack traces, and printed output."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string"},
                "task_id": {"type": "string"},
                "try_number": {"type": "integer", "default": 1},
                "tail_lines": {"type": "integer", "default": 100},
            },
            "required": ["dag_id", "run_id", "task_id"],
        },
    },
    {
        "name": "get_task_dependencies",
        "description": (
            "Get the upstream and downstream task IDs for a task. Use to understand "
            "if a failure could be caused by an upstream task."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "task_id": {"type": "string"},
            },
            "required": ["dag_id", "task_id"],
        },
    },
    {
        "name": "get_recent_run_history",
        "description": (
            "Get the last N runs of a DAG to determine if this failure is a one-off "
            "or part of a pattern. Useful for distinguishing flaky failures from "
            "chronic issues."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "n": {"type": "integer", "default": 10},
            },
            "required": ["dag_id"],
        },
    },
]


def dispatch_tool(tools: TriageTools, name: str, arguments: dict) -> str:
    """Route a tool_use call to the right method, return JSON string."""
    method = getattr(tools, name, None)
    if method is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    result = method(**arguments)
    return json.dumps(result, default=str)
