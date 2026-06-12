"""
Tools that the LLM agent can call during investigation and remediation.

TWO CATEGORIES:
  1. Read tools  - safe, no side effects, always available
  2. Write tools - modify Airflow state, gated by guardrails

GUARDRAIL RULES (enforced in code, not just in the prompt):
  - rate_limit      -> safe to auto-retry (transient)
  - infra_timeout   -> safe to auto-retry once (transient)
  - upstream_failure-> retry only if upstream is now healthy
  - schema_drift    -> NEVER auto-fix (needs code change)
  - data_quality    -> NEVER auto-fix (needs data investigation)
  - code_bug        -> NEVER auto-fix (needs code change)
  - unknown         -> NEVER auto-fix
  - recurring (3+)  -> NEVER auto-fix regardless of cause
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from agent.airflow_client import AirflowClient

logger = logging.getLogger(__name__)

ACTION_LOG_PATH = Path("action_log.jsonl")

AUTO_FIX_ALLOWED = {"rate_limit", "infra_timeout"}
NEVER_AUTO_FIX = {"schema_drift", "data_quality", "code_bug", "unknown"}
MAX_AUTO_FIX_FAILURES = 2


def _log_action(action: str, details: dict):
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": action,
        **details,
    }
    with open(ACTION_LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")
    logger.info(f"ACTION: {action} | {details}")


def check_guardrails(root_cause: str, failure_count: int, confidence: str) -> dict:
    if root_cause in NEVER_AUTO_FIX:
        return {
            "allowed": False,
            "reason": f"Root cause '{root_cause}' always requires human intervention.",
        }
    if confidence != "high":
        return {
            "allowed": False,
            "reason": f"Confidence is '{confidence}'. Auto-fix only runs when confidence is 'high'.",
        }
    if failure_count > MAX_AUTO_FIX_FAILURES:
        return {
            "allowed": False,
            "reason": f"DAG has failed {failure_count} times recently. Recurring failures require human review.",
        }
    if root_cause in AUTO_FIX_ALLOWED:
        return {
            "allowed": True,
            "reason": f"Root cause '{root_cause}' is transient and safe to auto-retry.",
        }
    return {
        "allowed": False,
        "reason": f"Root cause '{root_cause}' is not in the auto-fix allowlist.",
    }


def _safe(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}
    return wrapper


class TriageTools:

    def __init__(self, client: AirflowClient):
        self.client = client

    # ── READ TOOLS ──────────────────────────────────────────────────────────

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
    def get_task_logs(self, dag_id: str, run_id: str, task_id: str, try_number: int = 1, tail_lines: int = 150) -> dict:
        logs = self.client.get_task_logs(dag_id, run_id, task_id, try_number)
        lines = logs.splitlines()
        return {
            "task_id": task_id,
            "try_number": try_number,
            "total_log_lines": len(lines),
            "tail": "\n".join(lines[-tail_lines:]),
        }

    @_safe
    def get_task_dependencies(self, dag_id: str, task_id: str) -> dict:
        return self.client.get_task_dependencies(dag_id, task_id)

    @_safe
    def get_recent_run_history(self, dag_id: str, n: int = 10) -> dict:
        runs = self.client.get_recent_runs_for_dag(dag_id, n=n)
        recent_failures = sum(1 for r in runs if r.state == "failed")
        return {
            "dag_id": dag_id,
            "recent_runs": [asdict(r) for r in runs],
            "states_summary": {
                state: sum(1 for r in runs if r.state == state)
                for state in {r.state for r in runs}
            },
            "recent_failure_count": recent_failures,
        }

    @_safe
    def get_xcom_value(self, dag_id: str, run_id: str, task_id: str, key: str = "return_value") -> dict:
        return self.client.get_xcom_value(dag_id, run_id, task_id, key)

    @_safe
    def get_cross_dag_failures(self, current_dag_id: str, hours: int = 2) -> dict:
        runs = self.client.get_recent_failed_runs(hours=hours, limit=20)
        other = [asdict(r) for r in runs if r.dag_id != current_dag_id]
        return {
            "other_failed_dags": other,
            "count": len(other),
            "hint": "If any of these DAGs feed data into the current DAG, classify as upstream_failure.",
        }

    # ── WRITE TOOLS ─────────────────────────────────────────────────────────

    @_safe
    def check_fix_eligibility(self, dag_id: str, root_cause: str, confidence: str, recent_failure_count: int) -> dict:
        result = check_guardrails(root_cause, recent_failure_count, confidence)
        _log_action("check_fix_eligibility", {
            "dag_id": dag_id,
            "root_cause": root_cause,
            "confidence": confidence,
            "recent_failure_count": recent_failure_count,
            "allowed": result["allowed"],
        })
        return result

    @_safe
    def retry_failed_task(self, dag_id: str, run_id: str, task_id: str, root_cause: str, confidence: str, recent_failure_count: int) -> dict:
        guard = check_guardrails(root_cause, recent_failure_count, confidence)
        if not guard["allowed"]:
            _log_action("retry_blocked", {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "reason": guard["reason"]})
            return {"retried": False, "blocked": True, "reason": guard["reason"]}
        result = self.client.clear_task_instance(dag_id, run_id, task_id)
        _log_action("retry_task", {"dag_id": dag_id, "run_id": run_id, "task_id": task_id, "root_cause": root_cause})
        return {
            "retried": True,
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "message": f"Task '{task_id}' cleared and queued for retry. Monitor the Airflow UI to confirm it succeeds.",
        }

    @_safe
    def trigger_new_dag_run(self, dag_id: str, root_cause: str, confidence: str, recent_failure_count: int) -> dict:
        guard = check_guardrails(root_cause, recent_failure_count, confidence)
        if not guard["allowed"]:
            _log_action("trigger_blocked", {"dag_id": dag_id, "reason": guard["reason"]})
            return {"triggered": False, "blocked": True, "reason": guard["reason"]}
        result = self.client.trigger_dag_run(dag_id)
        _log_action("trigger_dag_run", {"dag_id": dag_id, "root_cause": root_cause, "new_run_id": result.get("new_run_id")})
        return {
            "triggered": True,
            "dag_id": dag_id,
            "new_run_id": result.get("new_run_id"),
            "message": f"New run triggered for '{dag_id}'. New run ID: {result.get('new_run_id')}",
        }


TOOL_SCHEMAS = [
    {
        "name": "get_dag_run_status",
        "description": "Get overall status of a DAG run including which tasks failed, succeeded, or were skipped. Always call this first.",
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
        "description": "Fetch the tail of logs for a specific task. Read these to find the actual error message and stack trace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string"},
                "task_id": {"type": "string"},
                "try_number": {"type": "integer", "default": 1},
                "tail_lines": {"type": "integer", "default": 150},
            },
            "required": ["dag_id", "run_id", "task_id"],
        },
    },
    {
        "name": "get_task_dependencies",
        "description": "Get upstream and downstream task IDs. Use when a failure might be caused by an upstream task.",
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
        "description": "Get recent run history for a DAG. Check recent_failure_count to assess if this is recurring.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "n": {"type": "integer", "default": 10},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "get_xcom_value",
        "description": (
            "Fetch an XCom value produced by a specific task. "
            "Call this when a downstream task failed and may have consumed bad/missing data "
            "from an upstream task's output."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string"},
                "task_id": {"type": "string"},
                "key": {"type": "string", "default": "return_value"},
            },
            "required": ["dag_id", "run_id", "task_id"],
        },
    },
    {
        "name": "get_cross_dag_failures",
        "description": (
            "Check for recent failures in other DAGs. "
            "Call this when the failing DAG is likely a consumer of another DAG "
            "(e.g., a reporting DAG that depends on ETL DAGs). "
            "If another DAG failed recently, this run's failure is probably upstream_failure."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "current_dag_id": {"type": "string"},
                "hours": {"type": "integer", "default": 2},
            },
            "required": ["current_dag_id"],
        },
    },
    {
        "name": "check_fix_eligibility",
        "description": (
            "Check whether auto-fix is permitted for this failure. "
            "You MUST call this before attempting any retry or trigger. "
            "Pass the root_cause and confidence you determined, plus recent_failure_count."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "root_cause": {
                    "type": "string",
                    "enum": ["rate_limit", "infra_timeout", "upstream_failure", "schema_drift", "data_quality", "code_bug", "unknown"],
                },
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                "recent_failure_count": {"type": "integer"},
            },
            "required": ["dag_id", "root_cause", "confidence", "recent_failure_count"],
        },
    },
    {
        "name": "retry_failed_task",
        "description": (
            "Clear a specific failed task so Airflow re-runs it. "
            "Only call after check_fix_eligibility returns allowed=true. "
            "Use for transient failures (rate_limit, infra_timeout) on a specific task."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "run_id": {"type": "string"},
                "task_id": {"type": "string"},
                "root_cause": {"type": "string"},
                "confidence": {"type": "string"},
                "recent_failure_count": {"type": "integer"},
            },
            "required": ["dag_id", "run_id", "task_id", "root_cause", "confidence", "recent_failure_count"],
        },
    },
    {
        "name": "trigger_new_dag_run",
        "description": (
            "Trigger a completely new DAG run from scratch. "
            "Use when a task-level retry is not sufficient. "
            "Only call after check_fix_eligibility returns allowed=true."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "root_cause": {"type": "string"},
                "confidence": {"type": "string"},
                "recent_failure_count": {"type": "integer"},
            },
            "required": ["dag_id", "root_cause", "confidence", "recent_failure_count"],
        },
    },
]


def dispatch_tool(tools: TriageTools, name: str, arguments: dict) -> str:
    method = getattr(tools, name, None)
    if method is None:
        return json.dumps({"error": f"Unknown tool: {name}"})
    result = method(**arguments)
    return json.dumps(result, default=str)
