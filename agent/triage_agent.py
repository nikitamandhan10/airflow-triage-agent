"""
Advanced triage agent with auto-fix capability.
TWO PHASES:
  Phase 1 - Investigate: read-only tools to understand the failure
  Phase 2 - Remediate:   decide whether to auto-fix or escalate

The agent MUST call check_fix_eligibility before any write action.
Guardrails are enforced both in the prompt AND in Python code.

Usage:
    python -m agent.triage_agent --latest
    python -m agent.triage_agent --dag weather_etl --run-id <run_id>
    python -m agent.triage_agent --watch
    python -m agent.triage_agent --latest --dry-run   # investigate only, no fixes
"""

from __future__ import annotations
import argparse
import json
import os
import time
from typing import Optional

import anthropic
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from agent.airflow_client import AirflowClient
from agent.tools import TOOL_SCHEMAS, TriageTools, dispatch_tool, ACTION_LOG_PATH
from slack.notify import post_to_slack

MODEL = "claude-sonnet-4-6"
MAX_AGENT_TURNS = 15

# Write tool names - used to block them in dry-run mode
WRITE_TOOLS = {"retry_failed_task", "trigger_new_dag_run"}

SYSTEM_PROMPT = """You are an autonomous on-call data engineering agent. You investigate \
failed Airflow DAG runs and fix them when it is safe to do so.

## PHASE 1: INVESTIGATE (always do this first)
1. Call get_dag_run_status to see which tasks failed
2. Call get_task_logs on the primary failed task to read the error
3. If the error suggests an upstream within-DAG cause, call get_task_dependencies
3b. If the DAG name suggests it consumes other DAGs (e.g., "reporting", "summary", "aggregate"),
    call get_cross_dag_failures to check if an upstream DAG failed recently
3c. If a task likely consumed XCom from an upstream task and the error involves missing/bad data,
    call get_xcom_value to inspect what the upstream task produced
3d. If root_cause is upstream_failure, call get_upstream_dag_health with the upstream dag_ids.
    Note whether all_upstream_healthy is true or false — you will pass this to check_fix_eligibility.
4. Call get_recent_run_history to check if this is recurring (get recent_failure_count)
5. Form a conclusion: root_cause + confidence + recent_failure_count

## PHASE 2: REMEDIATE (after investigation)
6. Call check_fix_eligibility with your root_cause, confidence, recent_failure_count,
   and upstream_healthy (only relevant for upstream_failure).
7a. If allowed=true  → call retry_failed_task OR trigger_new_dag_run, then write your final report
7b. If allowed=false → write your final report explaining why human intervention is needed

## GUARDRAIL RULES (these are also enforced in code)
- rate_limit, infra_timeout + high confidence + <=2 recent failures → auto-fix allowed
- schema_drift, data_quality, code_bug, unknown → NEVER auto-fix, always escalate
- upstream_failure + upstream recovered (all_upstream_healthy=true) + high confidence + <=2 failures → auto-fix allowed (trigger new run)
- upstream_failure + upstream still failing → NEVER auto-fix (fix upstream first)
- medium or low confidence → NEVER auto-fix
- 3+ recent failures → NEVER auto-fix (chronic issue needs human)

## RETRY FEEDBACK
If the initial message includes historical retry data for this DAG, factor it in:
- Low retry success rate (<50%) → raise your confidence threshold, prefer escalation over auto-fix

## RETRY vs TRIGGER
- Use retry_failed_task when a specific task failed transiently (rate limit, timeout)
- Use trigger_new_dag_run when the whole pipeline needs to restart from scratch

## FINAL REPORT FORMAT
Always end with ONLY this JSON (no other text after it):
{
  "root_cause": "rate_limit|infra_timeout|upstream_failure|schema_drift|data_quality|code_bug|unknown",
  "confidence": "high|medium|low",
  "explanation": "2-3 sentences",
  "evidence": ["finding 1", "finding 2"],
  "action_taken": "retried_task|triggered_new_run|escalated_to_human|none",
  "action_detail": "what exactly was done or why it was escalated",
  "needs_human": true|false
}"""


console = Console()


def get_retry_feedback(dag_id: str, airflow: AirflowClient) -> dict:
    """Return historical retry success rate for a DAG by checking past action log entries."""
    if not ACTION_LOG_PATH.exists():
        return {"attempts": 0, "successes": 0, "rate": None}

    lines = ACTION_LOG_PATH.read_text().strip().splitlines()
    retries = [
        json.loads(l) for l in lines
        if json.loads(l).get("dag_id") == dag_id
        and json.loads(l).get("action") in ("retry_task", "trigger_dag_run")
    ]

    if not retries:
        return {"attempts": 0, "successes": 0, "rate": None}

    successes = 0
    for entry in retries:
        run_id_entry = entry.get("run_id")
        task_id = entry.get("task_id")
        if run_id_entry and task_id:
            try:
                state = airflow.get_task_instance_state(dag_id, run_id_entry, task_id)
                if state == "success":
                    successes += 1
            except Exception:
                pass

    rate = successes / len(retries)
    return {"attempts": len(retries), "successes": successes, "rate": rate}


def run_agent(
    dag_id: str,
    run_id: str,
    airflow: Optional[AirflowClient] = None,
    llm: Optional[anthropic.Anthropic] = None,
    verbose: bool = True,
    dry_run: bool = False,
) -> dict:
    airflow = airflow or AirflowClient()
    llm = llm or anthropic.Anthropic()
    tools = TriageTools(airflow)

    mode = "[yellow]DRY RUN - no fixes will be applied[/yellow]" if dry_run else "[green]LIVE - agent can auto-fix[/green]"
    if verbose:
        console.print(f"Mode: {mode}")

    feedback = get_retry_feedback(dag_id, airflow)
    feedback_note = ""
    if feedback["attempts"] > 0:
        rate_pct = f"{feedback['rate']:.0%}"
        feedback_note = (
            f"\n\nHistorical retry data for {dag_id}: "
            f"{feedback['successes']}/{feedback['attempts']} past auto-retries succeeded ({rate_pct}). "
            + ("Low success rate — prefer escalation over auto-fix unless confidence is very high." if feedback["rate"] < 0.5 else "Retry history looks healthy.")
        )

    initial_message = (
        f"Investigate and remediate this failed Airflow DAG run:\n"
        f"  dag_id: {dag_id}\n"
        f"  run_id: {run_id}\n\n"
        f"Follow the two-phase process: investigate first, then remediate if safe."
        + feedback_note
        + ("\n\nNOTE: This is a dry run. Do not call retry_failed_task or trigger_new_dag_run." if dry_run else "")
    )

    messages = [{"role": "user", "content": initial_message}]

    for turn in range(MAX_AGENT_TURNS):
        if verbose:
            console.rule(f"[bold blue]Turn {turn + 1}")

        response = llm.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})

        if verbose:
            for block in response.content:
                if block.type == "text" and block.text.strip():
                    console.print(Panel(block.text, title="Agent thinking", border_style="cyan"))
                elif block.type == "tool_use":
                    is_write = block.name in WRITE_TOOLS
                    color = "red" if is_write else "yellow"
                    label = "Write tool call" if is_write else "Tool call"
                    console.print(Panel(
                        f"[{color}]{block.name}[/{color}]({json.dumps(block.input, indent=2, default=str)})",
                        title=label,
                        border_style=color,
                    ))

        if response.stop_reason == "end_turn":
            final_text = "".join(b.text for b in response.content if b.type == "text")
            return _parse_final_report(final_text, dag_id, run_id, turn + 1)

        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                # Block write tools in dry-run mode
                if dry_run and block.name in WRITE_TOOLS:
                    result_json = json.dumps({
                        "blocked": True,
                        "reason": "Dry run mode - no changes applied",
                    })
                else:
                    result_json = dispatch_tool(tools, block.name, block.input)

                if verbose:
                    preview = result_json[:400] + ("..." if len(result_json) > 400 else "")
                    console.print(Panel(preview, title=f"Result: {block.name}", border_style="green"))

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_json,
                })

        messages.append({"role": "user", "content": tool_results})

    return {
        "root_cause": "unknown",
        "confidence": "low",
        "explanation": f"Agent did not converge within {MAX_AGENT_TURNS} turns",
        "action_taken": "none",
        "action_detail": "Max turns exceeded",
        "needs_human": True,
        "dag_id": dag_id,
        "run_id": run_id,
    }


def _parse_final_report(text: str, dag_id: str, run_id: str, turns: int) -> dict:
    cleaned = text.strip()
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].rsplit("```", 1)[0]
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        report = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1:
            try:
                report = json.loads(cleaned[start:end + 1])
            except json.JSONDecodeError:
                report = {"root_cause": "unknown", "confidence": "low",
                          "explanation": f"Parse error: {text[:200]}", "needs_human": True}
        else:
            report = {"root_cause": "unknown", "confidence": "low",
                      "explanation": f"No JSON found: {text[:200]}", "needs_human": True}

    report.setdefault("action_taken", "none")
    report.setdefault("action_detail", "")
    report["dag_id"] = dag_id
    report["run_id"] = run_id
    report["agent_turns"] = turns
    return report


def print_summary_table(report: dict):
    """Print a clean summary table after the JSON report."""
    table = Table(title="Triage Summary", show_header=False, border_style="blue")
    table.add_column("Field", style="bold")
    table.add_column("Value")

    action = report.get("action_taken", "none")
    action_color = "green" if action in ("retried_task", "triggered_new_run") else "yellow" if action == "none" else "red"

    table.add_row("DAG", report.get("dag_id", "?"))
    table.add_row("Root Cause", report.get("root_cause", "?"))
    table.add_row("Confidence", report.get("confidence", "?"))
    table.add_row("Action Taken", f"[{action_color}]{action}[/{action_color}]")
    table.add_row("Needs Human", "YES ⚠️" if report.get("needs_human") else "No ✓")
    table.add_row("Agent Turns", str(report.get("agent_turns", "?")))
    console.print(table)


def show_action_log():
    """Print the last 10 entries from the action audit log."""
    if not ACTION_LOG_PATH.exists():
        console.print("[dim]No action log yet.[/dim]")
        return
    lines = ACTION_LOG_PATH.read_text().strip().splitlines()
    recent = lines[-10:]
    console.rule("[bold]Recent Actions (audit log)")
    for line in recent:
        entry = json.loads(line)
        color = "red" if "retry" in entry["action"] or "trigger" in entry["action"] else "dim"
        console.print(f"[{color}]{entry['timestamp']}[/{color}] {entry['action']} | {json.dumps({k:v for k,v in entry.items() if k not in ('timestamp','action')})}")


def check_action_outcomes(airflow: AirflowClient):
    """Read action log, find past retries, and report whether they succeeded."""
    if not ACTION_LOG_PATH.exists():
        console.print("[dim]No action log yet.[/dim]")
        return

    lines = ACTION_LOG_PATH.read_text().strip().splitlines()
    retries = [
        json.loads(l) for l in lines
        if json.loads(l).get("action") in ("retry_task", "trigger_dag_run")
    ]

    if not retries:
        console.print("[dim]No retries or triggers in action log.[/dim]")
        return

    table = Table(title="Retry Outcomes", show_header=True, border_style="blue")
    table.add_column("Timestamp", style="dim")
    table.add_column("DAG")
    table.add_column("Task / New Run")
    table.add_column("Current State")

    for entry in retries[-20:]:
        dag_id = entry.get("dag_id", "?")
        run_id = entry.get("run_id")
        task_id = entry.get("task_id")
        new_run_id = entry.get("new_run_id")

        label = task_id or new_run_id or "?"

        if run_id and task_id:
            try:
                state = airflow.get_task_instance_state(dag_id, run_id, task_id)
                color = "green" if state == "success" else "red" if state == "failed" else "yellow"
                state_str = f"[{color}]{state}[/{color}]"
            except Exception as e:
                state_str = f"[dim]unavailable ({e})[/dim]"
        else:
            state_str = "[dim]n/a[/dim]"

        table.add_row(entry["timestamp"][:19], dag_id, label, state_str)

    console.print(table)


def find_latest_failure(client: AirflowClient):
    runs = client.get_recent_failed_runs(hours=72, limit=5)
    if not runs:
        return None
    return runs[0].dag_id, runs[0].run_id


def main():
    parser = argparse.ArgumentParser(description="Airflow Triage Agent with Auto-Fix")
    parser.add_argument("--dag", help="DAG ID")
    parser.add_argument("--run-id", help="Run ID")
    parser.add_argument("--latest", action="store_true", help="Triage most recent failure")
    parser.add_argument("--watch", action="store_true", help="Poll and triage new failures")
    parser.add_argument("--dry-run", action="store_true", help="Investigate only, no auto-fixes")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--show-log", action="store_true", help="Show recent action audit log")
    parser.add_argument("--check-outcomes", action="store_true", help="Check whether past auto-retries succeeded")
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Set ANTHROPIC_API_KEY environment variable")

    if args.show_log:
        show_action_log()
        return

    airflow = AirflowClient()

    if args.check_outcomes:
        check_action_outcomes(airflow)
        return

    if args.watch:
        console.print("[bold]Watching for failures...[/bold] (Ctrl-C to stop)")
        if args.dry_run:
            console.print("[yellow]DRY RUN mode - will investigate but not fix[/yellow]")
        seen = set()
        while True:
            try:
                runs = airflow.get_recent_failed_runs(hours=1, limit=10)
                for run in runs:
                    key = (run.dag_id, run.run_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    console.rule(f"[bold red]New failure: {run.dag_id}")
                    report = run_agent(
                        run.dag_id, run.run_id,
                        airflow=airflow,
                        verbose=not args.quiet,
                        dry_run=args.dry_run,
                    )
                    console.print(Panel(
                        Syntax(json.dumps(report, indent=2), "json"),
                        title="FINAL REPORT",
                        border_style="red",
                    ))
                    print_summary_table(report)
                    post_to_slack(report)
                time.sleep(30)
            except KeyboardInterrupt:
                console.print("\nStopped.")
                break
        return

    if args.latest:
        latest = find_latest_failure(airflow)
        if latest is None:
            console.print("[red]No recent failures found.[/red]")
            return
        dag_id, run_id = latest
    else:
        if not (args.dag and args.run_id):
            parser.error("Provide --dag and --run-id, or use --latest or --watch")
        dag_id, run_id = args.dag, args.run_id

    console.print(f"[bold]Triaging:[/bold] {dag_id} / {run_id}")
    report = run_agent(
        dag_id, run_id,
        airflow=airflow,
        verbose=not args.quiet,
        dry_run=args.dry_run,
    )

    console.rule("[bold green]FINAL REPORT")
    console.print(Syntax(json.dumps(report, indent=2), "json"))
    print_summary_table(report)
    post_to_slack(report)


if __name__ == "__main__":
    main()
