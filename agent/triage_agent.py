"""
Week 5 milestone: the full agent.

The LLM is given tools and decides what to investigate. It calls
tools iteratively, building up understanding, until it produces a
final report.

This is the centerpiece of the portfolio - the demo GIF should show
this running.

Usage:
    python -m agent.triage_agent --latest
    python -m agent.triage_agent --dag weather_etl --run-id <run_id>
    python -m agent.triage_agent --watch
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

from agent.airflow_client import AirflowClient
from agent.tools import TOOL_SCHEMAS, TriageTools, dispatch_tool

MODEL = "claude-sonnet-4-5-20250929"
MAX_AGENT_TURNS = 10  # Safety cap to prevent runaway loops

SYSTEM_PROMPT = """You are an on-call data engineering assistant investigating a failed Airflow DAG run.

You have tools to inspect the failure. Use them iteratively:
1. Start by getting the run status to see which tasks failed
2. Read the logs of the primary failed task
3. If the failure looks like it could be caused upstream, check task dependencies and upstream task states
4. Check recent run history to see if this is recurring or new
5. Once you have enough information, stop calling tools and produce a final report

Keep tool calls focused - don't read every log if you've found the cause. Aim for 3-6 tool calls total.

When you're ready to conclude, respond with a final report (no more tool calls) in this JSON format:
{
  "root_cause": "data_quality|schema_drift|upstream_failure|infra_timeout|rate_limit|code_bug|unknown",
  "confidence": "high|medium|low",
  "explanation": "2-3 sentences explaining your reasoning",
  "evidence": ["bullet point 1", "bullet point 2"],
  "suggested_action": "concrete next step",
  "needs_human": true|false
}"""


console = Console()


def run_agent(
    dag_id: str,
    run_id: str,
    airflow: Optional[AirflowClient] = None,
    llm: Optional[anthropic.Anthropic] = None,
    verbose: bool = True,
) -> dict:
    """Run the agent loop on a single failure."""
    airflow = airflow or AirflowClient()
    llm = llm or anthropic.Anthropic()
    tools = TriageTools(airflow)

    initial_message = (
        f"Investigate this failed Airflow DAG run:\n"
        f"  dag_id: {dag_id}\n"
        f"  run_id: {run_id}\n\n"
        f"Use the available tools to understand what went wrong, then produce the final report."
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

        # Append assistant turn to history
        messages.append({"role": "assistant", "content": response.content})

        # Show what the agent is doing
        if verbose:
            for block in response.content:
                if block.type == "text" and block.text.strip():
                    console.print(Panel(block.text, title="Agent thinking", border_style="cyan"))
                elif block.type == "tool_use":
                    console.print(
                        Panel(
                            f"[yellow]{block.name}[/yellow]({json.dumps(block.input, default=str)})",
                            title="Tool call",
                            border_style="yellow",
                        )
                    )

        # If no tools called, agent is done
        if response.stop_reason == "end_turn":
            final_text = "".join(b.text for b in response.content if b.type == "text")
            return _parse_final_report(final_text, dag_id, run_id, turn + 1)

        # Otherwise, execute tools and feed results back
        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result_json = dispatch_tool(tools, block.name, block.input)
                if verbose:
                    preview = result_json[:300] + ("..." if len(result_json) > 300 else "")
                    console.print(
                        Panel(preview, title=f"Result: {block.name}", border_style="green")
                    )
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
        "needs_human": True,
        "dag_id": dag_id,
        "run_id": run_id,
    }


def _parse_final_report(text: str, dag_id: str, run_id: str, turns: int) -> dict:
    """Extract the JSON report from the agent's final message."""
    cleaned = text.strip()
    # Strip markdown fences
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].rsplit("```", 1)[0]
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        report = json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find a JSON object in the text
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1:
            try:
                report = json.loads(cleaned[start : end + 1])
            except json.JSONDecodeError:
                report = {
                    "root_cause": "unknown",
                    "confidence": "low",
                    "explanation": f"Could not parse final report: {text[:200]}",
                    "needs_human": True,
                }
        else:
            report = {
                "root_cause": "unknown",
                "confidence": "low",
                "explanation": f"No JSON in final response: {text[:200]}",
                "needs_human": True,
            }

    report["dag_id"] = dag_id
    report["run_id"] = run_id
    report["agent_turns"] = turns
    return report


def find_latest_failure(client: AirflowClient) -> Optional[tuple[str, str]]:
    runs = client.get_recent_failed_runs(hours=72, limit=5)
    if not runs:
        return None
    latest = runs[0]
    return latest.dag_id, latest.run_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag", help="DAG ID to triage")
    parser.add_argument("--run-id", help="Specific run ID")
    parser.add_argument("--latest", action="store_true", help="Triage most recent failure")
    parser.add_argument("--watch", action="store_true", help="Poll and triage new failures")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Set ANTHROPIC_API_KEY environment variable")

    airflow = AirflowClient()

    if args.watch:
        console.print("[bold]Watching for new failures...[/bold] (Ctrl-C to stop)")
        seen = set()
        while True:
            try:
                runs = airflow.get_recent_failed_runs(hours=1, limit=10)
                for run in runs:
                    key = (run.dag_id, run.run_id)
                    if key in seen:
                        continue
                    seen.add(key)
                    console.rule(f"[bold red]New failure: {run.dag_id} / {run.run_id}")
                    report = run_agent(run.dag_id, run.run_id, airflow=airflow, verbose=not args.quiet)
                    console.print(Panel(
                        Syntax(json.dumps(report, indent=2), "json"),
                        title="FINAL REPORT",
                        border_style="red",
                    ))
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
    report = run_agent(dag_id, run_id, airflow=airflow, verbose=not args.quiet)
    console.rule("[bold green]FINAL REPORT")
    console.print(Syntax(json.dumps(report, indent=2), "json"))


if __name__ == "__main__":
    main()
