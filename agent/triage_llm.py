"""
Week 4 milestone: a single LLM call enriches the structured report.

The LLM doesn't choose what to look at - we feed it pre-fetched context
and ask for a classification + suggestion. This is the simplest useful
LLM integration. Build this before going agentic in Week 5.

Usage:
    python -m agent.triage_llm --dag weather_etl --run-id <run_id>
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Optional

import anthropic

from agent.triage_dumb import triage

MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are an on-call data engineering assistant. You receive structured \
information about a failed Airflow DAG run and produce a triage report.

Classify the root cause as ONE of:
- data_quality: bad/missing/malformed data from a source
- schema_drift: source schema changed unexpectedly (column rename, type change)
- upstream_failure: this task failed because an upstream task or DAG failed
- infra_timeout: network timeout, connection refused, resource exhaustion
- rate_limit: API rate limiting (429s, throttling)
- code_bug: logic error, KeyError, AttributeError, etc.
- unknown: cannot determine from available info

Respond ONLY with valid JSON in this exact format:
{
  "root_cause": "<one of the categories above>",
  "confidence": "<high|medium|low>",
  "explanation": "<2-3 sentences explaining your reasoning>",
  "suggested_action": "<concrete next step the on-call engineer should take>",
  "needs_human": <true|false>
}"""


def classify_with_llm(report: dict, client: Optional[anthropic.Anthropic] = None) -> dict:
    client = client or anthropic.Anthropic()

    user_message = (
        "Here is the structured failure report:\n\n"
        f"{json.dumps(report, indent=2, default=str)}\n\n"
        "Classify the root cause and suggest a next action."
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}],
    )

    text = response.content[0].text
    # Strip markdown fences if present
    text = text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {
            "root_cause": "unknown",
            "confidence": "low",
            "explanation": f"LLM response could not be parsed as JSON: {text[:200]}",
            "suggested_action": "Manual investigation required",
            "needs_human": True,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Set ANTHROPIC_API_KEY environment variable")

    print("Building structured report...")
    report = triage(args.dag, args.run_id)
    print(json.dumps(report, indent=2, default=str))

    print("\nClassifying with LLM...")
    classification = classify_with_llm(report)
    print(json.dumps(classification, indent=2))


if __name__ == "__main__":
    main()
