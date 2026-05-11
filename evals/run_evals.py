"""
Evaluation harness.

Builds a small test set of known failures and measures the agent's
classification accuracy. Run this in Week 6 to put numbers in your README.

Usage:
    python -m evals.run_evals
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

from agent.airflow_client import AirflowClient
from agent.triage_agent import run_agent


@dataclass
class TestCase:
    description: str
    dag_id: str
    run_id: str
    expected_root_cause: str
    expected_confidence: Optional[str] = None  # If None, any confidence accepted


# Populate this list as you encounter real failures during Week 1 testing.
# Tip: When a flaky DAG fails in a known way, copy the run_id here with
# the expected classification.
TEST_CASES: list[TestCase] = [
    # Examples - replace with real run IDs from your local Airflow:
    # TestCase(
    #     description="weather API rate limit",
    #     dag_id="weather_etl",
    #     run_id="scheduled__2024-01-15T10:00:00+00:00",
    #     expected_root_cause="rate_limit",
    # ),
    # TestCase(
    #     description="sales schema drift - column rename",
    #     dag_id="sales_etl",
    #     run_id="scheduled__2024-01-15T10:20:00+00:00",
    #     expected_root_cause="schema_drift",
    # ),
    # TestCase(
    #     description="reporting DAG waiting on failed upstream",
    #     dag_id="reporting",
    #     run_id="scheduled__2024-01-15T11:00:00+00:00",
    #     expected_root_cause="upstream_failure",
    # ),
]


def run_evals():
    if not TEST_CASES:
        print("No test cases yet. Add some to TEST_CASES once you have real failures.")
        return

    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Set ANTHROPIC_API_KEY")

    airflow = AirflowClient()
    results = []
    correct = 0

    for tc in TEST_CASES:
        print(f"\n--- {tc.description} ---")
        try:
            report = run_agent(tc.dag_id, tc.run_id, airflow=airflow, verbose=False)
            predicted = report.get("root_cause")
            is_correct = predicted == tc.expected_root_cause
            if is_correct:
                correct += 1
            print(f"Expected: {tc.expected_root_cause} | Predicted: {predicted} | {'✓' if is_correct else '✗'}")
            print(f"Turns: {report.get('agent_turns')}")
            print(f"Explanation: {report.get('explanation')}")
            results.append({
                "case": tc.description,
                "expected": tc.expected_root_cause,
                "predicted": predicted,
                "correct": is_correct,
                "turns": report.get("agent_turns"),
                "confidence": report.get("confidence"),
            })
        except Exception as e:
            print(f"Error: {e}")
            results.append({
                "case": tc.description,
                "error": str(e),
            })

    accuracy = correct / len(TEST_CASES) if TEST_CASES else 0
    print(f"\n{'=' * 50}")
    print(f"Accuracy: {correct}/{len(TEST_CASES)} = {accuracy:.0%}")

    with open("evals/results.json", "w") as f:
        json.dump({"accuracy": accuracy, "results": results}, f, indent=2)


if __name__ == "__main__":
    run_evals()
