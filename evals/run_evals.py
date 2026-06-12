"""
Evaluation harness — runs against mock scenarios, no live Airflow needed.

Usage:
    python -m evals.run_evals
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

from evals.mock_airflow import MockAirflowClient
from agent.triage_agent import run_agent


@dataclass
class TestCase:
    description: str
    dag_id: str
    run_id: str
    scenario: str
    expected_root_cause: str
    expected_needs_human: Optional[bool] = None


TEST_CASES: list[TestCase] = [
    TestCase(
        description="weather API rate limit — transient, should auto-retry",
        dag_id="weather_etl",
        run_id="mock_rate_limit",
        scenario="rate_limit",
        expected_root_cause="rate_limit",
        expected_needs_human=False,
    ),
    TestCase(
        description="sales CSV schema drift — KeyError on renamed column, needs human",
        dag_id="sales_etl",
        run_id="mock_schema_drift",
        scenario="schema_drift",
        expected_root_cause="schema_drift",
        expected_needs_human=True,
    ),
    TestCase(
        description="reporting DAG failed because upstream sales_etl DAG failed",
        dag_id="reporting",
        run_id="mock_upstream",
        scenario="upstream_failure",
        expected_root_cause="upstream_failure",
        expected_needs_human=True,
    ),
]


def run_evals():
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Set ANTHROPIC_API_KEY")

    results = []
    correct = 0

    for tc in TEST_CASES:
        print(f"\n--- {tc.description} ---")
        mock = MockAirflowClient(tc.scenario)
        try:
            report = run_agent(tc.dag_id, tc.run_id, airflow=mock, verbose=False)
            predicted_cause = report.get("root_cause")
            predicted_human = report.get("needs_human")

            cause_ok = predicted_cause == tc.expected_root_cause
            human_ok = (tc.expected_needs_human is None
                        or predicted_human == tc.expected_needs_human)
            is_correct = cause_ok and human_ok

            if is_correct:
                correct += 1

            mark = "✓" if cause_ok else "✗"
            print(f"  root_cause  expected={tc.expected_root_cause:<20s} got={predicted_cause} {mark}")
            if tc.expected_needs_human is not None:
                mark2 = "✓" if human_ok else "✗"
                print(f"  needs_human expected={str(tc.expected_needs_human):<20s} got={predicted_human} {mark2}")
            print(f"  turns={report.get('agent_turns')}  confidence={report.get('confidence')}")
            print(f"  explanation: {report.get('explanation', '')[:120]}")

            results.append({
                "case": tc.description,
                "expected_root_cause": tc.expected_root_cause,
                "predicted_root_cause": predicted_cause,
                "expected_needs_human": tc.expected_needs_human,
                "predicted_needs_human": predicted_human,
                "correct": is_correct,
                "turns": report.get("agent_turns"),
                "confidence": report.get("confidence"),
            })
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({"case": tc.description, "error": str(e)})

    accuracy = correct / len(TEST_CASES) if TEST_CASES else 0
    print(f"\n{'=' * 50}")
    print(f"Accuracy: {correct}/{len(TEST_CASES)} = {accuracy:.0%}")

    os.makedirs("evals", exist_ok=True)
    with open("evals/results.json", "w") as f:
        json.dump({"accuracy": accuracy, "results": results}, f, indent=2)
    print("Results written to evals/results.json")


if __name__ == "__main__":
    run_evals()
