"""
Slack notifier. Set SLACK_WEBHOOK_URL to enable.

Get a webhook URL: https://api.slack.com/messaging/webhooks
"""
from __future__ import annotations

import json
import os
from typing import Optional

import requests


SEVERITY_EMOJI = {
    "high": "🔴",
    "medium": "🟡",
    "low": "🟢",
}

ROOT_CAUSE_EMOJI = {
    "data_quality": "📊",
    "schema_drift": "🔀",
    "upstream_failure": "⬆️",
    "infra_timeout": "⏱️",
    "rate_limit": "🚦",
    "code_bug": "🐛",
    "unknown": "❓",
}


def post_to_slack(report: dict, webhook_url: Optional[str] = None) -> bool:
    """Post a triage report to Slack. Returns True if sent."""
    url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("SLACK_WEBHOOK_URL not set - skipping Slack notification")
        return False

    confidence = report.get("confidence", "low")
    root_cause = report.get("root_cause", "unknown")

    emoji = ROOT_CAUSE_EMOJI.get(root_cause, "⚠️")
    severity = SEVERITY_EMOJI.get(confidence, "🟡")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Pipeline Failure: {report.get('dag_id', 'unknown')}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Root cause:*\n{root_cause}"},
                {"type": "mrkdwn", "text": f"*Confidence:* {severity} {confidence}"},
                {"type": "mrkdwn", "text": f"*Run ID:*\n`{report.get('run_id', '?')}`"},
                {
                    "type": "mrkdwn",
                    "text": f"*Needs human:*\n{'Yes' if report.get('needs_human') else 'No'}",
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Explanation:*\n{report.get('explanation', 'N/A')}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Suggested action:*\n{report.get('suggested_action', 'N/A')}",
            },
        },
    ]

    evidence = report.get("evidence")
    if evidence:
        bullets = "\n".join(f"• {e}" for e in evidence)
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Evidence:*\n{bullets}"},
        })

    payload = {"blocks": blocks}

    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"Failed to post to Slack: {e}")
        return False


if __name__ == "__main__":
    # Quick test
    sample = {
        "dag_id": "weather_etl",
        "run_id": "manual__2024-01-15T10:00:00",
        "root_cause": "rate_limit",
        "confidence": "high",
        "explanation": "The fetch_weather task failed with HTTP 429. Recent run history shows this is the third rate-limit hit today, suggesting we're consistently exceeding the API quota.",
        "evidence": ["HTTP 429 in logs", "3 of last 10 runs failed similarly"],
        "suggested_action": "Add exponential backoff retry, or request a higher rate-limit tier from the API provider.",
        "needs_human": False,
    }
    post_to_slack(sample)
