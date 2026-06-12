# Airflow Triage Agent

An LLM-powered agent that investigates failed Airflow DAG runs, identifies root causes, and automatically remediates transient failures.

## What it does

When an Airflow DAG fails, the agent:
1. Fetches the failed run's metadata via the Airflow REST API
2. Pulls task logs and upstream dependency states
3. Uses Claude with tool-use to investigate iteratively (the agent decides what to look at)
4. Classifies the root cause and confidence level
5. Checks guardrails, then either auto-fixes or escalates to human
6. Posts a structured report to Slack (or stdout)

## Auto-fix behaviour

The agent operates in two phases — investigate, then remediate.

**Auto-fix is allowed when:**
- Root cause is `rate_limit` or `infra_timeout` — high confidence, ≤2 recent failures
- Root cause is `upstream_failure` — but only after confirming the upstream DAG has since recovered

**Auto-fix is never attempted for:**
- `schema_drift`, `data_quality`, `code_bug`, `unknown` — always escalated to human
- Any failure with medium/low confidence
- DAGs that have failed 3+ times recently (chronic issue, needs human)

Guardrails are enforced in both the prompt and Python code, so the agent cannot bypass them.

## Retry feedback loop

Before investigating a run, the agent checks `action_log.jsonl` for the historical auto-retry success rate of that DAG. If past retries have succeeded less than 50% of the time, the agent is instructed to prefer escalation over auto-fix, even for otherwise eligible failures. This prevents the agent from repeatedly retrying something that keeps failing after the initial fix.

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   Airflow   │◄─────│  Triage Agent    │─────►│  Claude (LLM)    │
│  REST API   │      │  (tool loop)     │      │  with tool-use   │
└─────────────┘      └──────────────────┘      └──────────────────┘
                              │
                       ┌──────┴───────┐
                       ▼              ▼
              ┌──────────────┐  ┌───────────────┐
              │ Slack/stdout │  │ action_log    │
              └──────────────┘  │ .jsonl        │
                                └───────────────┘
```

## Project structure

```
airflow-triage-agent/
├── docker-compose.yaml      # Airflow + Postgres locally
├── dags/                    # Sample DAGs (some flaky on purpose)
│   ├── weather_etl.py       # API ingestion (rate limit failures)
│   ├── sales_etl.py         # CSV load (schema drift failures)
│   └── reporting.py         # Downstream DAG (cascading failures)
├── agent/
│   ├── airflow_client.py    # Airflow REST API wrapper
│   ├── triage_dumb.py       # Pre-LLM structured triage
│   ├── triage_llm.py        # Single LLM call (no tool loop)
│   ├── triage_agent.py      # Agentic with tool-use + auto-fix
│   └── tools.py             # Tools the agent can call
├── slack/
│   └── notify.py            # Slack webhook poster
├── evals/
│   ├── mock_airflow.py      # Mock Airflow client for testing
│   ├── run_evals.py         # Eval harness
│   └── results.json         # Last eval run output
└── requirements.txt
```

## Setup (15 minutes)

### 1. Prerequisites
- Docker Desktop running
- Python 3.10+
- An Anthropic API key (https://console.anthropic.com)

### 2. Start Airflow
```bash
docker compose up -d
# Wait ~60 seconds for Airflow to initialize
# UI: http://localhost:8080  (login: airflow / airflow)
```

### 3. Install Python deps
```bash
pip install -r requirements.txt
export ANTHROPIC_API_KEY="sk-ant-..."
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_USERNAME="airflow"
export AIRFLOW_PASSWORD="airflow"
```

### 4. Trigger some failures
The sample DAGs are designed to fail in interesting ways. In the Airflow UI, unpause `weather_etl`, `sales_etl`, and `reporting`. Within a few minutes you'll have failed runs to investigate.

### 5. Run the agent
```bash
# Investigate the most recent failure
python -m agent.triage_agent --latest

# Investigate a specific run
python -m agent.triage_agent --dag weather_etl --run-id <run_id>

# Watch mode: poll for new failures and triage each
python -m agent.triage_agent --watch

# Dry run: investigate only, no auto-fixes applied
python -m agent.triage_agent --latest --dry-run

# Show the audit log of past auto-fix actions
python -m agent.triage_agent --show-log

# Check whether past auto-retries actually succeeded
python -m agent.triage_agent --check-outcomes
```

### 6. Run evals
```bash
python -m evals.run_evals
# Results written to evals/results.json
```
