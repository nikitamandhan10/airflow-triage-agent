# Airflow Triage Agent

An LLM-powered agent that investigates failed Airflow DAG runs, identifies root causes, and posts structured triage reports.

## What it does

When an Airflow DAG fails, the agent:
1. Fetches the failed run's metadata via the Airflow REST API
2. Pulls task logs and upstream dependency states
3. Uses Claude with tool-use to investigate iteratively (the agent decides what to look at)
4. Classifies the root cause and suggests next actions
5. Posts a structured report to Slack (or stdout)

## Architecture

```
┌─────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   Airflow   │◄─────│  Triage Agent    │─────►│  Claude (LLM)    │
│  REST API   │      │  (tool loop)     │      │  with tool-use   │
└─────────────┘      └──────────────────┘      └──────────────────┘
                              │
                              ▼
                     ┌──────────────────┐
                     │  Slack / stdout  │
                     └──────────────────┘
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
│   ├── triage_dumb.py       # Week 3: pre-LLM structured triage
│   ├── triage_llm.py        # Week 4: single LLM call
│   ├── triage_agent.py      # Week 5: agentic with tool-use
│   └── tools.py             # Tools the agent can call
├── slack/
│   └── notify.py            # Slack webhook poster
├── evals/
│   └── test_cases.py        # Known failures + expected root causes
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
```

## Roadmap (the learning path)

This repo is structured so you can follow a 6-week build:

- **Week 1:** Get Airflow running with the DAGs in `dags/`
- **Week 2:** Read and extend `agent/airflow_client.py`
- **Week 3:** Run `agent/triage_dumb.py` — no LLM, just structured context
- **Week 4:** Run `agent/triage_llm.py` — single LLM call for classification
- **Week 5:** Run `agent/triage_agent.py` — full agent with tool-use loop
- **Week 6:** Wire `slack/notify.py`, add evals in `evals/`, write the README

## What "done" looks like for the portfolio

- Demo GIF of the agent investigating a real failure across multiple tool calls
- Eval results showing classification accuracy on known failure types
- A clear README (this file) with architecture, setup, and learnings
- Honest "what I'd do next" section
