# Airflow Project

## Introduction

This repository is a hands-on learning project for **Apache Airflow**, an open-source platform for authoring, scheduling, and monitoring workflows. It brings together a local Airflow environment and a collection of small Python DAG examples, so you can explore orchestration concepts in the UI and in code without needing a cloud account or an external data platform.

The examples build progressively from a first three-task workflow to more advanced patterns: passing data between tasks, running work in parallel, branching based on task results, defining schedules, reacting to data assets, and coordinating multiple DAGs. They are intentionally focused demonstrations rather than a single end-to-end business pipeline. Most tasks print or return simple example values; use them to understand Airflow behavior, then adapt the patterns to your own data sources and processing logic.

The project runs Airflow with Docker Compose. The API server, scheduler, DAG processor, Celery worker, and triggerer work together to parse and run the DAGs. PostgreSQL stores Airflow metadata such as DAG runs, task-instance state, and connections, while Redis provides the Celery message broker. The DAG source remains in your local `dags/` directory and is mounted into the containers, making it easy to inspect and edit.

### Project goals

- Learn how an Airflow DAG describes a workflow and how tasks define its individual units of work.
- See how dependencies control task order, parallel execution, and conditional branches.
- Compare schedule presets, cron expressions, timezones, interval-based timetables, and special-date schedules.
- Understand how task outputs can be shared using XComs and how asset-aware scheduling connects producer and consumer workflows.
- Explore how one DAG can trigger and coordinate other DAGs.
- Practice running, observing, and troubleshooting workflows in a self-contained local environment.

### How to explore the project

1. Start the Docker Compose environment using the instructions below.
2. Open the Airflow UI and inspect the DAG graph and task details before triggering a workflow.
3. Begin with `1_first_dag.py`, then compare the dependency, XCom, parallelism, and branching examples.
4. Review the scheduling examples and note each timetable's schedule and timezone.
5. Explore the asset and orchestration examples to see how workflows can depend on events or other DAGs.
6. Modify one example at a time and inspect task logs and run history in the UI.

> **Scope:** This repository is a local educational starter project, not a production-ready deployment. The sample tasks do not implement a real ETL pipeline, and the Compose configuration uses development defaults. Review schedules, credentials, networking, data handling, and operational requirements before adapting it for real workloads.

## Workflow concepts demonstrated

The examples are in [`dags/`](./dags/). This guide groups them by the concepts they demonstrate:

| File | Topic |
| --- | --- |
| `1_first_dag.py` | A simple linear workflow built with TaskFlow tasks |
| `2_dag_versioning.py` | DAG versioning concepts |
| `3_operators.py` | Python and Bash task/operator examples |
| `4_XCOMs_auto.py` | Passing task results with automatic XComs |
| `5_XCOMs_kwargs.py` | Explicitly pushing and pulling XCom values |
| `6_parallel_tasks.py` | Running independent tasks in parallel and combining dependencies |
| `7_branches.py` | Selecting a task path conditionally |
| `8_schedule_preset.py` | Scheduling with an Airflow preset such as `@daily` |
| `9_schedule_cron.py` | Cron scheduling with an explicit timezone |
| `10_schedule_delta.py` | Scheduling with a time-delta timetable |
| `11_incremental_load.py` | Cron data intervals and an incremental-load pattern |
| `12_special_dates.py` | Scheduling on configured special dates |
| `asset_13.py` | Defining a scheduled data asset producer |
| `14_asset_dependent.py` | Defining a workflow that depends on an upstream asset |
| `dag_orchestrate_1.py` | First example workflow for DAG orchestration |
| `dag_orchestrate_2.py` | Second example workflow for DAG orchestration |
| `dag_orchestrate_parent.py` | Triggering child DAGs from a parent workflow |

The numbered filenames provide a suggested starting order; the asset and orchestration examples are additional patterns to explore. Check each DAG's code and schedule before enabling it.

## What happens when a DAG runs

Airflow separates describing a workflow from executing it:

1. **Author:** A Python file under `dags/` declares a DAG and its tasks. Dependencies describe which tasks must finish before others can start.
2. **Parse:** The DAG processor reads the mounted files and makes valid DAG definitions available to Airflow.
3. **Schedule or trigger:** The scheduler creates runs according to a DAG's schedule, or a user starts one manually from the UI or CLI.
4. **Queue and execute:** The scheduler determines which tasks are ready. With the Celery executor, ready work is sent through Redis to a worker.
5. **Record and observe:** Task and run state is stored in PostgreSQL and displayed in the UI. Task logs help diagnose failures.

In this local setup, editing a file under `dags/` updates the mounted source. The DAG processor can then parse the updated definition without building a new image. A code edit does not automatically run the DAG; use the UI or CLI to trigger a run, or wait for its schedule.

## Architecture

Docker Compose starts the services used by this local development stack:

- **Airflow API server** — web UI and API on `http://localhost:8080`.
- **Scheduler** — schedules DAG runs and tasks.
- **DAG processor** — parses files mounted from `dags/`.
- **Celery worker** — executes queued tasks.
- **Triggerer** — handles deferrable tasks.
- **PostgreSQL 16** — Airflow metadata database, exposed on port `5432`.
- **Redis 7.2** — Celery message broker.

The Compose image defaults to Apache Airflow `3.4.0`, matching the version resolved by `uv.lock`. DAG files are mounted into the Airflow containers, so local edits in `dags/` are picked up without rebuilding an image.

## Prerequisites

- Windows with Docker Desktop running (WSL 2 backend recommended), or another supported Docker host.
- Docker Compose v2 (`docker compose`).
- At least 4 GB of memory available to Docker is recommended.
- Optional for local Python package development: Python 3.12 and [uv](https://docs.astral.sh/uv/).

## Start Airflow

Run these commands from the repository root in PowerShell:

```powershell
# Create your local Compose environment file.
Copy-Item .env.example .env

# Initialize the metadata database and create the initial Airflow user.
docker compose up airflow-init

# Start the Airflow services in the background.
docker compose up -d

# Check service health/status.
docker compose ps
```

Open [http://localhost:8080](http://localhost:8080) and sign in with the local development credentials:

- Username: `airflow`
- Password: `airflow`

These default credentials are for local learning only. Change them before exposing Airflow beyond your own machine. The username and password can be overridden with `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` in `.env`.

To follow API server logs:

```powershell
docker compose logs -f airflow-apiserver
```

To stop the services while retaining the PostgreSQL data volume:

```powershell
docker compose down
```

> **Warning:** `docker compose down --volumes` deletes the PostgreSQL volume, including Airflow users, metadata, connections, variables, and run history. Use it only when you intentionally want to reset the local Airflow database.

## Use the DAGs

1. Open the Airflow UI and select **DAGs**.
2. Wait for the DAG processor to load the files from `dags/`.
3. Unpause a DAG before expecting its schedule to create runs.
4. Trigger a DAG manually from its row or detail page to try it immediately.

You can also trigger the first example from PowerShell:

```powershell
docker compose exec airflow-scheduler airflow dags trigger first_dag
```

Some examples intentionally use schedules or task behavior that make more sense for learning than for immediate production use. Check the DAG source before triggering a workflow.

## Connect to PostgreSQL

The Compose configuration creates a local database with the development settings `airflow` / `airflow` / `airflow` for username, password, and database name. Open a PostgreSQL shell with:

```powershell
docker compose exec postgres psql -U airflow -d airflow
```

In this command, `-U` specifies the database **user** and `-d` specifies the database. For example, `-U airflow-d` would try to log in as a PostgreSQL role literally named `airflow-d`; that role is not created by this project.

The database settings are configured in [`docker-compose.yaml`](./docker-compose.yaml). PostgreSQL initializes its user and database when its data volume is first created. Changing the environment variables later does not automatically change roles inside an existing volume.

## Python development (optional)

The Docker Compose setup is the recommended way to run the Airflow services. To install the Python project dependencies into a local virtual environment with uv:

```powershell
uv sync
uv run airflow version
```

The project requires Python 3.12 or newer. The dependency lockfile pins Apache Airflow `3.4.0` and should be committed alongside `pyproject.toml` to keep installs reproducible.

## Repository layout

```text
.
├── dags/                 # Airflow DAG examples mounted into the containers
├── src/airflow_project/  # Python package source
├── Config/               # Local generated Airflow configuration (not committed)
├── Logs/                 # Local task and scheduler logs (not committed)
├── docker-compose.yaml   # Airflow, PostgreSQL, Redis, and worker services
├── pyproject.toml        # Python project metadata and dependencies
├── uv.lock               # Resolved Python dependency versions
└── .env.example          # Safe starting point for local environment settings
```

`Config/` and `Logs/` are generated runtime data. They are intentionally ignored by Git; do not commit them. The real `.env` file and Python virtual environment are local-only as well.

## Troubleshooting

### A PostgreSQL role does not exist

Use the configured PostgreSQL username and database:

```powershell
docker compose exec postgres psql -U airflow -d airflow
```

If PostgreSQL is not healthy, inspect the services and database logs:

```powershell
docker compose ps
docker compose logs postgres
```

If an existing PostgreSQL volume was initialized with different credentials, use those existing credentials or intentionally recreate the local database after backing up any data you need.

### Airflow UI is unavailable

Check that Docker Desktop is running and that the services have started:

```powershell
docker compose ps
docker compose logs airflow-apiserver airflow-scheduler
```

Wait for the API server health check to pass, then reload `http://localhost:8080`.

### A DAG is missing or has an import error

Check the DAG processor and scheduler logs:

```powershell
docker compose logs airflow-dag-processor airflow-scheduler
```

Confirm the Python file is under `dags/`, then review the reported import or syntax error in that file.

## Configuration and security

The Compose file is intended for local development. It contains default development database and Airflow credentials and publishes service ports on the host. Do not expose this setup to an untrusted network or use its defaults in production. Keep `.env` private and never commit credentials, logs, or generated configuration.
