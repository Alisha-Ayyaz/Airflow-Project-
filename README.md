# Airflow Project

A local Apache Airflow learning project with runnable examples of DAG authoring, task dependencies, scheduling, asset-aware workflows, and DAG-to-DAG orchestration. The project runs Airflow with Docker Compose and stores its metadata in PostgreSQL.

## What is included

The examples are in [`dags/`](./dags/). They are intended to be read, run, and modified as you learn Airflow:

| File | Topic |
| --- | --- |
| `1_first_dag.py` | A simple linear workflow built with TaskFlow tasks |
| `2_dag_versioning.py` | DAG versioning concepts |
| `3_operators.py` | Python and Bash task/operator examples |
| `4_XCOMs_auto.py` | Passing task results with automatic XComs |
| `5_XCOMs_kwargs.py` | Explicitly pushing and pulling XCom values |
| `6_parallel_tasks.py` | Running independent tasks in parallel |
| `7_branches.py` | Branching to different task paths |
| `8_schedule_preset.py` | Scheduling with an Airflow preset such as `@daily` |
| `9_schedule_cron.py` | Cron scheduling with an explicit timezone |
| `10_schedule_delta.py` | Scheduling with a delta timetable |
| `11_incremental_load.py` | Cron data intervals for incremental-load patterns |
| `12_special_dates.py` | Scheduling on configured special dates |
| `asset_13.py` | Producing a data asset |
| `14_asset_dependent.py` | Running a workflow when an upstream asset is updated |
| `dag_orchestrate_1.py` | First example workflow for DAG orchestration |
| `dag_orchestrate_2.py` | Second example workflow for DAG orchestration |
| `dag_orchestrate_parent.py` | Triggering child DAGs from a parent workflow |

These are educational examples, not production data pipelines. Review each DAG's schedule and task behavior before enabling it in a shared environment.

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
