# Dataplatform — Modern Data Orchestration

A self-hosted data orchestration platform built in Python. Define pipelines in YAML, run them via web UI or REST API, and manage everything from a single dashboard — without Airflow's complexity.

![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue)
![Plugins: 13](https://img.shields.io/badge/Plugins-13-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

---

## What it does

- **Run data pipelines** defined in YAML with task dependencies (DAG execution)
- **Generate pipelines from plain English** using the built-in NLP generator
- **Monitor runs** in real time with a web dashboard, alerts page, and metrics dashboard
- **Schedule pipelines** with cron expressions
- **Trigger pipelines** via webhooks or API events
- **Track lineage, costs, and data quality** per pipeline and asset
- **Manage users** with role-based access control (viewer / editor / admin)

---

## Quick start

### 1. Clone and install

```bash
git clone <repo_url>
cd dataplatform-modern-dbt
pip install -r requirements.txt
```

### 2. Configure environment

```bash
# Minimum required — sets the admin login
export DATAPLATFORM_USERNAME=admin
export DATAPLATFORM_PASSWORD=changeme
export DATAPLATFORM_SECRET_KEY=your-secret-key-here   # JWT signing key

# Optional plugin credentials
export POSTGRES_PASSWORD=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export MYSQL_PASSWORD=...
export API_TOKEN=...
export EMAIL_SENDER=...
export EMAIL_PASSWORD=...
```

A `.env` file at the project root is loaded automatically on startup.

### 3. Start the server

```bash
python -m dataplatform.core.api
# Server runs at http://localhost:8000
```

### 4. Open the dashboard

Navigate to `http://localhost:8000` and log in with the credentials you set above.

---

## Web dashboard

| Page | URL | Purpose |
|------|-----|---------|
| Home | `/` | Workspace overview, quick links |
| Pipelines | `/dashboard` | DAG view, run status, execution history |
| Generator | `/generator` | NLP pipeline builder |
| Catalog | `/catalog` | Data asset catalog |
| Lineage | `/lineage-viz` | Visual data lineage graph |
| Costs | `/costs` | Pipeline cost attribution |
| Templates | `/templates-ui` | Reusable pipeline templates |
| Alerts | `/alerts` | Incident management (Zenduty-style) |
| Monitoring | `/monitoring` | Metrics dashboard (Grafana-style) |
| Admin | `/admin` | User and role management |

---

## Pipelines

### Defining a pipeline

Pipelines are YAML files stored in the `pipelines/` folder. They are auto-discovered on startup.

```yaml
pipeline_name: daily_orders_etl
description: Extract orders from Postgres, validate, aggregate, load to Snowflake

schedule:
  minute: "0"
  hour: "6"
  day: "*"
  month: "*"
  day_of_week: "mon-fri"

tasks:
  - name: extract_orders
    id: extract_orders
    type: executor
    plugin: postgres
    config:
      connection:
        host: localhost
        port: 5432
        database: mydb
        user: user
        password: "${POSTGRES_PASSWORD}"
      sql: "SELECT * FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'"

  - name: validate_orders
    id: validate_orders
    type: executor
    plugin: duckdb
    depends_on: [extract_orders]
    config:
      file_path: data/orders.csv
      checks:
        - name: no_nulls
          sql: "SELECT COUNT(*) FROM data WHERE order_id IS NULL"
          expect: 0
        - name: positive_amounts
          sql: "SELECT COUNT(*) FROM data WHERE amount <= 0"
          expect: 0

  - name: load_to_snowflake
    id: load_to_snowflake
    type: executor
    plugin: snowflake
    depends_on: [validate_orders]
    config:
      snowflake_config:
        account: "${SNOWFLAKE_ACCOUNT}"
        user: "${SNOWFLAKE_USER}"
        password: "${SNOWFLAKE_PASSWORD}"
        warehouse: COMPUTE_WH
        database: MY_DB
        schema: PUBLIC
      table_name: orders_daily
      if_exists: replace
```

### Task fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Human-readable task name |
| `id` | No | Used in `depends_on` references (defaults to slugified name) |
| `type` | Yes | `executor` or `transformer` |
| `plugin` | Yes | Plugin name (see plugins below) |
| `config` | No | Plugin-specific config dict |
| `depends_on` | No | List of task IDs this task waits for |
| `retries` | No | Number of retry attempts on failure (default 0) |

### Execution model

Tasks are executed in **parallel waves** based on their dependency graph. Independent tasks within the same wave run concurrently; a wave must complete before the next begins.

```
Wave 1: [extract_postgres]  [fetch_api]          ← run concurrently
Wave 2: [validate]                               ← waits for both above
Wave 3: [load_snowflake]  [send_email]           ← run concurrently
```

---

## NLP pipeline generator

Describe your pipeline in plain English and get a ready-to-run YAML config.

**Example input:**
```
Extract orders from postgres, validate with duckdb, run dbt transformations,
then load results to snowflake and send email notification
```

**What it produces:** a fully structured YAML with correct plugin assignments, config templates, dependency chain, and task names derived from the detected entities.

Access at `/generator` in the dashboard or via API:

```bash
curl -X POST http://localhost:8000/generate-pipeline \
  -H "Content-Type: application/json" \
  -d '{"text": "load csv from s3, validate with duckdb, send results to bigquery"}'
```

The generator detects:
- Source and target systems (`from postgres`, `into snowflake`)
- ETL verbs (`extract`, `load`, `validate`, `transform`, `aggregate`, `send`, ...)
- Table and model names
- Parallel steps (`simultaneously`, `in parallel`)
- Schedule expressions (`daily at 6am`, `every business day`, `twice daily`)

---

## Plugins

### Executors

| Plugin | Key operations | Notes |
|--------|---------------|-------|
| `duckdb` | `load`, `query`, `validate`, `aggregate`, `transform` | In-process OLAP; good for validation and analytics |
| `postgres` | `query`, `load`, `execute` | Requires `psycopg2-binary` |
| `mysql` | `query`, `execute` | Requires `mysql-connector-python` |
| `snowflake` | `load_to_snowflake` | Requires `snowflake-connector-python` |
| `bigquery` | `query`, `load` | Requires `google-cloud-bigquery` |
| `api` | `GET`, `POST`, `PUT`, `DELETE` | HTTP/REST calls with retry support |
| `kafka` | `publish`, `subscribe` | Requires `kafka-python` |
| `spark` | `submit` | Requires `pyspark` |
| `python` | `execute_code`, `run_script` | Inline code or `.py` file |
| `shell` | `execute` | Shell command with timeout |
| `file` | `read`, `write`, `merge` | Local file operations |
| `email` | `send` | SMTP email notifications |

### Transformers

| Plugin | Operations | Notes |
|--------|-----------|-------|
| `dbt` | `run`, `test`, `compile`, `seed`, `snapshot`, `docs`, `ls` | Supports `--select` flag and `profiles_dir` |

### Plugin config examples

**DuckDB — validate**
```yaml
config:
  file_path: data/orders.csv
  checks:
    - name: positive_amounts
      sql: "SELECT COUNT(*) FROM data WHERE amount <= 0"
      expect: 0
```

**PostgreSQL — query**
```yaml
config:
  connection:
    host: localhost
    port: 5432
    database: mydb
    user: user
    password: "${POSTGRES_PASSWORD}"
  sql: "SELECT * FROM users WHERE active = true"
```

**API — GET with auth**
```yaml
config:
  method: GET
  url: https://api.example.com/orders
  headers:
    Authorization: "Bearer ${API_TOKEN}"
  params:
    page: 1
    limit: 1000
  retry_count: 3
```

**dbt — run with select**
```yaml
config:
  project_dir: dbt_project
  profiles_dir: ~/.dbt
  operation: run
  select: tag:daily
```

**Email notification**
```yaml
config:
  smtp_server: smtp.gmail.com
  smtp_port: 587
  sender_email: "${EMAIL_SENDER}"
  sender_password: "${EMAIL_PASSWORD}"
  recipients: [team@company.com]
  subject: "Pipeline complete"
  body: "Daily ETL finished successfully."
```

---

## REST API

All endpoints require a session cookie obtained from `POST /login` (except `/health`).

### Authentication

```bash
# Login
curl -c cookies.txt -X POST http://localhost:8000/login \
  -d "username=admin&password=changeme"

# Use session cookie in subsequent requests
curl -b cookies.txt http://localhost:8000/pipelines
```

### Endpoint reference

| Method | Endpoint | Role required | Description |
|--------|----------|--------------|-------------|
| `GET` | `/health` | — | Liveness check |
| `GET` | `/pipelines` | viewer | List all discovered pipelines |
| `GET` | `/pipeline-config?name=<n>` | viewer | Get raw YAML for a pipeline |
| `POST` | `/run` | editor | Run pipeline (async, returns run ID) |
| `POST` | `/run/sync` | editor | Run pipeline (blocking, returns result) |
| `POST` | `/validate` | editor | Validate config without running |
| `POST` | `/generate-pipeline` | editor | Generate YAML from text |
| `POST` | `/save-pipeline` | editor | Save generated YAML to disk |
| `GET` | `/status` | viewer | Execution status for all pipelines |
| `GET` | `/history/{name}` | viewer | Last N runs for a pipeline |
| `GET` | `/dag?name=<n>` | viewer | DAG structure (nodes + edges) |
| `GET` | `/dashboard-summary` | viewer | Aggregated stats |
| `POST` | `/schedule` | editor | Set cron schedule |
| `DELETE` | `/schedule/{name}` | editor | Remove schedule |
| `GET` | `/scheduled` | viewer | List all active schedules |
| `POST` | `/triggers` | editor | Create webhook/event trigger |
| `GET` | `/triggers` | viewer | List triggers |
| `DELETE` | `/triggers/{id}` | editor | Delete trigger |
| `POST` | `/triggers/webhook/{name}` | editor | Fire webhook trigger |
| `GET` | `/versions/{name}` | viewer | Pipeline version history |
| `GET` | `/versions/{name}/{id}` | viewer | Get specific version |
| `GET` | `/versions/{name}/{a}/diff/{b}` | viewer | Diff two versions |
| `GET` | `/lineage` | viewer | Full lineage graph |
| `GET` | `/lineage/asset` | viewer | Lineage for a specific asset |
| `GET` | `/catalog/assets` | viewer | All catalog assets |
| `GET` | `/catalog/pipelines` | viewer | Pipeline-level catalog entries |
| `GET` | `/quality/{name}` | viewer | Quality check results |
| `GET` | `/sla/violations` | viewer | SLA breach report |
| `GET` | `/metrics` | viewer | Execution metrics |
| `GET` | `/metrics/definitions` | viewer | Semantic metric definitions |
| `POST` | `/metrics/{name}/compute` | editor | Compute a semantic metric |
| `GET` | `/costs/summary` | viewer | Platform-wide cost summary |
| `GET` | `/costs/{name}` | viewer | Cost breakdown for a pipeline |
| `GET` | `/templates` | viewer | Available pipeline templates |
| `POST` | `/templates/{id}/use` | editor | Instantiate a template |
| `GET` | `/admin/users` | admin | List all users |
| `POST` | `/admin/users` | admin | Create user |
| `PATCH` | `/admin/users/{u}/role` | admin | Change user role |
| `DELETE` | `/admin/users/{u}` | admin | Delete user |
| `GET` | `/me` | viewer | Current user info |

---

## Access control

Three roles, each inheriting all permissions of the roles below it:

| Role | Permissions |
|------|-------------|
| `admin` | Everything — user management, all endpoints |
| `editor` | Run, schedule, validate, generate, and save pipelines |
| `viewer` | Read-only — list, status, history, DAG, metrics |

The admin account is bootstrapped from environment variables (`DATAPLATFORM_USERNAME` / `DATAPLATFORM_PASSWORD`) and always has admin role regardless of the user database.

---

## Pipeline templates

Four built-in templates are available in the `templates/` folder and through the `/templates-ui` page:

| Template | Description |
|----------|-------------|
| `etl_postgres_to_duckdb` | Extract from Postgres, validate, load to DuckDB |
| `dbt_run_and_test` | Run and test a dbt project |
| `api_ingest_and_validate` | Fetch from REST API, validate, store |
| `daily_python_etl` | Run a Python script on a daily schedule |

Instantiate via UI or API:

```bash
curl -b cookies.txt -X POST http://localhost:8000/templates/etl_postgres_to_duckdb/use \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "my_etl"}'
```

---

## Project structure

```
dataplatform-modern-dbt/
├── dataplatform/
│   ├── core/
│   │   ├── api.py               # FastAPI server — all routes and middleware
│   │   ├── config.py            # Pydantic pipeline/task config models
│   │   ├── dag.py               # NetworkX DAG builder + wave scheduler
│   │   ├── executor.py          # Task + pipeline execution engine (parallel)
│   │   ├── scheduler.py         # APScheduler cron integration
│   │   ├── auth.py              # JWT auth + RBAC
│   │   ├── database.py          # SQLite user store
│   │   ├── pipeline_generator.py# NLP entry point + legacy regex fallback
│   │   ├── nlp_generator.py     # NLP engine (50+ verb mappings)
│   │   ├── alerts.py            # Alert state management
│   │   ├── catalog.py           # Data asset catalog
│   │   ├── lineage.py           # Lineage tracking
│   │   ├── costs.py             # Cost attribution
│   │   ├── metrics.py           # Execution metrics
│   │   ├── semantic_metrics.py  # Business metric definitions
│   │   ├── quality.py           # Data quality checks
│   │   ├── secrets.py           # Secret management
│   │   ├── templates.py         # Template marketplace logic
│   │   ├── triggers.py          # Webhook + event triggers
│   │   ├── versioning.py        # Pipeline version control + diff
│   │   └── logging_config.py    # Centralized logging setup
│   ├── plugins/
│   │   ├── base.py              # BasePlugin interface
│   │   ├── registry.py          # Dynamic plugin loader
│   │   ├── executors/           # 12 executor plugins
│   │   └── transformers/        # dbt transformer
│   ├── static/                  # Web dashboard HTML pages (10 pages)
│   ├── cli/
│   │   └── main.py              # Typer CLI (dataplatform run / init)
│   └── templates/               # Jinja2 templates (internal)
├── pipelines/                   # Your pipeline YAML files (auto-discovered)
├── templates/                   # Reusable pipeline templates
├── data/                        # Runtime data (gitignored)
├── logs/                        # Log files (gitignored)
├── tests/                       # Pytest test suite (25+ test files)
├── requirements.txt
└── pyproject.toml
```

---

## Deployment

### Process manager (Linux/macOS)

```bash
# systemd — /etc/systemd/system/dataplatform.service
[Unit]
Description=Dataplatform API Server
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/opt/dataplatform-modern-dbt
EnvironmentFile=/opt/dataplatform-modern-dbt/.env
ExecStart=/usr/bin/python3 -m dataplatform.core.api
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable dataplatform
sudo systemctl start dataplatform
```

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "-m", "dataplatform.core.api"]
```

```bash
docker build -t dataplatform .
docker run -p 8000:8000 \
  -e DATAPLATFORM_USERNAME=admin \
  -e DATAPLATFORM_PASSWORD=changeme \
  -e DATAPLATFORM_SECRET_KEY=my-secret \
  -v $(pwd)/pipelines:/app/pipelines \
  -v $(pwd)/data:/app/data \
  dataplatform
```

### Nginx reverse proxy

```nginx
server {
    listen 80;
    server_name dataplatform.example.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 300s;
    }
}
```

### Environment variables reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATAPLATFORM_USERNAME` | Yes | — | Admin login username |
| `DATAPLATFORM_PASSWORD` | Yes | — | Admin login password |
| `DATAPLATFORM_SECRET_KEY` | Yes | — | JWT signing secret |
| `DATAPLATFORM_PORT` | No | `8000` | Server port |
| `DATAPLATFORM_HOST` | No | `0.0.0.0` | Bind address |
| `POSTGRES_PASSWORD` | No | — | Used in Postgres plugin configs |
| `SNOWFLAKE_USER` | No | — | Snowflake username |
| `SNOWFLAKE_PASSWORD` | No | — | Snowflake password |
| `MYSQL_PASSWORD` | No | — | MySQL password |
| `API_TOKEN` | No | — | Default Bearer token for API plugin |
| `EMAIL_SENDER` | No | — | SMTP sender email |
| `EMAIL_PASSWORD` | No | — | SMTP password |

---

## Writing a custom plugin

1. Create `dataplatform/plugins/executors/my_plugin.py`:

```python
from dataplatform.plugins.base import BasePlugin

class MyPlugin(BasePlugin):
    def execute(self, config: dict) -> tuple[bool, dict]:
        # config comes from the task's `config:` block in YAML
        try:
            result = do_something(config)
            return True, {"result": result}
        except Exception as e:
            return False, {"error": str(e)}
```

2. Register it in `dataplatform/plugins/registry.py`:

```python
"my_plugin": "dataplatform.plugins.executors.my_plugin.MyPlugin",
```

3. Use it in any pipeline:

```yaml
- name: my_task
  type: executor
  plugin: my_plugin
  config:
    my_key: my_value
```

---

## Running tests

```bash
pip install pytest pytest-asyncio
pytest tests/
```

The test suite covers: pipeline execution, DAG building, parallel executor, dbt plugin, config templates, auth/RBAC, API endpoints, catalog, lineage, costs, triggers, versioning, semantic metrics, and more.

---

## CLI

```bash
# Run a pipeline directly from the command line
dataplatform run pipelines/sample_pipeline.yaml

# Initialize a new project scaffold
dataplatform init my_project
```

---

## License

MIT
