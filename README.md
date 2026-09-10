# Dataplatform — Modern Data Orchestration

A self-hosted data orchestration platform built in Python. Define pipelines in YAML, run them via web UI or REST API, and manage everything from a single dashboard — without Airflow's complexity.

[![tests](https://github.com/saravanaprabhu1398-afk/dataplatform-modern-dbt/actions/workflows/tests.yml/badge.svg)](https://github.com/saravanaprabhu1398-afk/dataplatform-modern-dbt/actions/workflows/tests.yml)
![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue)
![Plugins: 13](https://img.shields.io/badge/Plugins-13-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-green)

---

## What it guarantees

Every number here was produced by running the code, not estimated. The script
that produces each one is named beside it.

**Exactly-once ingestion under process death.** Records and the offsets that
produced them commit in a single database transaction, under a fencing token,
so a crash before the commit replays and a crash after it moves on. There is no
window where one landed without the other.

| | duplicates | loss |
|---|---|---|
| at-least-once (write, then commit offsets) | 5.00% | 0 |
| at-most-once (commit offsets, then write) | 0 | 5.00% |
| this pipeline | **0** | **0** |

Killed with `SIGKILL` mid-transaction and again just after a commit, then
restarted: 10,000 of 10,000 records, zero duplicates, zero loss, offsets
agreeing with the data. Verified on both SQLite and PostgreSQL. Eight failure
injections run for real — seven handled, one recorded as a known limitation.
`demo/scripts/failure_matrix.py`

**Event time, not arrival time.** Windows are cut by when a record happened,
with a watermark, corrections for late arrivals, and a side output for records
past the allowed lateness. Nothing is dropped silently. Over 12,000 records with
1,443 arriving out of order:

| | windows correct | records misplaced |
|---|---|---|
| event time | 12 / 12 | 0 — 803 corrections, 58 set aside, all recorded |
| processing time | 0 / 12 | 530, plus 30 phantom buckets it never reports |

`demo/scripts/event_time_vs_processing_time.py`

**Multiple workers, safely.** Runs are claimed with `FOR UPDATE SKIP LOCKED`
and held under a lease with a monotonic fencing token, so a stalled worker
cannot report an outcome for the attempt that replaced it. Twelve workers
released simultaneously against twelve queued runs:

| claim | work picked up | idle workers |
|---|---|---|
| `SELECT` then conditional `UPDATE` | 3–5 of 12 | 7–9 |
| `FOR UPDATE SKIP LOCKED` | **12 of 12** | 0 |

`demo/scripts/claim_contention.py`

**Column-level lineage, enforced in CI.** SQL is parsed rather than
pattern-matched, and a change that stops producing a column fails the build with
the downstream columns it would break — including join keys, which decide which
rows exist rather than which values they take.

| lineage extraction | correct on a 28-query hand-checked corpus |
|---|---|
| regex | 11 / 28, every wrong answer silent |
| parser | **28 / 28** |

This is what the check says when it fires. One line was removed from a model —
`SUM(o.amount) AS gross` — and every test still passed:

```
comparing 5 model(s) against models at 9e183cd

ERROR   daily_revenue.gross: no longer produced, and 1 column(s) read it
           finance_export.revenue                 direct
warning customer_summary: 0 of 1 output columns fully resolved
           SELECT * over crm_extract, whose columns are not in the catalog

1 error(s), 1 warning(s)
```

Each finding is also emitted as a GitHub annotation against the model file, so
a reviewer reads it on the pull request instead of opening a job log:

```
::error file=demo/fixtures/models/daily_revenue.sql::daily_revenue.gross:
no longer produced, and 1 column(s) read it - finance_export.revenue direct
```

That output is from [#9](https://github.com/saravanaprabhu1398-afk/dataplatform-modern-dbt/pull/9),
a throwaway pull request opened to check that the check works. All three test
jobs passed on it; only `column lineage` failed.

`demo/scripts/lineage_parser_scorecard.py`

**Throughput.** 36,494 records/second, or 24,269 with event-time aggregation
enabled; p99 commit 29 ms and 47 ms at batch 1000. Measured over 12,000 records,
4 partitions, SQLite on local disk, with every run verified to have committed
every record. `demo/scripts/throughput.py`

What is deliberately **not** guaranteed — the sinks that cannot share the
transaction, clock skew, cross-partition ordering — is stated just as plainly in
[docs/STREAMING.md](docs/STREAMING.md).

### Reproduce any of it

```bash
dataplatform stream generate --out data/stream --keys 50 --per-key 200 --seed 7
dataplatform stream baseline --stream data/stream          # the naive consumers
dataplatform stream run --stream data/stream --table events_sink
dataplatform stream verify --stream data/stream --table events_sink

python demo/scripts/failure_matrix.py                      # eight injections
python demo/scripts/throughput.py
python demo/scripts/event_time_vs_processing_time.py

dataplatform lineage scan --models demo/fixtures/models --schema demo/fixtures/catalog.json
dataplatform lineage impact --column orders.amount         # what breaks if this changes
dataplatform lineage check --models demo/fixtures/models --git-ref main
```

Crash points can be injected anywhere in the streaming loop, and the exit is
`os._exit(137)` — no `finally` blocks, no flushes — because a clean shutdown
proves nothing:

```bash
DATAPLATFORM_CHAOS="after_write:3" python -m dataplatform.streaming.runner \
    --stream data/stream --table events_sink
```

---

## What it does

- **Run data pipelines** defined in YAML with task dependencies (DAG execution)
- **Ingest streams exactly-once** with watermarked event-time windowing, late-data
  corrections, and a verifier that checks a sink against a known-correct manifest
- **Execute across multiple workers** with lease-based claiming, heartbeats, fencing
  tokens, and automatic requeue of runs whose worker died
- **Trace lineage down to the column** with a SQL parser, and fail a pull request
  that would break a downstream column
- **Deploy pipelines** through a governed control plane with validation, targets, history, and rollback
- **Generate pipelines from plain English** using the built-in NLP generator
- **Monitor runs** in real time with a metrics collector, Grafana-style dashboard, alert rules, and incidents
- **Schedule pipelines** with cron expressions
- **Trigger pipelines** via webhooks or API events
- **Track costs and data quality** per pipeline and asset
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

## Production Runtime

For production-style deployments, run the API and worker separately:

```bash
export DATAPLATFORM_ENV=production
export DATAPLATFORM_EXECUTION_MODE=external
export DATAPLATFORM_USERNAME=platform-admin
export DATAPLATFORM_PASSWORD='use-a-real-secret'
export DATAPLATFORM_SESSION_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
export POSTGRES_URL='postgresql+psycopg2://user:password@host:5432/dataplatform'

dataplatform serve --host 0.0.0.0 --port 8000
dataplatform worker --poll-interval 2
```

`DATAPLATFORM_ENV=production` fails fast when unsafe defaults are used. In production, the app requires a strong session secret and external queue workers unless `DATAPLATFORM_ALLOW_EMBEDDED_WORKER=true` is explicitly set.

Docker Compose now starts PostgreSQL, the API, and a separate queue worker:

```bash
cp .env.example .env
# Set strong DATAPLATFORM_* and POSTGRES_* values before production use.
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

The production override requires `DATAPLATFORM_USERNAME`,
`DATAPLATFORM_PASSWORD`, `DATAPLATFORM_SESSION_SECRET`, `POSTGRES_DB`,
`POSTGRES_USER`, and `POSTGRES_PASSWORD`. Put them in `.env` or provide them
through the deployment platform; do not commit `.env`.

### Docker on another host

Build and publish the image to a registry, then run the same image with the
production environment variables and persistent mounts. The API and worker
must share the pipeline and data directories when using external execution:

```bash
docker build -t registry.example.com/dataplatform:VERSION .
docker push registry.example.com/dataplatform:VERSION
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

Use a reverse proxy or cloud load balancer for TLS. Do not expose PostgreSQL
publicly.

### Kubernetes

The baseline manifests in `deploy/kubernetes/` assume a registry image and a
storage class that supports `ReadWriteMany`, because the API and worker share
pipeline files and runtime data. Create the namespace, secret, and workloads:

```bash
kubectl create namespace dataplatform
kubectl -n dataplatform create secret generic dataplatform-secrets \
  --from-literal=DATAPLATFORM_USERNAME="$DATAPLATFORM_USERNAME" \
  --from-literal=DATAPLATFORM_PASSWORD="$DATAPLATFORM_PASSWORD" \
  --from-literal=DATAPLATFORM_SESSION_SECRET="$DATAPLATFORM_SESSION_SECRET" \
  --from-literal=POSTGRES_DB=dataplatform \
  --from-literal=POSTGRES_USER=dataplatform \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --from-literal=POSTGRES_URL="postgresql+psycopg2://dataplatform:${POSTGRES_PASSWORD}@postgres:5432/dataplatform"
kubectl -n dataplatform apply -k deploy/kubernetes
kubectl -n dataplatform rollout status statefulset/postgres
kubectl -n dataplatform rollout status deployment/dataplatform-api
kubectl -n dataplatform rollout status deployment/dataplatform-worker
```

Before applying, replace `dataplatform:local` in the two Deployment manifests
with the immutable image tag pushed to your registry. Expose the Service through
an Ingress or Gateway with TLS, authentication-aware network policy, and a
secret manager. For clusters without `ReadWriteMany`, use an external shared
pipeline store or package immutable pipeline YAML into the image and keep only
the metadata database on a PVC.

This baseline runs PostgreSQL inside the cluster for small installations. For
production workloads, use a managed PostgreSQL service when possible so
backups, replication, upgrades, and failover are handled outside the
application cluster.

---

## Web dashboard

| Page | URL | Purpose |
|------|-----|---------|
| Home | `/` | Workspace overview, quick links |
| Pipelines | `/dashboard` | DAG view, run status, execution history |
| Deployments | `/deployments` | Deployment validation, target selection, history, rollback |
| Generator | `/generator` | NLP pipeline builder |
| Catalog | `/catalog` | Data asset catalog |
| Lineage | `/lineage-viz` | Visual data lineage graph |
| Costs | `/costs` | Pipeline cost attribution |
| Templates | `/templates-ui` | Reusable pipeline templates |
| Alerts | `/alerts` | Incident management (Zenduty-style) |
| Monitoring | `/monitoring` | Metrics dashboard (Grafana-style) |
| Admin | `/admin` | User and role management |

---

## Observability and Alerting

The platform collects operational metrics from the metadata store and persists snapshots in `metric_samples`. Metrics include run volume, success/failure rate, queue depth, active/running runs, SLA violations, quality failure rate, P95 duration, and per-pipeline freshness.

- `GET /monitoring` shows live charts and collector status.
- `POST /observability/collect` stores a metric snapshot and evaluates alert rules.
- `GET /metrics` exposes Prometheus-compatible scrape output, including latest collected samples.
- `GET /alerts` manages alert rules and incidents.

The API starts an automatic collector by default. Tune it with:

```bash
export DATAPLATFORM_OBSERVABILITY_AUTO_COLLECT=true
export DATAPLATFORM_OBSERVABILITY_INTERVAL_SECONDS=60
export DATAPLATFORM_OBSERVABILITY_RANGE_HOURS=24
```

Default alert rules are seeded on startup unless `DATAPLATFORM_OBSERVABILITY_SEED_DEFAULT_RULES=false`. Built-ins cover queue depth, failure rate, P95 duration, SLA violations, quality failures, and stale pipeline activity.

For a dedicated collector process:

```bash
dataplatform metrics-collector --interval 60 --range-hours 24
dataplatform collect-metrics --range-hours 24
```

Alert rules support comparators such as `>`, `>=`, `<`, `<=`, `==`, and `!=`, optional pipeline scoping, wildcard pipeline scoping with `*`, severity, and email/webhook destinations. If a rule has no direct destination, the incident is routed to reusable notification channels matching the incident severity. Every delivery attempt is stored for audit and troubleshooting.

Shared notification routes can be managed from `/alerts` or via:

```bash
curl -b cookies.txt -X POST http://localhost:8000/notification-channels \
  -H "Content-Type: application/json" \
  -d '{"name":"Platform Slack","channel_type":"webhook","destination":"https://hooks.example.com/...","severities":["critical","warning"]}'
```

---

## Deployment Hub

`/deployments` provides a control plane for promoting pipeline YAML into a selected runtime profile, deployment target type, and connected target instance. It records the governed deployment intent: config snapshot hash, version ID, validation result, execution fabric manifest, selected cloud account/cluster/host, actor, active deployment, and rollback relationship.

Deployment records use the existing execution profiles and target types (`local`, `docker`, `kubernetes`, `cloud`, `on_prem`). The actual selectable destinations are stored as deployment connections, for example AWS/GCP/Azure cloud accounts, EKS/GKE/AKS/Kubernetes clusters, Docker hosts, or private on-prem sites. Successful deployments mark the previous active deployment for the same pipeline/profile/target/connection inactive; rollback restores the previous successful deployment for that same destination.

Register target instances from `/deployments` or via:

```bash
curl -b cookies.txt -X POST http://localhost:8000/deployment-connections \
  -H "Content-Type: application/json" \
  -d '{"connection_id":"eks-prod-a","name":"EKS Prod A","target_id":"kubernetes","provider":"eks","endpoint":"https://eks.example.com","region":"us-east-1","namespace":"data-platform","status":"connected","credentials_ref":"env:AWS_PROFILE"}'
```

```bash
curl -b cookies.txt -X POST http://localhost:8000/deployments/validate \
  -H "Content-Type: application/json" \
  -d '{"config_path":"pipelines/daily_orders.yaml","environment_profile":"prod","target_id":"kubernetes"}'

curl -b cookies.txt -X POST http://localhost:8000/deployments/deploy \
  -H "Content-Type: application/json" \
  -d '{"config_path":"pipelines/daily_orders.yaml","environment_profile":"prod","target_id":"kubernetes","connection_id":"eks-prod-a","notes":"release candidate"}'
```

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
| `execution_layer` | No | Platform layer: `ingest`, `quality`, `transform`, `serve`, or `operate` |
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

### Execution fabric

Pipelines can declare how they map onto the platform execution fabric. This keeps the same YAML portable across laptop, Docker, Kubernetes, cloud, or private/on-prem deployments.

```yaml
execution:
  profile: prod
  deployment_target: kubernetes
  default_layer: transform
  max_parallel_tasks: 8

tasks:
  - name: extract_orders
    type: executor
    plugin: postgres
    execution_layer: ingest

  - name: validate_orders
    type: executor
    plugin: duckdb
    operation: validate
    execution_layer: quality
    depends_on: [extract_orders]

  - name: build_marts
    type: transformer
    plugin: dbt
    execution_layer: transform
    depends_on: [validate_orders]

  - name: publish_marts
    type: executor
    plugin: snowflake
    execution_layer: serve
    depends_on: [build_marts]
```

Available layers are:

| Layer | Purpose |
|------|---------|
| `ingest` | Land files, API extracts, events, and database reads |
| `quality` | Validate shape, freshness, completeness, and business rules |
| `transform` | Prepare trusted datasets with SQL, dbt, Python, DuckDB, or Spark |
| `serve` | Publish curated outputs to warehouses, marts, APIs, or files |
| `operate` | Notify, audit, recover, and automate operational tasks |

Runtime profiles are exposed by `GET /execution-fabric` and `GET /environment-profiles`. Built-in profiles include `local`, `dev`, `prod`, `docker`, `kubernetes`, `cloud`, and `on_prem`.

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

All endpoints require a session cookie obtained from `POST /login` except `/health` and the Prometheus scrape endpoint `/metrics`.

### Authentication

```bash
# Login
curl -c cookies.txt -X POST http://localhost:8000/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"changeme"}'

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
| `GET` | `/deployment-connections` | viewer | List connected deployment targets |
| `POST` | `/deployment-connections` | editor | Register cloud account, cluster, host, or site |
| `PATCH` | `/deployment-connections/{id}` | editor | Update deployment target instance |
| `DELETE` | `/deployment-connections/{id}` | editor | Delete deployment target instance |
| `POST` | `/deployments/validate` | editor | Validate a pipeline for a deployment profile/target |
| `POST` | `/deployments/deploy` | editor | Create an active deployment record |
| `GET` | `/deployments/records` | viewer | Deployment history |
| `GET` | `/deployments/records/{id}` | viewer | Deployment detail |
| `POST` | `/deployments/records/{id}/rollback` | editor | Roll back to previous successful deployment |
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
| `GET` | `/metrics` | — | Prometheus-compatible execution and collected metrics |
| `GET` | `/metrics/catalog` | viewer | Operational metric catalog |
| `GET` | `/metrics/timeseries` | viewer | Dashboard time-series data |
| `GET` | `/metrics/definitions` | viewer | Semantic metric definitions |
| `POST` | `/metrics/{name}/compute` | editor | Compute a semantic metric |
| `GET` | `/observability/dashboard` | viewer | Live operational metric and alert dashboard payload |
| `POST` | `/observability/collect` | viewer | Collect/store operational metric samples and evaluate rules |
| `GET` | `/alert-rules` | viewer | List alert rules |
| `POST` | `/alert-rules` | editor | Create alert rule |
| `PATCH` | `/alert-rules/{id}` | editor | Update alert rule |
| `DELETE` | `/alert-rules/{id}` | editor | Delete alert rule |
| `GET` | `/alerts/incidents` | viewer | List alert incidents |
| `POST` | `/alerts/incidents/{id}/ack` | editor | Acknowledge incident |
| `POST` | `/alerts/incidents/{id}/resolve` | editor | Resolve incident |
| `GET` | `/notification-channels` | viewer | List shared alert routes |
| `POST` | `/notification-channels` | editor | Create shared alert route |
| `PATCH` | `/notification-channels/{id}` | editor | Update shared alert route |
| `DELETE` | `/notification-channels/{id}` | editor | Delete shared alert route |
| `GET` | `/notification-deliveries` | viewer | Alert notification delivery history |
| `GET` | `/costs/summary` | viewer | Platform-wide cost summary |
| `GET` | `/costs/{name}` | viewer | Cost breakdown for a pipeline |
| `GET` | `/templates` | viewer | Available pipeline templates |
| `POST` | `/templates/{id}/use` | editor | Instantiate a template |
| `GET` | `/git/remotes` | viewer | List registered Git remotes |
| `POST` | `/git/remotes` | editor | Register a Git remote |
| `DELETE` | `/git/remotes/{id}` | admin | Remove a Git remote and local clone |
| `POST` | `/git/remotes/{id}/test` | viewer | Test remote Git connectivity |
| `GET` | `/git/remotes/{id}/workspace/tree` | viewer | Browse repository files |
| `GET` | `/git/remotes/{id}/workspace/file?path=<p>` | viewer | Read a repository file |
| `PUT` | `/git/remotes/{id}/workspace/file?path=<p>` | editor | Save a repository file |
| `GET` | `/git/remotes/{id}/workspace/status` | viewer | Git branch, ahead/behind, and changes |
| `GET` | `/git/remotes/{id}/workspace/diff` | viewer | Unified diff for repo or file |
| `POST` | `/git/remotes/{id}/workspace/pull` | editor | Pull configured branch when clean |
| `POST` | `/git/remotes/{id}/workspace/commit` | editor | Commit selected or all workspace changes |
| `POST` | `/git/remotes/{id}/workspace/push` | editor | Push committed workspace changes |
| `GET` | `/admin/users` | admin | List all users |
| `POST` | `/admin/users` | admin | Create user |
| `PATCH` | `/admin/users/{u}/role` | admin | Change user role |
| `DELETE` | `/admin/users/{u}` | admin | Delete user |
| `GET` | `/me` | viewer | Current user info |

---

### Git workspace configuration

The `/git-integration` UI is a full repository workspace: select a remote, browse the cloned tree, edit UTF-8 text files, inspect diffs, commit selected files, pull, and push. Remote definitions live in the metadata DB table `git_remotes`; local clones live under `GIT_CLONES_PATH` (default `data/git-clones`). Set `GIT_WORKSPACE_MAX_FILE_BYTES` to control the largest file editable through the browser (default `1048576`, 1 MiB).

HTTPS token, SSH, and local/no-auth remotes are supported by the same registered remote model. SSH access uses the keys and known-hosts configuration available to the running server process.

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
│   │   ├── database.py          # SQLite/PostgreSQL metadata store
│   │   ├── pipeline_generator.py# NLP entry point + legacy regex fallback
│   │   ├── nlp_generator.py     # NLP engine (50+ verb mappings)
│   │   ├── alerts.py            # Email/webhook alert delivery
│   │   ├── observability.py     # Metric collection, alert rules, incidents
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
