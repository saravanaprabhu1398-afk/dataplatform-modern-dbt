# DataPlatform — Architecture & Deployment Guide

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [Current Architecture](#2-current-architecture)
3. [Request & Execution Flows](#3-request--execution-flows)
4. [Prerequisites](#4-prerequisites)
5. [Deployment — Local Development](#5-deployment--local-development)
6. [Deployment — Docker (single container)](#6-deployment--docker-single-container)
7. [Deployment — Docker Compose (production-ready)](#7-deployment--docker-compose-production-ready)
8. [Deployment — Kubernetes](#8-deployment--kubernetes)
9. [Deployment — Cloud Native](#9-deployment--cloud-native)
10. [Deployment — On-Premises / Air-Gapped](#10-deployment--on-premises--air-gapped)
11. [Environment Variables Reference](#11-environment-variables-reference)
12. [Multi-Architecture Roadmap](#12-multi-architecture-roadmap)

---

## 1. What This System Does

DataPlatform is a self-hosted pipeline orchestration platform. It lets teams define, schedule, run, monitor, and govern data pipelines using YAML config files — without needing Airflow, dbt Cloud, or a managed data platform subscription.

### Core Capabilities

| Area | What it does |
|---|---|
| **Pipeline execution** | Parallel DAG execution with wave-based scheduling; task-level retries and timeouts |
| **Plugin system** | DuckDB SQL, Python (exec), PostgreSQL COPY, Snowflake, Kafka, Spark — dynamically loaded |
| **Scheduling** | Cron-based scheduling via APScheduler; schedule state persisted in DB |
| **Run queue** | Persistent `pipeline_queue` table — runs survive server restarts; orphan recovery on startup |
| **Real-time logs** | Per-run log files streamed via Server-Sent Events at `GET /run/{run_id}/logs/stream` |
| **Git integration** | Push/pull pipelines to GitHub/GitLab/Bitbucket; full push history |
| **Secrets** | Env-var interpolation (`${MY_VAR}`) + HashiCorp Vault KV v1/v2 via hvac |
| **Lineage** | Auto-captured from DuckDB SQL (FROM/JOIN/INSERT INTO); manual declaration in YAML |
| **Data quality** | SQL-based checks on task output; pass/fail history per pipeline |
| **Audit log** | Immutable event log for login, run, user, config changes |
| **Cost tracking** | Compute-unit estimates per run, per team, per pipeline |
| **Data catalog** | Searchable asset catalog across all pipelines |
| **Templates** | Pipeline template marketplace with one-click reuse |
| **Versioning** | Hash-based pipeline YAML versioning with diff view |
| **SLA monitoring** | Per-pipeline SLA limits with webhook/email alerts on breach |
| **Error handlers** | Webhook/email dispatch on pipeline or task failure |
| **LLM pipeline gen** | Natural language → pipeline YAML via Anthropic/OpenAI, regex fallback |
| **Prometheus metrics** | `/metrics` endpoint for Grafana scraping |
| **RBAC** | Admin / Editor / Viewer roles; bcrypt password hashing |
| **UI** | Web interface: Job Builder, pipeline catalog, lineage graph, run history |

---

## 2. Current Architecture

### High-Level System Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                             │
│                                                                 │
│   Browser (Job Builder UI)    API consumers (CI, scripts)       │
│         │                              │                        │
└─────────┼──────────────────────────────┼────────────────────────┘
          │  HTTP / SSE                  │  REST API
          ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API LAYER  (FastAPI)                       │
│                                                                 │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │  Auth /    │  │ Pipeline │  │ Lineage  │  │  Git /      │  │
│  │  RBAC      │  │ CRUD     │  │ Quality  │  │  Versioning │  │
│  └────────────┘  └──────────┘  └──────────┘  └─────────────┘  │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │  /run      │  │ /queue   │  │ /metrics │  │  /generate  │  │
│  │  /schedule │  │ SSE logs │  │ /catalog │  │  (LLM/NLP)  │  │
│  └────────────┘  └──────────┘  └──────────┘  └─────────────┘  │
│                                                                 │
└───────────┬───────────────────────────┬────────────────────────┘
            │  submit(run_id, fn)        │  read/write
            ▼                            ▼
┌───────────────────────┐    ┌───────────────────────────────────┐
│   WORKER POOL         │    │         STORAGE LAYER             │
│   (ThreadPoolExecutor)│    │                                   │
│                       │    │  ┌───────────────┐                │
│   max 4 concurrent    │    │  │  SQLite  /    │                │
│   pipelines (default) │    │  │  PostgreSQL   │                │
│                       │    │  │               │                │
│   ┌─────────────────┐ │    │  │  • pipeline_runs               │
│   │ execute_pipeline│ │    │  │  • pipeline_queue              │
│   │ _background()   │ │    │  │  • users                       │
│   │                 │ │    │  │  • audit_log                   │
│   │  DAGBuilder     │ │    │  │  • scheduler_schedules         │
│   │  ↓ waves        │ │    │  │  • lineage_records             │
│   │  PipelineExecutor│ │   │  │  • quality_results             │
│   │  ↓ parallel     │ │    │  │  • sla_violations              │
│   │  TaskExecutor   │ │    │  │  • pipeline_versions           │
│   │  ↓ per task     │ │    │  │  • pipeline_costs              │
│   │  Plugin (DuckDB │ │    │  │  • git_remotes / push_log      │
│   │  Python, PG,    │ │    │  │  • metric_results              │
│   │  Snowflake...)  │ │    │  └───────────────┘                │
│   └─────────────────┘ │    │                                   │
│                       │    │  ┌───────────────┐                │
│   writes per-run log  │    │  │  Filesystem   │                │
│   logs/runs/{id}.log  │    │  │               │                │
└───────────────────────┘    │  │  pipelines/   │                │
                             │  │  logs/runs/   │                │
┌──────────────────────┐     │  │  data/git-    │                │
│   SCHEDULER          │     │  │  clones/      │                │
│   (APScheduler)      │     │  └───────────────┘                │
│                      │     │                                   │
│   cron triggers      │     │  ┌───────────────┐                │
│   pipeline_completion│     │  │  Vault (opt.) │                │
│   file_sensor        │     │  │  hvac KV v1/2 │                │
│   state in DB        │     │  └───────────────┘                │
└──────────────────────┘     └───────────────────────────────────┘
```

### Plugin Architecture

```
TaskExecutor
     │
     ├── type: executor
     │       ├── plugin: python     → PythonPlugin   (exec() in sandbox)
     │       ├── plugin: duckdb     → DuckDBPlugin   (SQL + auto-lineage)
     │       ├── plugin: postgres   → PostgresPlugin (COPY + psycopg2.sql)
     │       ├── plugin: snowflake  → SnowflakePlugin
     │       ├── plugin: spark      → SparkPlugin
     │       └── plugin: <custom>   → dynamic import via importlib
     │
     └── type: transformer
             └── plugin: <custom>   → TransformerPlugin subclass
```

### Pipeline Run Lifecycle

```
POST /run
    │
    ├── load_config() → validate YAML (Pydantic v2, extra="forbid")
    ├── DAGBuilder.build() → detect cycles (networkx.find_cycle)
    ├── save_run_status(..., "queued")
    ├── enqueue_run() → pipeline_queue table
    ├── append_audit_event("run_queued")
    └── worker_pool.submit(run_id, execute_pipeline_background)
                │
                └── [Worker Thread]
                        ├── set_run_status_in_queue("running")
                        ├── get_execution_waves() → [[wave1_tasks], [wave2_tasks], ...]
                        ├── For each wave: ThreadPoolExecutor → TaskExecutor.run()
                        │       └── Plugin.execute() → success, output
                        ├── check_sla_and_alert()
                        ├── record_run_cost()
                        ├── set_run_status_in_queue("completed" | "failed")
                        ├── _dispatch_error_handlers() if failed
                        └── notify_pipeline_completed() → trigger downstream
```

### Authentication Flow

```
Browser                    FastAPI                      DB
   │                          │                          │
   ├─ POST /login ──────────► │                          │
   │   {user, pass}           ├─ verify_user() ─────────►│
   │                          │◄─ bcrypt compare ────────│
   │◄─ Set-Cookie: session ───│                          │
   │   (HMAC-signed payload)  │                          │
   │                          │                          │
   ├─ GET /pipelines ────────►│                          │
   │   Cookie: session        ├─ _read_session_cookie()  │
   │                          │  verify HMAC + expiry    │
   │                          ├─ _require_permission()   │
   │◄─ 200 JSON ──────────────│  role check              │
```

---

## 3. Request & Execution Flows

### Pipeline Execution with SSE Log Streaming

```
Client                        API                      Worker Thread
  │                            │                            │
  ├─ POST /run ───────────────►│                            │
  │◄─ {run_id, status:queued} ─│                            │
  │                            │── submit(run_id, fn) ─────►│
  │                            │                            ├─ write logs/runs/{id}.log
  ├─ GET /run/{id}/logs/stream►│                            │
  │◄══ SSE stream ════════════►│◄── tail log file (0.5s) ───│
  │   data: INFO starting...   │                            ├─ execute tasks
  │   data: INFO wave 1...     │                            ├─ write task output to log
  │   data: INFO completed     │                            │
  │   data: [STREAM_END]       │◄── run finished ───────────│
  │                            │
  ├─ GET /run/{id}/status ─────►│
  │◄─ {status: completed} ─────│
```

### Secret Resolution in Task Config

```
YAML config:
  config:
    password: "${DB_PASSWORD}"
    api_key:  "${vault:secret/myapp:api_key}"

resolve_secrets(task.config)
    │
    ├─ ${DB_PASSWORD}          → os.environ["DB_PASSWORD"] = "s3cr3t"
    │
    └─ ${vault:secret/myapp:api_key}
            │
            ├─ _get_vault_client()
            │      ├─ VAULT_TOKEN → token auth
            │      └─ VAULT_ROLE_ID + VAULT_SECRET_ID → AppRole auth
            ├─ kv.v2.read_secret_version(path="myapp", mount="secret")
            └─ returns secret["data"]["data"]["api_key"]
```

---

## 4. Prerequisites

### All Environments

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.9 – 3.12 | 3.11 recommended |
| pip | 23+ | `pip install --upgrade pip` |
| git | 2.x | Required for Git integration |

### Local Development only

Nothing extra — SQLite is built into Python.

### Docker deployments

| Requirement | Version |
|---|---|
| Docker | 24+ |
| Docker Compose | v2 (`docker compose`, not `docker-compose`) |

### Kubernetes

| Requirement | Notes |
|---|---|
| kubectl 1.28+ | Configured against your cluster |
| Helm 3.12+ | For chart-based deploy |
| A running cluster | EKS, GKE, AKS, k3s, or Minikube |

### Optional integrations

| Feature | Extra requirement |
|---|---|
| PostgreSQL backend | `psycopg2-binary` (already in requirements.txt) + Postgres 14+ server |
| Vault secrets | `pip install hvac` + Vault 1.12+ server |
| LLM pipeline gen | `pip install anthropic` and/or `pip install openai` + API key |
| Snowflake plugin | Already in requirements.txt + Snowflake account |
| Spark plugin | `pyspark` already in requirements.txt + Java 11+ |
| Email alerts | SMTP server (Gmail app password works) |

---

## 5. Deployment — Local Development

**Best for:** developing pipelines, testing locally.

### Step 1 — Clone and install

```bash
git clone <your-repo-url>
cd dataplatform-modern-dbt

# Create a virtual environment (recommended)
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -e .                   # installs the dataplatform package
```

### Step 2 — Configure environment

```bash
# Copy the example env file
cp .env.example .env

# Minimum required changes in .env:
DATAPLATFORM_USERNAME=admin
DATAPLATFORM_PASSWORD=changeme
DATAPLATFORM_SESSION_SECRET=any-random-32-char-string
```

### Step 3 — Start the server

```bash
python3 -m dataplatform.cli.main serve
# or
./start-server.sh
```

Server starts at **http://localhost:8000**. SQLite DB is created automatically at `data/platform.db`.

### Step 4 — Verify

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

Open **http://localhost:8000** in your browser → Job Builder UI.

### Run tests

```bash
python3 -m pytest tests/ -q
```

---

## 6. Deployment — Docker (single container)

**Best for:** demos, staging, single-server production with low traffic.

### Step 1 — Build the image

```bash
docker build -t dataplatform:latest .
```

### Step 2 — Run

```bash
docker run -d \
  --name dataplatform \
  -p 8000:8000 \
  -e DATAPLATFORM_USERNAME=admin \
  -e DATAPLATFORM_PASSWORD=changeme \
  -e DATAPLATFORM_SESSION_SECRET=$(openssl rand -hex 32) \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  -v $(pwd)/pipelines:/app/pipelines \
  --restart unless-stopped \
  dataplatform:latest
```

### Step 3 — Verify

```bash
docker ps                              # container should be Up
curl http://localhost:8000/health      # {"status":"ok"}
docker logs dataplatform               # check startup logs
```

### Persistent data

All state lives in the volumes:
- `./data/platform.db` — SQLite database (runs, users, schedules)
- `./logs/` — application and per-run logs
- `./pipelines/` — pipeline YAML files

Back these up before upgrades.

### Upgrade

```bash
docker build -t dataplatform:latest .
docker stop dataplatform && docker rm dataplatform
# re-run the docker run command above
```

---

## 7. Deployment — Docker Compose (production-ready)

**Best for:** team use, production with PostgreSQL, persistent state across deploys.

### Step 1 — Create `.env`

```bash
cat > .env << 'EOF'
DATAPLATFORM_USERNAME=admin
DATAPLATFORM_PASSWORD=changeme_in_prod
DATAPLATFORM_SESSION_SECRET=change_this_to_a_random_32char_string
DATAPLATFORM_PORT=8000
LOG_LEVEL=INFO
LOG_JSON=false
EOF
```

### Step 2 — Start

```bash
docker compose up -d
```

### Step 3 — Verify

```bash
docker compose ps            # both services should be healthy
docker compose logs -f       # follow logs
curl http://localhost:8000/health
```

### Step 4 (optional) — Add PostgreSQL

Create `docker-compose.override.yml`:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: dataplatform
      POSTGRES_USER: dpflow
      POSTGRES_PASSWORD: ${DB_PASSWORD:-dpflow_dev}
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "dpflow"]
      interval: 10s
      retries: 5

  dataplatform:
    environment:
      POSTGRES_URL: postgresql://dpflow:${DB_PASSWORD:-dpflow_dev}@postgres:5432/dataplatform
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pg_data:
```

```bash
echo "DB_PASSWORD=$(openssl rand -hex 16)" >> .env
docker compose up -d
```

The app will use PostgreSQL automatically when `POSTGRES_URL` is set.

### Step 5 (optional) — Add NGINX + TLS

Create `nginx.conf`:

```nginx
upstream api { server dataplatform:8000; }

server {
    listen 443 ssl;
    server_name your.domain.com;

    ssl_certificate     /etc/ssl/certs/cert.pem;
    ssl_certificate_key /etc/ssl/private/key.pem;

    # SSE log streaming — must disable buffering
    location ~ ^/run/.*/logs/stream$ {
        proxy_pass http://api;
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 3600s;
        proxy_set_header Connection '';
        proxy_http_version 1.1;
    }

    location / {
        proxy_pass http://api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

server {
    listen 80;
    server_name your.domain.com;
    return 301 https://$host$request_uri;
}
```

Add to `docker-compose.override.yml`:

```yaml
services:
  nginx:
    image: nginx:alpine
    ports: ["80:80", "443:443"]
    volumes:
      - ./nginx.conf:/etc/nginx/conf.d/default.conf
      - ./certs:/etc/ssl
    depends_on: [dataplatform]
```

### Upgrade without downtime

```bash
docker compose build
docker compose up -d --no-deps dataplatform   # rolling restart of app only
```

---

## 8. Deployment — Kubernetes

**Best for:** multiple teams, high availability, autoscaling.

### Prerequisites

```bash
kubectl version --client   # 1.28+
helm version               # 3.12+
kubectl cluster-info       # confirm cluster is reachable
```

### Step 1 — Create namespace and secrets

```bash
kubectl create namespace dataplatform

kubectl create secret generic platform-secrets \
  --namespace dataplatform \
  --from-literal=session-secret=$(openssl rand -hex 32) \
  --from-literal=admin-password=changeme \
  --from-literal=db-password=$(openssl rand -hex 16)
```

### Step 2 — Push image to registry

```bash
docker build -t your-registry.io/dataplatform:v1.0.0 .
docker push your-registry.io/dataplatform:v1.0.0
```

### Step 3 — Create deployment manifests

`k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dataplatform-api
  namespace: dataplatform
spec:
  replicas: 2
  selector:
    matchLabels: { app: dataplatform-api }
  template:
    metadata:
      labels: { app: dataplatform-api }
    spec:
      containers:
        - name: api
          image: your-registry.io/dataplatform:v1.0.0
          ports: [{ containerPort: 8000 }]
          env:
            - name: DATAPLATFORM_SESSION_SECRET
              valueFrom:
                secretKeyRef: { name: platform-secrets, key: session-secret }
            - name: DATAPLATFORM_PASSWORD
              valueFrom:
                secretKeyRef: { name: platform-secrets, key: admin-password }
            - name: POSTGRES_URL
              value: postgresql://dpflow:$(DB_PASSWORD)@postgres-svc:5432/dataplatform
            - name: DATABASE_PATH
              value: /app/data/platform.db
            - name: PIPELINE_WORKERS
              value: "4"
          volumeMounts:
            - name: pipelines
              mountPath: /app/pipelines
            - name: data
              mountPath: /app/data
          livenessProbe:
            httpGet: { path: /health, port: 8000 }
            initialDelaySeconds: 15
            periodSeconds: 30
          readinessProbe:
            httpGet: { path: /health, port: 8000 }
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: pipelines
          persistentVolumeClaim: { claimName: pipelines-pvc }
        - name: data
          persistentVolumeClaim: { claimName: data-pvc }
---
apiVersion: v1
kind: Service
metadata:
  name: dataplatform-svc
  namespace: dataplatform
spec:
  selector: { app: dataplatform-api }
  ports: [{ port: 80, targetPort: 8000 }]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: dataplatform-ingress
  namespace: dataplatform
  annotations:
    nginx.ingress.kubernetes.io/proxy-buffering: "off"     # Required for SSE
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
spec:
  rules:
    - host: dataplatform.your.domain.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service: { name: dataplatform-svc, port: { number: 80 } }
```

`k8s/pvc.yaml`:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: pipelines-pvc, namespace: dataplatform }
spec:
  accessModes: [ReadWriteMany]   # RWX for multi-pod access
  resources: { requests: { storage: 10Gi } }
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: data-pvc, namespace: dataplatform }
spec:
  accessModes: [ReadWriteOnce]
  resources: { requests: { storage: 20Gi } }
```

### Step 4 — Deploy

```bash
kubectl apply -f k8s/pvc.yaml
kubectl apply -f k8s/deployment.yaml

# Watch rollout
kubectl rollout status deployment/dataplatform-api -n dataplatform
```

### Step 5 — Verify

```bash
kubectl get pods -n dataplatform
kubectl logs -n dataplatform -l app=dataplatform-api --tail=50
kubectl port-forward svc/dataplatform-svc 8080:80 -n dataplatform
curl http://localhost:8080/health
```

### Autoscaling by queue depth

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata: { name: dataplatform-hpa, namespace: dataplatform }
spec:
  scaleTargetRef: { apiVersion: apps/v1, kind: Deployment, name: dataplatform-api }
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource: { name: cpu, target: { type: Utilization, averageUtilization: 70 } }
```

---

## 9. Deployment — Cloud Native

### AWS (ECS Fargate)

```
┌─────────────┐    ┌──────────┐    ┌─────────────────────┐
│  Route 53   │───►│   ALB    │───►│  ECS Fargate         │
│  (DNS)      │    │(port 443)│    │                      │
└─────────────┘    └──────────┘    │  Task: dataplatform  │
                                   │  (api + worker)      │
                                   └──────────┬───────────┘
                                              │
                   ┌──────────────────────────┼──────────────┐
                   │                          │              │
                   ▼                          ▼              ▼
            ┌─────────────┐          ┌──────────────┐  ┌─────────┐
            │  RDS Aurora │          │     S3       │  │ Secrets │
            │  PostgreSQL │          │  (pipelines, │  │ Manager │
            └─────────────┘          │   logs)      │  └─────────┘
                                     └──────────────┘
```

**Deploy steps:**
```bash
# 1. Push image to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.<region>.amazonaws.com
docker build -t dataplatform .
docker tag dataplatform:latest <account>.dkr.ecr.<region>.amazonaws.com/dataplatform:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/dataplatform:latest

# 2. Set secrets in AWS Secrets Manager
aws secretsmanager create-secret --name /dataplatform/session-secret \
  --secret-string $(openssl rand -hex 32)

# 3. Create ECS task definition referencing the ECR image and secrets
# 4. Create ECS Service behind ALB
# 5. Set POSTGRES_URL to RDS endpoint in task environment
```

**Required env vars for AWS:**
```
POSTGRES_URL=postgresql://dpflow:<pass>@<rds-endpoint>:5432/dataplatform
DATAPLATFORM_SESSION_SECRET=<from Secrets Manager>
AWS_DEFAULT_REGION=us-east-1
```

---

### GCP (Cloud Run)

```bash
# 1. Build and push to Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev
docker build -t us-central1-docker.pkg.dev/<project>/dataplatform/app:latest .
docker push us-central1-docker.pkg.dev/<project>/dataplatform/app:latest

# 2. Deploy to Cloud Run
gcloud run deploy dataplatform \
  --image us-central1-docker.pkg.dev/<project>/dataplatform/app:latest \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --port 8000 \
  --set-env-vars POSTGRES_URL=postgresql://dpflow:<pass>@/<db>?host=/cloudsql/<project>:<region>:<instance> \
  --add-cloudsql-instances <project>:<region>:<instance> \
  --set-secrets DATAPLATFORM_SESSION_SECRET=dataplatform-session-secret:latest
```

---

### Azure (Container Apps)

```bash
# 1. Push to ACR
az acr login --name <acr-name>
docker build -t <acr-name>.azurecr.io/dataplatform:latest .
docker push <acr-name>.azurecr.io/dataplatform:latest

# 2. Deploy Container App
az containerapp create \
  --name dataplatform \
  --resource-group <rg> \
  --environment <env-name> \
  --image <acr-name>.azurecr.io/dataplatform:latest \
  --target-port 8000 \
  --ingress external \
  --registry-server <acr-name>.azurecr.io \
  --env-vars \
    POSTGRES_URL=postgresql://dpflow:<pass>@<pg-host>:5432/dataplatform \
    DATAPLATFORM_SESSION_SECRET=secretref:session-secret
```

---

## 10. Deployment — On-Premises / Air-Gapped

**For environments with no internet access.**

### Step 1 — Export the image on an internet-connected machine

```bash
docker build -t dataplatform:latest .
docker save dataplatform:latest | gzip > dataplatform.tar.gz
# Transfer dataplatform.tar.gz to the air-gapped server via USB/SCP
```

### Step 2 — Load on the air-gapped server

```bash
gunzip -c dataplatform.tar.gz | docker load
# Verify
docker images | grep dataplatform
```

### Step 3 — Run with internal services

```yaml
# docker-compose.yml (air-gapped)
services:
  dataplatform:
    image: dataplatform:latest   # no build: needed — loaded from tar
    ports: ["8000:8000"]
    environment:
      POSTGRES_URL: postgresql://dpflow:${DB_PASSWORD}@postgres:5432/dataplatform
      DATAPLATFORM_SESSION_SECRET: ${SESSION_SECRET}
      VAULT_ADDR: http://vault:8200          # internal Vault
      VAULT_TOKEN: ${VAULT_TOKEN}
    volumes:
      - dp_data:/app/data
      - dp_pipelines:/app/pipelines
      - dp_logs:/app/logs
    depends_on: { postgres: { condition: service_healthy } }

  postgres:
    image: postgres:16-alpine    # also loaded from tar
    environment:
      POSTGRES_DB: dataplatform
      POSTGRES_USER: dpflow
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes: [pg_data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "dpflow"]
      interval: 10s

volumes:
  dp_data:
  pg_data:
  dp_pipelines:
  dp_logs:
```

```bash
docker compose up -d
```

---

## 11. Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `DATAPLATFORM_USERNAME` | `admin` | Admin login username |
| `DATAPLATFORM_PASSWORD` | `admin` | Admin login password (change in production) |
| `DATAPLATFORM_SESSION_SECRET` | `dpflow-dev-secret-change-me` | HMAC signing key for session cookies |
| `DATAPLATFORM_PORT` | `8000` | Port the server listens on |
| `DATABASE_PATH` | `data/platform.db` | SQLite database path (ignored if POSTGRES_URL set) |
| `POSTGRES_URL` | _(not set)_ | If set, uses PostgreSQL instead of SQLite |
| `PIPELINE_WORKERS` | `4` | Max concurrent pipeline runs in the worker pool |
| `DATA_DIR` | `data` | Directory for scheduler state and other data files |
| `PIPELINES_PATH` | `pipelines` | Directory where pipeline YAML files are stored |
| `GIT_CLONES_PATH` | `data/git-clones` | Directory for cloned Git repositories |
| `LOG_LEVEL` | `INFO` | Logging level: DEBUG, INFO, WARNING, ERROR |
| `LOG_FILE` | `logs/pipeline.log` | Application log file path |
| `LOG_JSON` | `false` | Set to `true` for JSON-structured logging |
| `VAULT_ADDR` | _(not set)_ | HashiCorp Vault server URL (e.g. `http://vault:8200`) |
| `VAULT_TOKEN` | _(not set)_ | Vault token for token auth |
| `VAULT_ROLE_ID` | _(not set)_ | Vault AppRole role ID (alternative to token) |
| `VAULT_SECRET_ID` | _(not set)_ | Vault AppRole secret ID (alternative to token) |
| `ANTHROPIC_API_KEY` | _(not set)_ | Enables LLM pipeline generation via Claude |
| `OPENAI_API_KEY` | _(not set)_ | Enables LLM pipeline generation via GPT |
| `ALERT_SMTP_SERVER` | `smtp.gmail.com` | SMTP server for email alerts |
| `ALERT_SMTP_PORT` | `587` | SMTP port |
| `ALERT_SMTP_USER` | _(not set)_ | SMTP sender email address |
| `ALERT_SMTP_PASSWORD` | _(not set)_ | SMTP password or app password |

---

## 12. Multi-Architecture Roadmap

### Migration Sequence

```
NOW                         T+3 months              T+9 months             T+18 months
────────────────────────    ─────────────────────   ──────────────────     ───────────────────
Single FastAPI process   →  Postgres + separate  →  Kubernetes + Helm  →   Cloud-native or
SQLite storage              worker process          HPA autoscaling        on-prem full stack
Docker single container     Docker Compose          Vault wired            Keycloak SSO
                            NGINX + TLS             Alembic migrations     MinIO object store
                                                    Pipeline YAML in DB    Loki log aggregation
```

### What still needs to be built for Tier 1

| Item | Effort | Unlocks |
|---|---|---|
| `dataplatform/cli/worker.py` — separate worker process | 1–2 days | API and execution fully decoupled |
| Alembic migrations (`alembic init`) | 1 day | Safe schema upgrades in prod |
| PostgreSQL in `docker-compose.yml` | 2 hours | Persistent multi-writer storage |
| NGINX config with SSE headers | 2 hours | TLS termination, SSE through proxy |
| Prometheus `pipeline_queue_depth` metric | 2 hours | Grafana dashboard, HPA trigger |

### What still needs to be built for Tier 2 (Kubernetes)

| Item | Effort |
|---|---|
| Helm chart (`helm/dataplatform/`) | 3–5 days |
| Pipeline YAML storage in DB (remove filesystem dependency) | 2–3 days |
| JWT auth (replace HMAC cookie for multi-replica) | 2 days |
| S3/GCS/MinIO abstraction for logs | 3 days |
| Terraform/Pulumi IaC | 5 days |

### Already done — Vault integration is now complete

The `secrets.py` module now contains a full `hvac` client:
- Supports token auth (`VAULT_TOKEN`) and AppRole (`VAULT_ROLE_ID` + `VAULT_SECRET_ID`)
- Reads KV v2 with fallback to KV v1
- Caches the client; invalidates on failure
- Falls back gracefully (token left in place) if Vault is unreachable
