# 🚀 Data Platform - Modern DBT Alternative

A comprehensive, enterprise-grade data orchestration platform with YAML configuration, multi-plugin architecture, and built-in dashboard. Similar to Airflow but simpler and more extensible.

![Status: Production Ready](https://img.shields.io/badge/Status-Production--Ready-brightgreen)
![Python: 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue)
![Plugins: 12+](https://img.shields.io/badge/Plugins-12%2B-orange)

## ✨ Key Features

### 🎯 Core Capabilities
- **YAML-Based Pipelines**: Define complex data workflows with simple YAML configs
- **12+ Built-in Plugins**: Database, API, file, cloud, and streaming integrations
- **Web Dashboard**: Real-time monitoring with DAG visualization (Airflow-style)
- **REST API**: Programmatic pipeline management
- **Background Execution**: Async pipeline runs without blocking
- **Flexible Scheduling**: Cron-based scheduling with UI picker
- **Error Tracking**: Task-level error reporting and alerting
- **Multi-Platform**: Works on Linux, macOS, Windows

### 🔌 Integrated Plugins

| Category | Plugins |
|----------|---------|
| **Databases** | PostgreSQL, MySQL, Snowflake, DuckDB |
| **Cloud** | Google BigQuery, Snowflake |
| **APIs** | HTTP/REST, Kafka |
| **Processing** | Spark, Python, DuckDB |
| **Files** | Local files, CSV, JSON |
| **Execution** | Shell commands, Python scripts |
| **Notifications** | Email alerts |

## 🚀 Quick Start

### 1. Install

```bash
# Clone repository
git clone <repo_url>
cd data-platform-modern-dbt

# Install dependencies
pip install -r requirements.txt
```

### 2. Start Web Dashboard

```bash
# Start the API server (runs on port 8000)
python3 -m dataplatform.core.api

# Or in background
python3 -m dataplatform.core.api &
```

**Dashboard**: http://localhost:8000

### 3. Create Your First Pipeline

**Best Practice**: Store pipelines in the `pipelines/` folder for auto-discovery.

```yaml
# pipelines/my_pipeline.yaml
pipeline_name: My First Pipeline
description: Load data and transform

schedule:
  minute: "0"
  hour: "9"
  day: "*"
  month: "*"
  day_of_week: "*"

tasks:
  - name: Fetch Data
    id: fetch_data
    type: executor
    plugin: api
    config:
      method: GET
      url: https://api.example.com/data

  - name: Validate Data
    id: validate
    type: executor
    plugin: duckdb
    config:
      operation: validate
      sql: "SELECT * FROM data"
    depends_on: [fetch_data]

  - name: Load to Warehouse
    id: load_warehouse
    type: executor
    plugin: snowflake
    config:
      operation: load
      table_name: raw_data
    depends_on: [validate]
```

**Key points:**
- ✅ Save in `pipelines/` folder - auto-discovered
- ✅ Use `pipeline_name` (required field)
- ✅ Each task needs `name` field
- ✅ Task IDs in `depends_on` create dependencies
- ✅ Use YAML dict for `schedule` field

See [pipelines/README.md](pipelines/README.md) for complete guide and examples.

### 4. Run It!

- **Via Web UI**: Go to http://localhost:8000, select pipeline, click "Run"
- **Via REST API**: `curl http://localhost:8000/run/my_pipeline`
- **View Status**: Real-time DAG visualization with task status

## 🎨 Web Dashboard Features

### 📊 Pipeline Management
- **Auto-discovery** of all pipelines in `pipelines/` folder
- **Real-time validation** - errors shown immediately
- **Error highlighting** - failed pipelines red-highlighted with details
- **One-click execution** with async background processing
- **Configuration preview** before running

### 📈 Live Monitoring
- **Real-time status updates** (2-second polling)
- **Airflow-style DAG visualization** with task status colors
- **Execution history** (last 5 runs per pipeline)
- **Error details** at task level
- **Task dependency visualization** in DAG

### ⏰ Scheduling
- **Flexible cron scheduling** with second-level precision
- **UI schedule picker** with quick presets (Daily, Hourly, Weekdays, Weekends)
- **Custom schedules** saved per pipeline

### 🔴 Error Handling
- **Failed task highlighting** (red nodes in DAG)
- **Error message display** in execution history
- **Pipeline validation errors** shown in dashboard with fix suggestions
- **Error type and path** displayed for debugging
- **Retry configuration** support

## 📚 Detailed Documentation

### Plugins Guide
See [PLUGINS_GUIDE.md](PLUGINS_GUIDE.md) for:
- Configuration examples for each plugin
- Best practices and performance tips
- Troubleshooting guide
- Custom plugin development

### Example Pipelines

**Sample Pipelines Included**:
- `sample_pipeline.yaml` - Employee analytics (DuckDB + Snowflake)
- `sales_pipeline.yaml` - Sales transaction processing (8-task workflow)
- `multi_source_pipeline.yaml` - Advanced multi-plugin integration

## 🔌 Available Plugins

### Databases
- **PostgreSQL** - Traditional RDBMS, ETL pipelines
- **MySQL/MariaDB** - MySQL integration, legacy systems
- **Snowflake** - Cloud data warehouse, scalable analytics
- **DuckDB** - In-memory OLAP, data validation

### APIs & Streaming
- **HTTP/REST** - API calls, webhooks, data ingestion
- **Kafka** - Event streaming, message queues
- **Email** - Notifications and alerts

### Data Processing
- **Python** - Custom Python code and scripts
- **Spark** - Distributed processing, machine learning
- **Shell** - System commands, custom scripts

### Files & Storage
- **File Operations** - Read, write, transform, merge files
- **Local & Cloud** - File operations with easy extension

### Cloud
- **Google BigQuery** - Query, load, export operations

## 🛠️ Configuration Examples

### PostgreSQL Integration
```yaml
task:
  type: executor
  plugin: postgres
  config:
    operation: query
    connection:
      host: localhost
      database: analytics
      user: postgres
    sql: "SELECT COUNT(*) FROM users"
```

### API Data Ingestion
```yaml
task:
  type: executor
  plugin: api
  config:
    method: GET
    url: https://api.example.com/data
    headers:
      Authorization: Bearer token
    retry_count: 3
```

### Kafka Event Publishing
```yaml
task:
  type: executor
  plugin: kafka
  config:
    operation: publish
    brokers: ["localhost:9092"]
    topic: events
    message:
      event_type: data_loaded
      timestamp: "{{ timestamp }}"
```

### Email Notifications
```yaml
task:
  type: executor
  plugin: email
  config:
    smtp_server: smtp.gmail.com
    sender_email: alerts@company.com
    recipients: [team@company.com]
    subject: "Pipeline {{ pipeline_name }} - {{ status }}"
```

## 📊 Architecture

```
dataplatform/
├── core/
│   ├── api.py           # FastAPI server with REST endpoints
│   ├── executor.py      # Task execution engine
│   ├── scheduler.py     # APScheduler wrapper
│   └── config.py        # Configuration models
├── plugins/
│   ├── base.py          # Base plugin classes
│   ├── registry.py      # Plugin registry
│   └── executors/       # Individual plugin implementations
│       ├── duckdb_plugin.py
│       ├── postgres_plugin.py
│       ├── api_plugin.py
│       ├── kafka_plugin.py
│       └── ... (8+ more)
└── static/
    └── index.html       # Web dashboard
```

## 🔐 Security

- **Environment variables** for credentials (`.env`)
- **No secrets in YAML** configs
- **Secure password storage** support
- **API authentication** ready (extend as needed)

## 📈 Performance

- **DuckDB**: 10-100x faster than pandas for analytical queries
- **Spark**: Distributed processing for large datasets (GB-TB scale)
- **Async execution**: Non-blocking pipeline runs
- **Efficient scheduling**: Low CPU overhead

## 🐛 Troubleshooting

If you encounter issues:

1. **Check logs**:
   ```bash
   # View API server logs
   tail -f /path/to/combined.log
   ```

2. **Quick diagnostics**:
   ```bash
   python3 diagnose_api.py
   ```

3. **Common issues**:
   - Port 8000 in use: Kill existing process or change port
   - Pluggy/module not found: Install requirements
   - API not responding: Check server is running

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for detailed help.

## ✅ Supported Platforms

- ✅ Linux (Ubuntu, CentOS, etc.)
- ✅ macOS (Intel and Apple Silicon)
- ✅ Windows (with WSL recommended)
- ✅ Docker (containerize for production)

## 🤝 Contributing

To add a custom plugin:

1. Create `dataplatform/plugins/executors/my_plugin.py`
2. Implement `execute(config: dict) -> tuple[bool, dict]`
3. Add to registry in `dataplatform/plugins/registry.py`
4. Reference in pipelines as `plugin: my_plugin`

See [PLUGINS_GUIDE.md](PLUGINS_GUIDE.md#contributing-custom-plugins) for details.

## 📝 Examples

Run the included example pipelines:

```bash
# View in web dashboard at http://localhost:8000
# Or run via API:
curl -X POST http://localhost:8000/run/sample_pipeline
curl -X POST http://localhost:8000/run/sales_pipeline
curl -X POST http://localhost:8000/run/multi_source_pipeline
```

## 🎓 Learning Resources

- [PLUGINS_GUIDE.md](PLUGINS_GUIDE.md) - Complete plugin reference
- [sample_pipeline.yaml](sample_pipeline.yaml) - Basic example
- [sales_pipeline.yaml](sales_pipeline.yaml) - Advanced example
- [multi_source_pipeline.yaml](multi_source_pipeline.yaml) - Multi-plugin integration

## 📄 API Reference

### Endpoints

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/pipelines` | List all pipelines |
| GET | `/status` | Get all pipeline statuses |
| GET | `/history/{name}` | Get last 5 runs |
| GET | `/dag/{name}` | Get DAG for pipeline |
| POST | `/run/{name}` | Execute pipeline |
| POST | `/schedule/{name}` | Set schedule with custom cron |

## 🚀 Production Deployment

For production deployment:

1. Use a process manager (systemd, supervisor)
2. Set environment variables for credentials
3. Configure database for persistent storage
4. Use reverse proxy (nginx) for security
5. Enable monitoring and alerting
6. Run in Docker for consistency

See deployment guides for specific platforms.

## 📄 License

[Add your license here]

## 💬 Support

- Check [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
- Review [PLUGINS_GUIDE.md](PLUGINS_GUIDE.md)
- File an issue on GitHub
- **Status Persistence**: Execution status is maintained across page refreshes
- **Error Handling**: Clear error messages and failure diagnostics

## 📊 API Endpoints

The web UI communicates with a REST API:

- `GET /` - Main dashboard
- `GET /pipelines` - List available pipelines
- `POST /run` - Trigger pipeline execution
- `GET /status` - Get pipeline execution status
- `GET /dag` - Get DAG structure for visualization
- `POST /schedule` - Schedule pipelines for automatic execution

## ⚙️ Pipeline Configuration

```yaml
pipeline_name: my_pipeline
file_path: data/my_data.csv

tasks:
  - name: load_data
    type: executor
    plugin: duckdb
    operation: load

  - name: validate
    type: executor
    plugin: duckdb
    operation: validate
    config:
      checks:
        - name: positive_amounts
          sql: 'SELECT COUNT(*) FROM data WHERE amount <= 0'
          expect: 0
    depends_on: [load_data]

  - name: analyze
    type: executor
    plugin: duckdb
    operation: aggregate
    config:
      group_by: ['category']
      metrics:
        - column: 'revenue'
          function: 'sum'
          alias: 'total_revenue'
    depends_on: [validate]
```

## 🔧 Available Operations

### DuckDB Executor
- **load**: Load CSV data
- **validate**: Data quality checks
- **aggregate**: Group and aggregate
- **transform**: Add derived columns
- **query**: Execute custom SQL (can return data for next tasks)

### Snowflake Executor
- **load_to_snowflake**: Load data from previous task into Snowflake table

## ❄️ Snowflake Integration

The framework supports loading processed data into Snowflake as an aggregated layer:

```yaml
- name: load_aggregates_to_snowflake
  type: executor
  plugin: snowflake
  operation: load_to_snowflake
  config:
    snowflake_config:
      account: "your_account.snowflakecomputing.com"
      user: "your_user"
      password: "your_password"
      warehouse: "your_warehouse"
      database: "your_database"
      schema: "your_schema"
    table_name: "department_analytics"
    if_exists: "replace"  # or "append"
  depends_on:
    - generate_aggregates  # Task that returns data
```

### Data Flow Between Tasks

Tasks can pass data to dependent tasks:

```yaml
- name: generate_report
  type: executor
  plugin: duckdb
  operation: query
  config:
    sql: "SELECT * FROM analytics_table"
    return_data: true  # Return query results
  depends_on: [create_analytics]

- name: load_to_snowflake
  type: executor
  plugin: snowflake
  operation: load_to_snowflake
  config:
    table_name: "final_analytics"
  depends_on: [generate_report]  # Receives data from generate_report
```
- **query**: Custom SQL queries
- **transform**: Add derived columns

## 📁 Project Structure

```
data-platform/
├── dataplatform/     # Core framework
├── data/            # Your data files
├── sample_pipeline.yaml    # Example config
└── README.md        # This file
```

## 🎯 Key Features

- **YAML Configuration**: No code changes needed
- **Plugin Architecture**: Extensible with new data sources
- **DAG Execution**: Handles task dependencies
- **Error Handling**: Automatic retries and logging
- **Data Validation**: Built-in quality checks
- **Export Support**: Save results to files

## 📈 Use Cases

- ETL pipelines
- Data validation
- Business analytics
- Report generation
- Data transformation
- Quality assurance

---

**Ready? Run `python3 -m dataplatform.cli.main run sample_pipeline.yaml` to see it in action!** 🚀
        ↓
Storage + Logs
        ↓
Deployment (Docker + Kubernetes)
```

---

# 🧩 Core Components

## 1. CLI Layer

### Commands:

```
dataplatform init <project>
dataplatform run <config>
dataplatform install <plugin>
```

### Responsibilities:

* Project initialization
* Pipeline execution trigger
* Plugin installation

---

## 2. Config-Driven Pipelines

### Example: `pipeline.yaml`

```yaml
pipeline_name: sample_pipeline

file_path: data/sample.csv

tasks:
  - name: ingest
    type: executor
    plugin: duckdb
    retries: 2

  - name: transform
    type: transformer
    plugin: dbt
    depends_on:
      - ingest
    retries: 1

schedule:
  minute: "0"
  hour: "*/2"
```

---

## 3. Plugin Architecture (Key Innovation)

### Types:

* Executors → Run data processing
* Transformers → Apply transformations

### Example:

```
plugins/
├── executors/
│   └── duckdb_executor.py
├── transformers/
│   └── dbt_transformer.py
```

### Benefits:

* Replace tools without changing core logic
* Extend platform easily
* Cloud-agnostic design

---

## 4. DAG Engine

### Features:

* Task dependency management
* Execution ordering
* Supports Directed Acyclic Graph (DAG)

### Example Flow:

```
ingest → transform → validate
```

### Components:

* Task model
* DAG builder
* DAG executor

---

## 5. Execution Engine

### Responsibilities:

* Load plugins dynamically
* Execute tasks in dependency order
* Handle failures and retries

---

## 6. Retry & Failure Handling

### Features:

* Configurable retries per task
* Fail-fast pipeline execution
* Error logging

### Example:

```yaml
retries: 2
```

---

## 7. Logging System

### Features:

* Centralized logging
* Task-level logs
* Stored in `logs/pipeline.log`

### Tracks:

* Task start/end
* Errors
* Retry attempts

---

## 8. Scheduler (Cron-Based)

### Implementation:

* Background scheduler

### Example:

```yaml
schedule:
  minute: "0"
  hour: "*/2"
```

### Capabilities:

* Automated execution
* Recurring pipelines

---

## 9. UI Layer (React + D3)

### Features:

* DAG visualization
* Pipeline trigger
* Status monitoring

### Graph Representation:

* Nodes → Tasks
* Edges → Dependencies

---

## 10. Backend API

### Built using:

* FastAPI

### Endpoints:

```
GET /dag
GET /run
GET /status
```

### Frontend UI

* A React-based DAG visualizer is available in the `frontend/` folder
* It uses the backend `/dag` endpoint to render pipeline graphs
* Serve it as static content with `python3 -m http.server` or any file server

---

## 11. Multi-User System

### Features:

* Authentication (JWT-based)
* User-specific pipelines
* Role-based access (future)

### Database Tables:

* users
* pipelines
* runs

---

## 12. Plugin Marketplace

### Concept:

Install plugins dynamically

### Example:

```
dataplatform install spark
```

### Features:

* Plugin registry
* pip-based installation
* Dynamic loading

---

## 13. Packaging (pip)

### Installation:

```
pip install dataplatform
```

### Structure:

```
dataplatform/
  ├── cli/
  ├── core/
  ├── plugins/
  ├── templates/
```

---

## 14. Deployment

## 🐳 Docker

```
docker build -t dataplatform .
docker run -p 8000:8000 dataplatform
```

---

## ☸️ Kubernetes

* Scalable deployment
* Multi-instance execution
* Production-ready setup

---

# 🔥 Key Differentiators

✅ Config-driven pipelines
✅ Plugin-based architecture
✅ Cloud-agnostic design
✅ Local-first execution (DuckDB)
✅ DAG orchestration engine
✅ Built-in retries & logging
✅ Extensible marketplace

---

# 🚀 MVP vs Advanced Roadmap

## MVP

* CLI
* Config pipelines
* DuckDB executor
* dbt integration
* Basic DAG

---

## Intermediate

* Plugin system
* Retry + logging
* Scheduler
* FastAPI backend

---

## Advanced

* React + D3 UI
* Multi-user system
* Plugin marketplace
* Cloud deployment

---

# 💼 Resume Value

> Built a cloud-agnostic, plugin-based data platform with DAG orchestration, scheduling, and interactive UI, enabling config-driven pipeline execution similar to modern data platforms.

---

# 🧠 Learning Outcomes

This project demonstrates:

* System design
* Platform engineering
* Distributed thinking
* Extensibility patterns
* Production-grade practices

---

# 🎯 Future Enhancements

* Parallel DAG execution
* Data lineage integration
* Cost optimization engine
* AI-based pipeline recommendations
* SaaS platform version

---

# 🏁 Conclusion

This is not just a project—it is a **data platform foundation** that can evolve into:

* Internal enterprise platform
* Open-source tool
* SaaS product

---

🚀 You've essentially built a **mini modern data platform ecosystem**
