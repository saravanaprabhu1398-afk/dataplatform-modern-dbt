# Plugin Quick Reference Guide

A quick lookup guide for the 13 available executor plugins. Use this as a cheat sheet while building pipelines.

---

## 🗂️ Plugin Categories

### 📦 Database Plugins (4)
- [PostgreSQL](#postgresql)
- [MySQL](#mysql)
- [Snowflake](#snowflake)
- [DuckDB](#duckdb)

### 🔌 API & Streaming (2)
- [HTTP/REST API](#http-api)
- [Kafka](#kafka)

### ⚡ Processing Plugins (2)
- [Spark](#spark)
- [Python](#python)

### 📁 System Plugins (3)
- [File Operations](#file)
- [Shell Commands](#shell)
- [Email](#email)

### ☁️ Cloud Plugins (1)
- [Google BigQuery](#bigquery)

### 🎯 Data Quality
- [DuckDB Validate](#duckdb)

---

## Database Plugins

### PostgreSQL

```yaml
plugin: postgres
operations: query | execute | load | extract

# Query data (SELECT)
config:
  operation: query
  connection:
    host: localhost
    port: 5432
    database: mydb
    user: postgres
    password: secret
  sql: "SELECT * FROM users"

# Execute (INSERT/UPDATE/DELETE)
config:
  operation: execute
  connection: {...}
  sql: "INSERT INTO users VALUES (...)"

# Load from CSV
config:
  operation: load
  connection: {...}
  table_name: staging_users
  file_path: /path/to/file.csv

# Extract to CSV
config:
  operation: extract
  connection: {...}
  sql: "SELECT * FROM users"
  output_file: /path/to/output.csv

Dependencies: psycopg2-binary
```

### MySQL

```yaml
plugin: mysql
operations: query | execute | bulk_insert

config:
  operation: query  # or execute, bulk_insert
  connection:
    host: localhost
    port: 3306
    user: root
    password: root_password
    database: mydb
  sql: "SELECT * FROM orders"
  # OR for bulk_insert:
  # table_name: my_table
  # file_path: /data.csv
  # columns: [id, name, email]

Dependencies: mysql-connector-python
```

### Snowflake

```yaml
plugin: snowflake
operations: load | query | execute

config:
  operation: load
  connection:
    account: xy12345.us-east-1
    user: ETL_USER
    password: password
    warehouse: COMPUTE_WH
    database: ANALYTICS_DB
    schema: STAGING
  table_name: raw_data
  data:
    - {id: 1, amount: 100}
    - {id: 2, amount: 200}

Dependencies: snowflake-connector-python
```

### DuckDB

```yaml
plugin: duckdb
operations: query | load | aggregate | validate | transform

# Query (SELECT)
config:
  operation: query
  sql: "SELECT COUNT(*) FROM 'data.csv'"

# Load CSV/Parquet
config:
  operation: load
  file_path: /path/to/data.csv
  table_name: my_table

# Aggregate
config:
  operation: aggregate
  sql: "SELECT category, COUNT(*) FROM data GROUP BY category"
  metrics:
    - count
    - sum

# Validate data quality
config:
  operation: validate
  sql: "SELECT * FROM 'raw_data.csv'"
  rules:
    - field: id
      type: not_null
    - field: amount
      type: positive
    - field: email
      type: valid_email

# Transform
config:
  operation: transform
  sql: "SELECT *, UPPER(name) as name_upper FROM data"

Dependencies: duckdb
```

---

## API & Streaming Plugins

### HTTP API

```yaml
plugin: api
methods: GET | POST | PUT | DELETE | PATCH

config:
  method: GET
  url: https://api.example.com/endpoint
  headers:
    Authorization: Bearer token123
    Accept: application/json
  params:
    limit: 100
    offset: 0
  json:         # For POST/PUT/PATCH
    key: value
  retry_count: 3
  timeout: 30

Dependencies: requests
```

### Kafka

```yaml
plugin: kafka
operations: publish | consume | create_topic

# Publish
config:
  operation: publish
  brokers: ["localhost:9092", "kafka2:9092"]
  topic: events
  message:
    event_type: data_loaded
    timestamp: "2024-01-15"
  partition: 0

# Consume
config:
  operation: consume
  brokers: ["localhost:9092"]
  topic: raw_events
  group_id: my_group
  max_messages: 1000
  timeout_ms: 5000

# Create topic
config:
  operation: create_topic
  brokers: ["localhost:9092"]
  topic: new_topic
  partitions: 3
  replication_factor: 2

Dependencies: kafka-python
```

---

## Processing Plugins

### Spark

```yaml
plugin: spark
operations: sql_query | submit_job | dataframe_transform

config:
  operation: sql_query
  spark_master: spark://localhost:7077
  app_name: DataProcessing
  sql: |
    SELECT 
      category, 
      COUNT(*) as total
    FROM sales
    GROUP BY category

# Or transform DataFrame
config:
  operation: dataframe_transform
  spark_master: spark://localhost:7077
  input_path: /data/input
  output_path: /data/output
  format: parquet
  sql: "SELECT * FROM input_data WHERE amount > 100"

Dependencies: pyspark
```

### Python

```yaml
plugin: python
operations: execute_code | run_script

# Inline code
config:
  operation: execute_code
  code: |
    import pandas as pd
    df = pd.read_csv('data.csv')
    df['total'] = df['price'] * df['qty']
    df.to_csv('output.csv')
  imports:
    - pandas
    - numpy

# Run script
config:
  operation: run_script
  script_path: /path/to/script.py
  parameters:
    input_file: data.csv
    output_file: output.csv
    threshold: 100
  timeout: 300

Dependencies: None (built-in)
```

---

## System Plugins

### File Operations

```yaml
plugin: file
operations: read | create | append | copy | move | delete | merge | list

# Read
config:
  operation: read
  file_path: /path/to/file.txt

# Create
config:
  operation: create
  file_path: /path/to/new_file.txt
  content: "file content here"

# Append
config:
  operation: append
  file_path: /path/to/file.txt
  content: "\nmore content"

# Copy
config:
  operation: copy
  source: /source/file.csv
  destination: /dest/file.csv

# Move/Rename
config:
  operation: move
  source: /old/path.csv
  destination: /new/path.csv

# Delete
config:
  operation: delete
  file_path: /path/to/file.txt

# Merge files
config:
  operation: merge
  source_files:
    - /logs/app.log
    - /logs/error.log
  destination: /logs/combined.log

# List files
config:
  operation: list
  directory: /path/to/dir
  pattern: "*.csv"
  recursive: true

Dependencies: None (built-in)
```

### Shell Commands

```yaml
plugin: shell

config:
  command: "bash script.sh"
  shell: bash
  cwd: /opt/app
  env:
    VAR1: value1
    VAR2: value2
  timeout: 300

Dependencies: None (built-in)
```

### Email

```yaml
plugin: email

config:
  smtp_server: smtp.gmail.com
  smtp_port: 587
  sender_email: alerts@company.com
  sender_password: app_password
  recipients:
    - user1@example.com
    - user2@example.com
  subject: "Pipeline Alert - {{ pipeline_name }}"
  body: |
    Pipeline completed!
    Status: {{ status }}
    Duration: {{ duration }}
  html: false
  attachments:
    - /path/to/report.csv
    - /path/to/log.txt

Dependencies: None (built-in)
```

---

## Cloud Plugins

### Google BigQuery

```yaml
plugin: bigquery
operations: query | load | export

# Query
config:
  operation: query
  project_id: my-project
  credentials_path: /secrets/bq_key.json
  sql: |
    SELECT 
      date,
      COUNT(*) as events
    FROM dataset.table
    WHERE date >= "2024-01-01"

# Load
config:
  operation: load
  project_id: my-project
  dataset_id: raw_data
  table_id: imports
  source_file: /data/export.csv

# Export
config:
  operation: export
  project_id: my-project
  dataset_id: analytics
  table_id: results
  destination_uri: gs://my-bucket/results.csv

Dependencies: google-cloud-bigquery
```

---

## 🎯 Operation Matrix

| Plugin | query | execute | load | extract | validate | publish | consume |
|--------|-------|---------|------|---------|----------|---------|---------|
| **postgres** | ✅ | ✅ | ✅ | ✅ | - | - | - |
| **mysql** | ✅ | ✅ | ✅ | - | - | - | - |
| **snowflake** | ✅ | ✅ | ✅ | - | - | - | - |
| **duckdb** | ✅ | - | ✅ | - | ✅ | - | - |
| **api** | GET | POST | - | - | - | - | - |
| **kafka** | - | - | - | - | - | ✅ | ✅ |
| **spark** | ✅ | - | - | - | - | - | - |
| **python** | - | ✅ | - | - | - | - | - |
| **file** | ✅ | ✅ | ✅ | ✅ | - | - | - |
| **shell** | - | ✅ | - | - | - | - | - |
| **email** | - | ✅ | - | - | - | - | - |
| **bigquery** | ✅ | - | ✅ | ✅ | - | - | - |

---

## 📋 Common Configuration Patterns

### Error Handling
```yaml
task:
  type: executor
  plugin: postgres
  config: {...}
  retries: 3              # Retry 3 times
  timeout: 120            # 120 second timeout
```

### Dependencies
```yaml
task:
  type: executor
  plugin: api
  config: {...}
  depends_on:
    - previous_task
    - another_task
```

### Templating
```yaml
task:
  type: executor
  plugin: email
  config:
    subject: "Pipeline {{ pipeline_name }} - {{ status }}"
    body: "Completed in {{ duration }} seconds"
    recipients: ["{{ email }}"]
```

### Environment Variables
```yaml
task:
  type: executor
  plugin: postgres
  config:
    connection:
      host: "{{ env.DB_HOST }}"
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASSWORD }}"
```

---

## 🚀 Quick Tips

1. **Always use environment variables for credentials**
   ```yaml
   password: "{{ env.SECRET_PASSWORD }}"
   ```

2. **Set reasonable timeouts to prevent hanging**
   ```yaml
   config: {...}
   timeout: 300  # 5 minutes
   ```

3. **Use retry_count for unreliable operations**
   ```yaml
   config: {...}
   retries: 3
   ```

4. **Check dependencies for plugin requirements**
   ```bash
   pip install psycopg2-binary mysql-connector-python
   ```

5. **Test API URLs independently before using**
   ```bash
   curl -H "Authorization: Bearer token" https://api.example.com
   ```

---

## 💡 When to Use Each Plugin

| Task | Plugin |
|------|--------|
| Load to data warehouse | Snowflake, BigQuery, PostgreSQL |
| Validate data quality | DuckDB, Python |
| Real-time streaming | Kafka |
| Distribute heavy computation | Spark |
| Simple SQL queries | DuckDB, PostgreSQL |
| Call external APIs | API plugin |
| Transform with Pandas | Python |
| File manipulation | File operations |
| System automation | Shell |
| Send alerts | Email |

---

## 📞 Need Help?

- **Detailed guide**: See [PLUGINS_GUIDE.md](PLUGINS_GUIDE.md)
- **Examples**: See [INTEGRATION_EXAMPLES.md](INTEGRATION_EXAMPLES.md)
- **Quick start**: See [README.md](README.md)
- **Sample pipelines**: Check `.yaml` files in root directory
