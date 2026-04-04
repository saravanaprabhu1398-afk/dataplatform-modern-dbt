# Data Platform Enterprise Plugins Guide

This guide covers all available executor plugins in the data platform, their operations, configuration options, and usage examples.

## Table of Contents

1. [Plugin Overview](#plugin-overview)
2. [Database Plugins](#database-plugins)
3. [Data Processing Plugins](#data-processing-plugins)
4. [Integration Plugins](#integration-plugins)
5. [File & System Plugins](#file--system-plugins)
6. [Cloud Plugins](#cloud-plugins)
7. [Configuration Examples](#configuration-examples)

---

## Plugin Overview

### Currently Available Plugins

| Plugin | Type | Purpose | Status |
|--------|------|---------|--------|
| **duckdb** | Executor | In-memory analytical database | ✅ Production |
| **snowflake** | Executor | Cloud data warehouse | ✅ Production |
| **postgres** | Executor | PostgreSQL database | ✅ Production |
| **mysql** | Executor | MySQL/MariaDB database | ✅ Production |
| **api** | Executor | HTTP/REST calls | ✅ Production |
| **file** | Executor | File operations | ✅ Production |
| **shell** | Executor | Shell command execution | ✅ Production |
| **python** | Executor | Python code execution | ✅ Production |
| **email** | Executor | Email notifications | ✅ Production |
| **spark** | Executor | Distributed processing | ✅ Available |
| **kafka** | Executor | Event streaming | ✅ Available |
| **bigquery** | Executor | Google BigQuery | ✅ Available |

---

## Database Plugins

### PostgreSQL Executor

**Purpose**: Execute operations on PostgreSQL databases

**Operations**:
- `execute`: Insert, update, delete operations
- `query`: SELECT queries
- `load`: Bulk load from CSV
- `extract`: Export to CSV

**Configuration**:

```yaml
task_name:
  type: executor
  plugin: postgres
  config:
    operation: query
    connection:
      host: localhost
      port: 5432
      database: mydb
      user: postgres
      password: password
    sql: "SELECT * FROM users WHERE age > 30"
```

**Examples**:

```yaml
# Execute INSERT
- id: insert_users
  type: executor
  plugin: postgres
  config:
    operation: execute
    connection:
      host: localhost
      database: analytics
      user: postgres
      password: secret
    sql: |
      INSERT INTO users (name, email)
      VALUES ('John Doe', 'john@example.com')

# Bulk load from CSV
- id: load_data
  type: executor
  plugin: postgres
  config:
    operation: load
    connection:
      host: localhost
      database: analytics
      user: postgres
    table_name: staging_users
    file_path: /data/users.csv

# Query data
- id: count_users
  type: executor
  plugin: postgres
  config:
    operation: query
    connection:
      host: localhost
      database: analytics
      user: postgres
    sql: "SELECT COUNT(*) as total FROM users"
```

---

### MySQL Executor

**Purpose**: Execute operations on MySQL/MariaDB databases

**Operations**:
- `execute`: INSERT, UPDATE, DELETE
- `query`: SELECT queries
- `bulk_insert`: Bulk load from CSV

**Configuration**:

```yaml
task_name:
  type: executor
  plugin: mysql
  config:
    operation: query
    connection:
      host: localhost
      port: 3306
      user: root
      password: root_password
      database: my_database
    sql: "SELECT * FROM orders"
```

---

### Snowflake Executor

**Purpose**: Load data to Snowflake cloud data warehouse

**Operations**:
- `load`: Insert data into tables
- `query`: Execute Snowflake SQL
- `execute`: Perform DDL/DML operations

**Configuration**:

```yaml
task_name:
  type: executor
  plugin: snowflake
  config:
    operation: load
    connection:
      account: xy12345.us-east-1
      user: ETL_USER
      password: password
      warehouse: COMPUTE_WH
      database: ANALYTICS_DB
      schema: STAGING
    table_name: sales_raw
    data:
      - {id: 1, amount: 100, date: "2024-01-01"}
      - {id: 2, amount: 200, date: "2024-01-02"}
```

---

## Data Processing Plugins

### DuckDB Executor

**Purpose**: In-memory SQL analytics and data validation

**Operations**:
- `load`: Load CSV/Parquet data
- `query`: SQL SELECT queries
- `aggregate`: Group by and aggregation
- `validate`: Data quality checks
- `transform`: Column transformation

**Configuration**:

```yaml
task_name:
  type: executor
  plugin: duckdb
  config:
    operation: query
    sql: "SELECT COUNT(*) FROM 'data/file.csv'"

# Data validation example
- id: validate_data_quality
  type: executor
  plugin: duckdb
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
```

---

### Python Executor

**Purpose**: Execute Python code and scripts

**Operations**:
- `execute_code`: Run Python code inline
- `run_script`: Execute Python script from file

**Configuration**:

```yaml
# Execute inline Python code
- id: data_transform
  type: executor
  plugin: python
  config:
    operation: execute_code
    code: |
      import pandas as pd
      df = pd.read_csv('data.csv')
      df['total'] = df['price'] * df['quantity']
      df.to_csv('output.csv', index=False)
    imports:
      - pandas
      - numpy

# Run Python script
- id: run_ml_model
  type: executor
  plugin: python
  config:
    operation: run_script
    script_path: scripts/train_model.py
    parameters:
      input_file: data/training_data.csv
      output_model: models/model.pkl
      epochs: 100
```

---

### Spark Executor

**Purpose**: Distributed data processing using Apache Spark

**Operations**:
- `submit_job`: Submit Spark job
- `sql_query`: Execute Spark SQL
- `dataframe_transform`: Transform Spark DataFrames

**Configuration**:

```yaml
- id: spark_processing
  type: executor
  plugin: spark
  config:
    operation: sql_query
    spark_master: spark://localhost:7077
    app_name: DataProcessing
    sql: |
      SELECT 
        category,
        COUNT(*) as total,
        SUM(amount) as revenue
      FROM sales_data
      GROUP BY category
```

---

## Integration Plugins

### HTTP/REST API Executor

**Purpose**: Make HTTP requests and API calls

**Operations**: GET, POST, PUT, DELETE, PATCH

**Configuration**:

```yaml
# GET request
- id: fetch_data
  type: executor
  plugin: api
  config:
    method: GET
    url: https://api.github.com/repos/owner/repo
    headers:
      Authorization: Bearer token123
      Accept: application/json

# POST request with retry
- id: send_webhook
  type: executor
  plugin: api
  config:
    method: POST
    url: https://webhook.site/unique-endpoint
    json:
      event: pipeline_completed
      status: success
      timestamp: "{{ timestamp }}"
    retry_count: 3
    timeout: 30

# API data collection
- id: collect_metrics
  type: executor
  plugin: api
  config:
    method: GET
    url: https://api.metrics.com/v1/data
    params:
      start_date: "2024-01-01"
      end_date: "2024-01-31"
      format: json
```

---

### Kafka Executor

**Purpose**: Event streaming and message queue operations

**Operations**:
- `publish`: Send messages to topic
- `consume`: Read messages from topic
- `create_topic`: Create Kafka topic

**Configuration**:

```yaml
# Publish message
- id: publish_event
  type: executor
  plugin: kafka
  config:
    operation: publish
    brokers:
      - kafka1:9092
      - kafka2:9092
    topic: data_events
    message:
      event_type: data_loaded
      timestamp: "2024-01-15"
      record_count: 1000

# Consume messages
- id: consume_events
  type: executor
  plugin: kafka
  config:
    operation: consume
    brokers:
      - localhost:9092
    topic: raw_events
    group_id: data_platform
    max_messages: 100
    timeout_ms: 5000
```

---

## File & System Plugins

### File Operations Executor

**Purpose**: Local file manipulation and processing

**Operations**:
- `read`: Read file content
- `create`: Create new file
- `append`: Append to file
- `copy`: Copy file
- `move`: Move/rename file
- `delete`: Delete file
- `merge`: Merge multiple files
- `list`: List files in directory

**Configuration**:

```yaml
# Read file
- id: read_config
  type: executor
  plugin: file
  config:
    operation: read
    file_path: config/settings.json

# Create file
- id: create_report
  type: executor
  plugin: file
  config:
    operation: create
    file_path: reports/daily_report.txt
    content: |
      Daily Report
      ============
      Date: {{ today }}
      Records Processed: {{ record_count }}

# Copy file
- id: backup_data
  type: executor
  plugin: file
  config:
    operation: copy
    source: data/production.csv
    destination: backups/production_backup_{{ timestamp }}.csv

# Merge files
- id: combine_logs
  type: executor
  plugin: file
  config:
    operation: merge
    source_files:
      - logs/app.log
      - logs/errors.log
      - logs/warnings.log
    destination: logs/combined.log
```

---

### Shell Executor

**Purpose**: Execute shell commands and scripts

**Operations**: Execute shell commands

**Configuration**:

```yaml
# Simple command
- id: check_disk_space
  type: executor
  plugin: shell
  config:
    command: df -h

# Run shell script
- id: deploy_application
  type: executor
  plugin: shell
  config:
    command: bash deploy.sh
    shell: bash
    cwd: /opt/app
    env:
      ENVIRONMENT: production
      VERSION: 1.0.0

# Complex command with pipes
- id: process_logs
  type: executor
  plugin: shell
  config:
    command: |
      cat logs/app.log | \
      grep "ERROR" | \
      wc -l
    timeout: 60
```

---

### Email Executor

**Purpose**: Send email notifications

**Operations**: Send email

**Configuration**:

```yaml
- id: send_alert
  type: executor
  plugin: email
  config:
    smtp_server: smtp.gmail.com
    smtp_port: 587
    sender_email: alerts@company.com
    sender_password: app_specific_password
    recipients:
      - admin@company.com
      - team@company.com
    subject: "Pipeline Execution Report - {{ date }}"
    body: |
      Pipeline Status: {{ status }}
      
      Task Summary:
      - Completed: {{ completed_tasks }}
      - Failed: {{ failed_tasks }}
      
      Duration: {{ execution_time }} seconds
    html: false
    attachments:
      - reports/execution_report.csv
      - logs/pipeline.log
```

---

## Cloud Plugins

### Google BigQuery Executor

**Purpose**: Google Cloud data warehouse operations

**Operations**:
- `query`: Execute SQL queries
- `load`: Load data into table
- `export`: Export table to GCS

**Configuration**:

```yaml
# Query data
- id: analyze_data
  type: executor
  plugin: bigquery
  config:
    operation: query
    project_id: my-analytics-project
    credentials_path: /secrets/bigquery_credentials.json
    sql: |
      SELECT 
        DATE(timestamp) as date,
        COUNT(*) as events,
        COUNT(DISTINCT user_id) as users
      FROM events.events_table
      WHERE DATE(timestamp) >= "2024-01-01"
      GROUP BY date
      ORDER BY date DESC

# Load data
- id: load_to_bq
  type: executor
  plugin: bigquery
  config:
    operation: load
    project_id: my-analytics-project
    dataset_id: raw_data
    table_id: imports
    source_file: data/export.csv

# Export data
- id: export_results
  type: executor
  plugin: bigquery
  config:
    operation: export
    project_id: my-analytics-project
    dataset_id: analytics
    table_id: results
    destination_uri: gs://my-bucket/exports/results_2024.csv
```

---

## Configuration Examples

### Complete Multi-Plugin Pipeline Example

See `multi_source_pipeline.yaml` for a complex pipeline using multiple plugins.

### Best Practices

1. **Connection Management**:
   ```yaml
   # Store credentials in environment or secure vaults
   connection:
     host: "{{ env.DB_HOST }}"
     user: "{{ env.DB_USER }}"
     password: "{{ env.DB_PASSWORD }}"
   ```

2. **Error Handling**:
   ```yaml
   task:
     type: executor
     plugin: postgres
     retries: 3
     timeout: 120
     on_failure: notify_team  # Reference to email task
   ```

3. **Templating**:
   ```yaml
   config:
     subject: "Pipeline {{ pipeline_name }} - {{ status }} at {{ timestamp }}"
     file_path: "/data/{{ date }}/output.csv"
   ```

4. **Dependency Management**:
   ```yaml
   task:
     depends_on:
       - previous_task_1
       - previous_task_2
     wait_for_completion: true
   ```

### Installation Requirements

Each plugin may require specific dependencies:

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install mysql-connector-python

# Snowflake
pip install snowflake-connector-python

# Spark
pip install pyspark

# Kafka
pip install kafka-python

# BigQuery
pip install google-cloud-bigquery

# API
pip install requests

# Data processing
pip install pandas pyarrow duckdb
```

---

## Performance Considerations

### DuckDB
- **Memory**: Uses system RAM; suitable for datasets < available memory
- **Speed**: Excellent for analytical queries, 10-100x faster than pandas

### Spark
- **Scalability**: Distributed; ideal for large datasets (GB-TB scale)
- **Memory**: Adaptive; uses cluster resources
- **Overhead**: Startup time; suitable for batch processing

### Snowflake
- **Scalability**: Virtually unlimited
- **Cost**: Pay-per-query model
- **Latency**: Lower latency for real-time analytics

### PostgreSQL
- **Scalability**: Good for OLTP; moderate for OLAP
- **Performance**: Excellent for < 10GB datasets
- **Throughput**: High transaction throughput

---

## Troubleshooting

### Connection Issues
- Verify network connectivity
- Check credentials in error logs
- Ensure firewall rules allow connections
- Test connection independently

### Performance Issues
- Monitor resource usage (CPU, memory)
- Check query execution plans
- Optimize SQL queries
- Consider partitioning large datasets

### Plugin Not Loading
- Check plugin file exists in `dataplatform/plugins/executors/`
- Verify class name matches expected pattern
- Check for import errors in logs
- Ensure all dependencies installed

---

## Contributing Custom Plugins

To create a custom plugin:

1. Create file: `dataplatform/plugins/executors/custom_plugin.py`
2. Implement the executor class with `execute(config: dict) -> tuple[bool, dict]`
3. Add to plugin registry in `dataplatform/plugins/registry.py`
4. Test with example pipeline

See `postgres_plugin.py` for reference implementation.
