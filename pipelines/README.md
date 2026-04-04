# Pipelines Directory

This directory contains all your data pipeline configuration files. Pipelines placed here are **automatically discovered and loaded** by the Data Platform dashboard.

## How to Create a Pipeline

1. **Create a YAML file** with your pipeline configuration (e.g., `my_pipeline.yaml`)
2. **Place it in this folder** (`pipelines/`)
3. **Refresh the dashboard** - it will automatically appear in the pipeline list

## Pipeline Structure

A valid pipeline must follow this schema:

```yaml
pipeline_name: My Pipeline Name           # Required: Name of the pipeline
description: "What this pipeline does"     # Optional: Description
file_path: "data/input.csv"               # Optional: Input file path

schedule:                                  # Optional: Scheduler configuration
  minute: "0"
  hour: "9"
  day: "*"
  month: "*"
  day_of_week: "*"

tasks:                                     # Required: List of tasks
  - name: "Task Name"                     # Required: Human-readable name
    id: task_id                           # Required: Unique identifier
    type: executor                        # Required: "executor" or "transformer"
    plugin: plugin_name                   # Required: Plugin name (e.g., "duckdb", "api")
    config: {}                            # Required: Plugin-specific config
    retries: 3                            # Optional: Number of retries
    timeout: 300                          # Optional: Timeout in seconds
    depends_on:                           # Optional: Task dependencies
      - previous_task
```

## Available Plugins

See the main [PLUGINS_GUIDE.md](../PLUGINS_GUIDE.md#plugin-categories) for a complete list of available plugins.

Quick reference:
- **Databases**: `postgres`, `mysql`, `snowflake`, `duckdb`, `bigquery`
- **APIs**: `api`, `kafka`
- **Processing**: `spark`, `python`, `duckdb`
- **Files**: `file`, `shell`
- **Notifications**: `email`

## Examples

### Simple DuckDB Query
```yaml
pipeline_name: Simple Query Pipeline
description: "Query data with DuckDB"

tasks:
  - name: Query Data
    id: query
    type: executor
    plugin: duckdb
    config:
      operation: query
      sql: "SELECT COUNT(*) FROM 'data/file.csv'"
```

### Multi-Step Pipeline
```yaml
pipeline_name: Multi-Step ETL
description: "Extract, transform, and load"

tasks:
  - name: Extract Data
    id: extract
    type: executor
    plugin: api
    config:
      method: GET
      url: "https://api.example.com/data"

  - name: Transform Data
    id: transform
    type: executor
    plugin: python
    config:
      operation: execute_code
      code: |
        import pandas as pd
        df = pd.read_json('data.json')
        df['processed'] = True
        df.to_csv('output.csv')
    depends_on:
      - extract

  - name: Load to Warehouse
    id: load
    type: executor
    plugin: snowflake
    config:
      operation: load
      table_name: staging_data
    depends_on:
      - transform
```

## Error Handling

If a pipeline fails to load, you'll see:
- **Error highlighted in red** in the dashboard
- **Error type** (e.g., validation error)
- **Full error message** to help you fix it
- **File path** for quick reference

Common errors:
- ❌ `pipeline_name` field missing - use correct field name
- ❌ `tasks.*.name` field missing - each task needs a name
- ❌ Invalid plugin name - check [PLUGINS_GUIDE.md](../PLUGINS_GUIDE.md)
- ❌ Required config fields missing - see plugin documentation

## Running Pipelines

### Via Dashboard
1. Go to http://localhost:8000
2. Find your pipeline in the list
3. Click **Run** button

### Via REST API
```bash
curl -X POST http://localhost:8000/run/my_pipeline \
  -H "Content-Type: application/json"
```

### Via CLI (Future)
```bash
dataplatform run pipelines/my_pipeline.yaml
```

## Scheduling

Pipelines can be scheduled with cron expressions:

```yaml
schedule:
  minute: "0"          # 0-59 or *
  hour: "9"            # 0-23 or *
  day: "*"             # 1-31 or *
  month: "*"           # 1-12 or *
  day_of_week: "*"     # 0-6 (0=Sunday) or *
```

Or use the dashboard UI:
1. Click **Schedule** button
2. Set your cron expression
3. Use quick presets (Daily, Hourly, Weekdays, etc.)

## Included Examples

This folder includes example pipelines:
- `sample_pipeline.yaml` - Employee analytics with DuckDB & Snowflake
- `sales_pipeline.yaml` - Sales transaction processing
- `multi_source_pipeline.yaml` - Advanced multi-plugin integration

Start with these as templates!

## Auto-Discovery

Pipelines are **automatically discovered** when:
- ✅ File ends with `.yaml` extension
- ✅ File is in the `pipelines/` directory
- ✅ Unique `pipeline_name` field

Simply refresh the dashboard to see new pipelines.

## Troubleshooting

### Pipeline not appearing?
- Check file extension is `.yaml` (not `.yml`)
- Verify file is in `pipelines/` directory
- Refresh the browser (Ctrl+R or Cmd+R)
- Check dashboard for error messages in red

### Pipeline fails to load?
- Click on the error message to see full details
- Fix the validation error (usually missing required fields)
- Operator often highlights the exact issue
- Refresh when fixed

### Can't run pipeline?
- Check all connection credentials exist
- Verify plugin dependencies installed
- Check logs for detailed error messages
- Review task configuration against plugin docs

## Best Practices

1. **Use descriptive names**
   ```yaml
   pipeline_name: "Daily Sales Report Generation"  # ✓ Good
   pipeline_name: "report"                         # ✗ Too vague
   ```

2. **Add descriptions**
   ```yaml
   description: "Aggregate sales data and email report daily at 9 AM"
   ```

3. **Organize with IDs**
   ```yaml
   id: fetch_sales      # ✓ Clear, unambiguous
   id: task1            # ✗ Unclear, use descriptive names
   ```

4. **Use task dependencies**
   ```yaml
   depends_on: [fetch_data]  # Clear dependency chain
   ```

5. **Set appropriate timeouts**
   ```yaml
   timeout: 300  # 5 minutes for API calls
   ```

6. **Version your pipelines**
   Add date or version: `sales_pipeline_v2_2024_04.yaml`

## Need Help?

- See [PLUGINS_GUIDE.md](../PLUGINS_GUIDE.md) for plugin reference
- See [INTEGRATION_EXAMPLES.md](../INTEGRATION_EXAMPLES.md) for example pipelines
- See [README.md](../README.md) for quick start

Start building! 🚀
