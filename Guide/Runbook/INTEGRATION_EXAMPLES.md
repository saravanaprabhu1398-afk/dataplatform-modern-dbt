"""
Plugin Integration Examples - Real-world use cases demonstrating the plugin ecosystem.

This file shows practical examples of how to use different plugins together
in realistic data platform scenarios.
"""

# Example 1: Data Lake Ingestion Pipeline
data_lake_pipeline = """
name: Data Lake Ingestion
description: Ingest data from APIs, validate, and load to multiple targets

tasks:
  # Fetch data from multiple APIs
  - id: fetch_sales_api
    type: executor
    plugin: api
    config:
      method: GET
      url: https://api.salesforce.com/v1/records
      headers:
        Authorization: Bearer {{ env.SALESFORCE_TOKEN }}
      retry_count: 3

  - id: fetch_webhook_data
    type: executor
    plugin: kafka
    config:
      operation: consume
      brokers: ["kafka1:9092", "kafka2:9092"]
      topic: events
      max_messages: 1000
    depends_on: [fetch_sales_api]

  # Validate data quality
  - id: validate_data
    type: executor
    plugin: duckdb
    config:
      operation: validate
      sql: "SELECT * FROM 'raw_data.parquet'"
      rules:
        - field: id
          type: not_null
        - field: amount
          type: numeric
        - field: date
          type: valid_date

  # Transform with Python/Pandas
  - id: transform_data
    type: executor
    plugin: python
    config:
      operation: execute_code
      code: |
        import pandas as pd
        import numpy as np
        
        df = pd.read_parquet('raw_data.parquet')
        df['revenue'] = df['quantity'] * df['price']
        df['date'] = pd.to_datetime(df['date'])
        df.to_parquet('transformed_data.parquet')
      imports: [pandas, numpy]
    depends_on: [validate_data]

  # Load to multiple destinations
  - id: load_snowflake
    type: executor
    plugin: snowflake
    config:
      operation: load
      connection:
        account: xy12345.us-east-1
        user: etl_user
        password: "{{ env.SF_PASSWORD }}"
      table_name: raw_events
      data: "{{ transform_data.output }}"
    depends_on: [transform_data]

  - id: load_postgres
    type: executor
    plugin: postgres
    config:
      operation: load
      connection:
        host: "{{ env.PG_HOST }}"
        database: analytics
        user: "{{ env.PG_USER }}"
      table_name: events_staging
      file_path: transformed_data.parquet
    depends_on: [transform_data]

  - id: load_bigquery
    type: executor
    plugin: bigquery
    config:
      operation: load
      project_id: analytics-project
      dataset_id: raw_data
      table_id: events
      source_file: transformed_data.parquet
    depends_on: [transform_data]

  # Send notifications
  - id: notify_success
    type: executor
    plugin: email
    config:
      smtp_server: smtp.gmail.com
      sender_email: "{{ env.ALERT_EMAIL }}"
      recipients: [data-team@company.com]
      subject: "Data Lake Ingestion - Success"
      body: |
        Pipeline completed successfully!
        
        - Records loaded to Snowflake: {{ load_snowflake.affected_rows }}
        - Records loaded to PostgreSQL: {{ load_postgres.rows_inserted }}
        - Records loaded to BigQuery: {{ load_bigquery.rows_loaded }}
    depends_on: [load_snowflake, load_postgres, load_bigquery]
"""

# Example 2: Real-time Analytics Pipeline
realtime_analytics_pipeline = """
name: Real-time Analytics Processing
description: Stream processing with Kafka and Spark

tasks:
  # Subscribe to event stream
  - id: subscribe_events
    type: executor
    plugin: kafka
    config:
      operation: consume
      brokers: ["kafka1:9092"]
      topic: user_events
      group_id: analytics_group
      max_messages: 5000
      timeout_ms: 30000

  # Process with Spark
  - id: spark_aggregation
    type: executor
    plugin: spark
    config:
      operation: sql_query
      spark_master: spark://spark-master:7077
      app_name: EventsProcessing
      sql: |
        SELECT 
          event_date,
          user_id,
          COUNT(*) as event_count,
          MAX(event_timestamp) as last_event
        FROM events
        GROUP BY event_date, user_id
    depends_on: [subscribe_events]

  # Load to Snowflake
  - id: load_metrics
    type: executor
    plugin: snowflake
    config:
      operation: load
      table_name: user_event_metrics
      data: "{{ spark_aggregation.output }}"
    depends_on: [spark_aggregation]

  # Publish enriched events back to Kafka
  - id: publish_metrics
    type: executor
    plugin: kafka
    config:
      operation: publish
      brokers: ["kafka1:9092"]
      topic: metrics
      message: "{{ load_metrics.output }}"
    depends_on: [load_metrics]
"""

# Example 3: Machine Learning Pipeline
ml_pipeline = """
name: ML Model Training Pipeline
description: Fetch data, prepare, train model, and deploy

tasks:
  # Query training data
  - id: fetch_training_data
    type: executor
    plugin: postgres
    config:
      operation: query
      connection:
        host: "{{ env.PG_HOST }}"
        database: ml_data
        user: ml_user
      sql: |
        SELECT features, label
        FROM training_data
        WHERE created_date >= CURRENT_DATE - 30
      output_file: data/training.csv

  # Data preparation
  - id: prepare_data
    type: executor
    plugin: python
    config:
      operation: execute_code
      code: |
        import pandas as pd
        from sklearn.preprocessing import StandardScaler
        
        df = pd.read_csv('data/training.csv')
        X = df.drop('label', axis=1)
        y = df['label']
        
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        import pickle
        pickle.dump(scaler, open('models/scaler.pkl', 'wb'))
        print(f"Prepared {len(df)} samples")
      imports: [pandas, scikit-learn]
    depends_on: [fetch_training_data]

  # Train model
  - id: train_model
    type: executor
    plugin: python
    config:
      operation: run_script
      script_path: scripts/train_model.py
      parameters:
        input_file: data/training.csv
        output_model: models/model_v2.pkl
        test_size: 0.2
    depends_on: [prepare_data]

  # Register model
  - id: register_model
    type: executor
    plugin: api
    config:
      method: POST
      url: https://model-registry.company.com/api/models
      json:
        name: fraud_detector
        version: "{{ timestamp }}"
        model_path: models/model_v2.pkl
        accuracy: "{{ train_model.accuracy }}"
      retry_count: 3
    depends_on: [train_model]

  # Notify ML team
  - id: notify_team
    type: executor
    plugin: email
    config:
      smtp_server: smtp.gmail.com
      sender_email: "{{ env.ALERT_EMAIL }}"
      recipients: [ml-team@company.com]
      subject: "Model Training Complete"
      attachments: ["models/metrics.csv"]
    depends_on: [register_model]
"""

# Example 4: ETL with Error Handling
etl_with_errors = """
name: Robust ETL Pipeline
description: ETL with comprehensive error handling and recovery

tasks:
  - id: extract_data
    type: executor
    plugin: api
    config:
      method: GET
      url: https://api.source.com/data
      retry_count: 5
      timeout: 120

  - id: validate_schema
    type: executor
    plugin: python
    config:
      operation: execute_code
      code: |
        import pandas as pd
        import jsonschema
        
        df = pd.read_json('data/extracted.json')
        schema = {
          'type': 'object',
          'properties': {
            'id': {'type': 'integer'},
            'name': {'type': 'string'},
            'amount': {'type': 'number'}
          }
        }
        
        # Validate each row
        for row in df.to_dict('records'):
          jsonschema.validate(row, schema)
        print(f"Schema validation passed for {len(df)} records")
    depends_on: [extract_data]

  - id: transform
    type: executor
    plugin: duckdb
    config:
      operation: transform
      sql: |
        SELECT 
          id,
          UPPER(name) as name,
          amount * 1.1 as adjusted_amount
        FROM 'data/extracted.json'
    depends_on: [validate_schema]

  - id: load
    type: executor
    plugin: snowflake
    config:
      operation: load
      table_name: staging_table
      data: "{{ transform.output }}"
    depends_on: [transform]

  # Error notification
  - id: error_notification
    type: executor
    plugin: email
    config:
      smtp_server: smtp.gmail.com
      sender_email: "{{ env.ALERT_EMAIL }}"
      recipients: [ops-team@company.com]
      subject: "ETL Pipeline Failed - Intervention Required"
      body: |
        Pipeline {{ pipeline_name }} failed at task {{ failed_task }}.
        Error: {{ error_message }}
        
        Action required: Review logs and fix issues before retry.
    # This would be triggered by error handler (implementation in progress)
"""

# Example 5: Data Quality Monitoring
quality_monitoring = """
name: Data Quality Dashboard
description: Monitor data quality across sources

tasks:
  - id: collect_metrics
    type: executor
    plugin: duckdb
    config:
      operation: aggregate
      sql: |
        SELECT 
          COUNT(*) as total_records,
          COUNT(DISTINCT id) as unique_ids,
          COUNT(CASE WHEN status IS NULL THEN 1 END) as null_status,
          MAX(created_date) as latest_date,
          MIN(created_date) as earliest_date
        FROM raw_data

  - id: calculate_profiles
    type: executor
    plugin: python
    config:
      operation: execute_code
      code: |
        import pandas as pd
        import json
        
        # Profile analysis
        df = pd.read_parquet('data/current.parquet')
        profile = {
          'row_count': len(df),
          'columns': len(df.columns),
          'missing_values': df.isnull().sum().to_dict(),
          'dtypes': df.dtypes.astype(str).to_dict()
        }
        
        with open('reports/profile.json', 'w') as f:
          json.dump(profile, f)
      imports: [pandas, json]
    depends_on: [collect_metrics]

  - id: export_dashboard
    type: executor
    plugin: bigquery
    config:
      operation: export
      project_id: company-analytics
      dataset_id: quality_metrics
      table_id: daily_reports
      destination_uri: gs://quality-reports/daily_{{ date }}.csv
    depends_on: [calculate_profiles]

  - id: publish_metrics
    type: executor
    plugin: kafka
    config:
      operation: publish
      brokers: ["kafka1:9092"]
      topic: quality_metrics
      message: "{{ calculate_profiles.output }}"
    depends_on: [calculate_profiles]
"""

print("✅ Plugin Integration Examples Created")
print("\nThese examples show real-world uses of the plugin ecosystem:")
print("- Data Lake Ingestion: Multi-source, multi-destination pipeline")
print("- Real-time Analytics: Kafka + Spark streaming")
print("- ML Pipeline: Complete model training workflow")
print("- ETL: Robust error handling and validation")
print("- Quality Monitoring: Data quality tracking")
print("\nSee PLUGINS_GUIDE.md for complete configuration reference.")
