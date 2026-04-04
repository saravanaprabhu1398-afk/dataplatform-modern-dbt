"""
Plugin Registry - Documents all available executor and transformer plugins.
This registry helps users discover available plugins and their capabilities.
"""

EXECUTOR_PLUGINS = {
    "duckdb": {
        "name": "DuckDB Executor",
        "description": "In-memory SQL database for analytical queries",
        "operations": ["load", "query", "aggregate", "validate", "transform"],
        "use_cases": ["Local data analytics", "CSV processing", "Data validation"],
        "dependencies": ["duckdb"],
        "config_example": {
            "operation": "query",
            "sql": "SELECT COUNT(*) FROM table"
        }
    },
    "snowflake": {
        "name": "Snowflake Executor",
        "description": "Cloud data warehouse for scalable analytics",
        "operations": ["load", "query", "execute"],
        "use_cases": ["Cloud data warehouse", "Large-scale analytics", "Data aggregation"],
        "dependencies": ["snowflake-connector-python"],
        "config_example": {
            "operation": "load",
            "table_name": "my_table",
            "data": []
        }
    },
    "postgres": {
        "name": "PostgreSQL Executor",
        "description": "Traditional relational database operations",
        "operations": ["execute", "query", "load", "extract"],
        "use_cases": ["RDBMS operations", "ETL pipelines", "Data warehousing"],
        "dependencies": ["psycopg2"],
        "config_example": {
            "operation": "query",
            "connection": {
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "user": "postgres"
            },
            "sql": "SELECT * FROM users"
        }
    },
    "mysql": {
        "name": "MySQL/MariaDB Executor",
        "description": "MySQL database operations",
        "operations": ["execute", "query", "bulk_insert"],
        "use_cases": ["MySQL/MariaDB integration", "Legacy system migration"],
        "dependencies": ["mysql-connector-python"],
        "config_example": {
            "operation": "query",
            "connection": {
                "host": "localhost",
                "port": 3306,
                "database": "mydb"
            }
        }
    },
    "api": {
        "name": "HTTP/REST API Executor",
        "description": "Make HTTP requests and API calls",
        "operations": ["GET", "POST", "PUT", "DELETE", "PATCH"],
        "use_cases": ["Webhooks", "Microservice integration", "Data ingestion"],
        "dependencies": ["requests"],
        "config_example": {
            "method": "POST",
            "url": "https://api.example.com/endpoint",
            "json": {"data": "value"}
        }
    },
    "file": {
        "name": "File Operations Executor",
        "description": "Local file manipulation and processing",
        "operations": ["read", "create", "append", "copy", "move", "delete", "merge", "list"],
        "use_cases": ["File processing", "Data staging", "Backup operations"],
        "dependencies": [],
        "config_example": {
            "operation": "read",
            "file_path": "/path/to/file.txt"
        }
    },
    "shell": {
        "name": "Shell/Bash Executor",
        "description": "Execute shell commands and scripts",
        "operations": ["execute"],
        "use_cases": ["System commands", "Custom scripts", "Infrastructure automation"],
        "dependencies": [],
        "config_example": {
            "command": "bash script.sh",
            "shell": "bash"
        }
    },
    "python": {
        "name": "Python Executor",
        "description": "Execute Python code and scripts",
        "operations": ["execute_code", "run_script"],
        "use_cases": ["Custom data processing", "Complex logic", "ML model execution"],
        "dependencies": [],
        "config_example": {
            "operation": "execute_code",
            "code": "result = 2 + 2"
        }
    },
    "email": {
        "name": "Email Executor",
        "description": "Send email notifications",
        "operations": ["send"],
        "use_cases": ["Pipeline alerts", "Status notifications", "Report distribution"],
        "dependencies": [],
        "config_example": {
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587,
            "sender_email": "sender@example.com",
            "recipients": ["user@example.com"],
            "subject": "Pipeline Alert"
        }
    },
    "spark": {
        "name": "Apache Spark Executor",
        "description": "Distributed data processing framework",
        "operations": ["submit_job", "sql_query", "dataframe_transform"],
        "use_cases": ["Large-scale processing", "Distributed computing", "ML pipelines"],
        "dependencies": ["pyspark"],
        "config_example": {
            "operation": "sql_query",
            "spark_master": "spark://localhost:7077",
            "sql": "SELECT * FROM my_table"
        }
    },
    "kafka": {
        "name": "Apache Kafka Executor",
        "description": "Event streaming and message queue operations",
        "operations": ["publish", "consume", "create_topic"],
        "use_cases": ["Event streaming", "Real-time data", "Message queue"],
        "dependencies": ["kafka-python"],
        "config_example": {
            "operation": "publish",
            "brokers": ["localhost:9092"],
            "topic": "events",
            "message": {"event": "data"}
        }
    },
    "bigquery": {
        "name": "Google BigQuery Executor",
        "description": "Google Cloud data warehouse",
        "operations": ["query", "load", "export"],
        "use_cases": ["Cloud analytics", "Google Cloud integration", "Serverless warehouse"],
        "dependencies": ["google-cloud-bigquery"],
        "config_example": {
            "operation": "query",
            "project_id": "my-project",
            "sql": "SELECT * FROM dataset.table"
        }
    }
}

TRANSFORMER_PLUGINS = {
    "duckdb": {
        "name": "DuckDB Transformer",
        "description": "Transform data using DuckDB SQL",
        "use_cases": ["Data cleaning", "Aggregation", "Enrichment"],
    }
}


def get_plugin_info(plugin_type: str, plugin_name: str) -> dict:
    """Get information about a specific plugin."""
    if plugin_type == "executor":
        return EXECUTOR_PLUGINS.get(plugin_name)
    elif plugin_type == "transformer":
        return TRANSFORMER_PLUGINS.get(plugin_name)
    return None


def list_executor_plugins() -> list:
    """List all available executor plugins."""
    return list(EXECUTOR_PLUGINS.keys())


def list_transformer_plugins() -> list:
    """List all available transformer plugins."""
    return list(TRANSFORMER_PLUGINS.keys())


def get_plugin_by_operation(operation: str) -> list:
    """Find plugins that support a given operation."""
    matching_plugins = []
    for name, info in EXECUTOR_PLUGINS.items():
        if operation in info.get("operations", []):
            matching_plugins.append(name)
    return matching_plugins
