import snowflake.connector
import pandas as pd
from dataplatform.plugins.base import ExecutorPlugin


class SnowflakeExecutor(ExecutorPlugin):
    """
    Snowflake executor for loading data into Snowflake database.

    Supports loading aggregated data from DuckDB results into Snowflake tables.
    """

    def execute(self, config: dict) -> tuple[bool, None]:
        """
        Execute Snowflake operation based on configuration.

        config = {
            "operation": "load_to_snowflake",
            "snowflake_config": {
                "account": "your_account.snowflakecomputing.com",
                "user": "your_user",
                "password": "your_password",
                "warehouse": "your_warehouse",
                "database": "your_database",
                "schema": "your_schema"
            },
            "table_name": "target_table_name",
            "previous_data": [...]  # Data from previous task (automatically passed)
        }
        """
        operation = config.get("operation", "load_to_snowflake")

        if operation == "load_to_snowflake":
            return self._load_to_snowflake(config)
        else:
            print(f"❌ Unsupported operation: {operation}")
            return False, None

    def _load_to_snowflake(self, config: dict) -> tuple[bool, None]:
        """
        Load data into Snowflake table.

        Config format:
        {
            "snowflake_config": {
                "account": "...",
                "user": "...",
                "password": "...",
                "warehouse": "...",
                "database": "...",
                "schema": "..."
            },
            "table_name": "target_table",
            "previous_data": [...],  # Data from previous task
            "columns": [...],  # Optional: column names
            "if_exists": "replace" | "append"  # replace or append to table
        }
        """
        snowflake_config = config.get("snowflake_config")
        table_name = config.get("table_name")
        data = config.get("previous_data", [])
        columns = config.get("columns", [])
        if_exists = config.get("if_exists", "replace")

        if not snowflake_config:
            print("❌ Error: snowflake_config required")
            return False, None

        if not table_name:
            print("❌ Error: table_name required")
            return False, None

        if not data:
            print("❌ Error: previous_data required to load")
            return False, None

        try:
            print(f"\n❄️  LOADING TO SNOWFLAKE: {table_name}")
            print("-" * 60)

            # Connect to Snowflake
            conn = snowflake.connector.connect(**snowflake_config)
            cursor = conn.cursor()

            # Create table if it doesn't exist or replace if specified
            if if_exists == "replace":
                self._create_or_replace_table(cursor, table_name, data, columns)

            # Insert data
            self._insert_data(cursor, table_name, data, columns)

            # Commit and close
            conn.commit()
            cursor.close()
            conn.close()

            print(f"✓ Successfully loaded {len(data)} rows to {table_name}")
            return True, None

        except Exception as e:
            print(f"❌ Snowflake load failed: {e}")
            return False, None

    def _create_or_replace_table(self, cursor, table_name: str, data: list, columns: list):
        """Create or replace Snowflake table based on data structure."""
        if not data:
            return

        # Infer column types from first row
        first_row = data[0]
        if not columns:
            # Generate column names if not provided
            columns = [f"col_{i+1}" for i in range(len(first_row))]

        # Infer types from data
        column_defs = []
        for i, col in enumerate(columns):
            sample_value = first_row[i] if i < len(first_row) else None
            col_type = self._infer_snowflake_type(sample_value)
            column_defs.append(f'"{col}" {col_type}')

        # Create table
        create_sql = f'CREATE OR REPLACE TABLE "{table_name}" ({", ".join(column_defs)})'
        cursor.execute(create_sql)
        print(f"✓ Created/replaced table {table_name}")

    def _insert_data(self, cursor, table_name: str, data: list, columns: list):
        """Insert data into Snowflake table."""
        if not data:
            return

        if not columns:
            # Generate column names if not provided
            columns = [f"col_{i+1}" for i in range(len(data[0]))]

        # Prepare INSERT statement
        quoted_columns = [f'"{col}"' for col in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f'INSERT INTO "{table_name}" ({", ".join(quoted_columns)}) VALUES ({placeholders})'

        # Insert data in batches
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            print(f"✓ Inserted batch {i//batch_size + 1} ({len(batch)} rows)")

    def _infer_snowflake_type(self, value) -> str:
        """Infer Snowflake column type from Python value."""
        if value is None:
            return "VARCHAR(255)"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            # Estimate string length
            if len(value) > 1000:
                return "VARCHAR(4000)"
            elif len(value) > 100:
                return "VARCHAR(1000)"
            else:
                return "VARCHAR(255)"
        else:
            # Default to string for unknown types
            return "VARCHAR(255)"