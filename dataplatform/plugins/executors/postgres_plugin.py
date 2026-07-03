import psycopg2
from psycopg2 import sql as _sql
import logging
from dataplatform.plugins.base import ExecutorPlugin

logger = logging.getLogger(__name__)


class PostgresExecutor(ExecutorPlugin):
    """PostgreSQL executor for database operations."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute PostgreSQL operations.
        
        config = {
            "operation": "execute" | "query" | "load" | "extract",
            "connection": {
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "user": "postgres",
                "password": "password"
            },
            ... operation-specific fields ...
        }
        """
        try:
            operation = config.get("operation", "query")
            
            if operation == "execute":
                return self._execute_sql(config)
            elif operation == "query":
                return self._query_data(config)
            elif operation == "load":
                return self._load_data(config)
            elif operation == "extract":
                return self._extract_data(config)
            else:
                logger.error(f"Unknown operation: {operation}")
                return False, None

        except Exception as e:
            logger.error(f"PostgreSQL error: {e}")
            return False, None

    def _get_connection(self, config: dict):
        """Create PostgreSQL connection."""
        conn_config = config.get("connection", {})
        return psycopg2.connect(
            host=conn_config.get("host", "localhost"),
            port=conn_config.get("port", 5432),
            database=conn_config.get("database"),
            user=conn_config.get("user"),
            password=conn_config.get("password")
        )

    def _execute_sql(self, config: dict) -> tuple[bool, dict]:
        """Execute SQL statement (INSERT, UPDATE, DELETE)."""
        conn = None
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            sql = config.get("sql")
            if not sql:
                return False, {"error": "No SQL provided"}
            
            cursor.execute(sql)
            conn.commit()
            
            affected_rows = cursor.rowcount
            logger.info(f"✓ Executed SQL, affected rows: {affected_rows}")
            
            return True, {"affected_rows": affected_rows}
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to execute SQL: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()

    def _query_data(self, config: dict) -> tuple[bool, list]:
        """Execute SELECT query and return data."""
        conn = None
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            sql = config.get("sql")
            if not sql:
                return False, {"error": "No SQL provided"}
            
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            results = [dict(zip(columns, row)) for row in rows]
            logger.info(f"✓ Fetched {len(results)} rows")
            
            return True, results
            
        except Exception as e:
            logger.error(f"Failed to query data: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()

    def _load_data(self, config: dict) -> tuple[bool, dict]:
        """Load data from file into PostgreSQL table."""
        conn = None
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            file_path = config.get("file_path")
            table_name = config.get("table_name")
            
            if not file_path or not table_name:
                return False, {"error": "file_path and table_name required"}
            
            # Use COPY for CSV files — Identifier quotes the name to prevent SQL injection
            parts = table_name.split(".", 1)
            copy_sql = _sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
                _sql.Identifier(*parts)
            )
            with open(file_path, 'r') as f:
                cursor.copy_expert(copy_sql, f)
            conn.commit()
            
            logger.info(f"✓ Loaded data from {file_path} into {table_name}")
            return True, {"file": file_path, "table": table_name}
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to load data: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()

    def _extract_data(self, config: dict) -> tuple[bool, dict]:
        """Extract data from PostgreSQL to file."""
        conn = None
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()
            
            sql = config.get("sql")
            output_file = config.get("output_file")
            
            if not sql or not output_file:
                return False, {"error": "sql and output_file required"}
            
            with open(output_file, 'w') as out_f:
                cursor.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", out_f)
            
            logger.info(f"✓ Extracted data to {output_file}")
            return True, {"file": output_file}
            
        except Exception as e:
            logger.error(f"Failed to extract data: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()
