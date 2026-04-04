import mysql.connector
import logging

logger = logging.getLogger(__name__)


class MySQLExecutor:
    """MySQL/MariaDB executor for database operations."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute MySQL operations.
        
        config = {
            "operation": "execute" | "query" | "bulk_insert",
            "connection": {
                "host": "localhost",
                "port": 3306,
                "user": "root",
                "password": "password",
                "database": "mydb"
            },
            "sql": "SELECT * FROM table",
            ...
        }
        """
        try:
            operation = config.get("operation", "query")

            if operation == "execute":
                return self._execute_sql(config)
            elif operation == "query":
                return self._query_data(config)
            elif operation == "bulk_insert":
                return self._bulk_insert(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"MySQL error: {e}")
            return False, {"error": str(e)}

    def _get_connection(self, config: dict):
        """Create MySQL connection."""
        conn_config = config.get("connection", {})
        return mysql.connector.connect(
            host=conn_config.get("host", "localhost"),
            port=conn_config.get("port", 3306),
            user=conn_config.get("user"),
            password=conn_config.get("password"),
            database=conn_config.get("database")
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
            cursor = conn.cursor(dictionary=True)

            sql = config.get("sql")
            if not sql:
                return False, {"error": "No SQL provided"}

            cursor.execute(sql)
            results = cursor.fetchall()

            logger.info(f"✓ Fetched {len(results)} rows")

            return True, results

        except Exception as e:
            logger.error(f"Failed to query data: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()

    def _bulk_insert(self, config: dict) -> tuple[bool, dict]:
        """Bulk insert data from CSV file."""
        conn = None
        try:
            conn = self._get_connection(config)
            cursor = conn.cursor()

            file_path = config.get("file_path")
            table_name = config.get("table_name")
            columns = config.get("columns", [])

            if not file_path or not table_name:
                return False, {"error": "file_path and table_name required"}

            # Read CSV and insert
            import csv
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = 0
                for row in reader:
                    values = tuple(row[col] for col in columns)
                    placeholders = ", ".join(["%s"] * len(values))
                    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(sql, values)
                    rows += 1
                    if rows % 1000 == 0:
                        conn.commit()

            conn.commit()
            logger.info(f"✓ Inserted {rows} rows")

            return True, {"rows_inserted": rows}

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Failed to bulk insert: {e}")
            return False, {"error": str(e)}
        finally:
            if conn:
                conn.close()
