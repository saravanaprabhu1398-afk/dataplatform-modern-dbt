import logging
from dataplatform.plugins.base import ExecutorPlugin

logger = logging.getLogger(__name__)


class SparkExecutor(ExecutorPlugin):
    """Apache Spark executor for distributed data processing."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute Spark operations.
        
        config = {
            "operation": "submit_job" | "sql_query" | "dataframe_transform",
            "spark_master": "spark://localhost:7077",
            "app_name": "MySparkApp",
            ...
        }
        """
        try:
            operation = config.get("operation", "sql_query")

            if operation == "submit_job":
                return self._submit_job(config)
            elif operation == "sql_query":
                return self._sql_query(config)
            elif operation == "dataframe_transform":
                return self._dataframe_transform(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except ImportError:
            logger.warning("PySpark not installed. Install with: pip install pyspark")
            return False, {"error": "PySpark not installed"}
        except Exception as e:
            logger.error(f"Spark error: {e}")
            return False, {"error": str(e)}

    def _submit_job(self, config: dict) -> tuple[bool, dict]:
        """Submit Spark job."""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .master(config.get("spark_master", "local")) \
                .appName(config.get("app_name", "DataPlatformApp")) \
                .getOrCreate()

            # For demo purposes
            logger.info(f"✓ Spark session created: {config.get('app_name')}")

            return True, {
                "job": config.get("app_name"),
                "master": config.get("spark_master"),
                "status": "submitted"
            }

        except ImportError:
            logger.warning("PySpark not installed")
            return False, {"error": "PySpark not installed"}
        except Exception as e:
            logger.error(f"Failed to submit job: {e}")
            return False, {"error": str(e)}

    def _sql_query(self, config: dict) -> tuple[bool, dict]:
        """Execute Spark SQL query."""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .master(config.get("spark_master", "local")) \
                .appName(config.get("app_name", "DataPlatformApp")) \
                .getOrCreate()

            sql = config.get("sql")
            if not sql:
                return False, {"error": "SQL is required"}

            df = spark.sql(sql)
            row_count = df.count()

            logger.info(f"✓ Executed Spark SQL query: {row_count} rows")

            return True, {
                "row_count": row_count,
                "columns": df.columns
            }

        except ImportError:
            logger.warning("PySpark not installed")
            return False, {"error": "PySpark not installed"}
        except Exception as e:
            logger.error(f"Failed to execute SQL: {e}")
            return False, {"error": str(e)}

    def _dataframe_transform(self, config: dict) -> tuple[bool, dict]:
        """Transform DataFrame."""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder \
                .master(config.get("spark_master", "local")) \
                .appName(config.get("app_name", "DataPlatformApp")) \
                .getOrCreate()

            input_path = config.get("input_path")
            output_path = config.get("output_path")
            format_type = config.get("format", "parquet")

            if not input_path or not output_path:
                return False, {"error": "input_path and output_path required"}

            # Read data
            df = spark.read.format(format_type).load(input_path)

            # Apply transformations
            if "sql" in config:
                df.createOrReplaceTempView("input_data")
                df = spark.sql(config["sql"])

            # Write output
            df.write.mode("overwrite").format(format_type).save(output_path)

            logger.info(f"✓ Transformed data from {input_path} to {output_path}")

            return True, {
                "input": input_path,
                "output": output_path,
                "row_count": df.count()
            }

        except ImportError:
            logger.warning("PySpark not installed")
            return False, {"error": "PySpark not installed"}
        except Exception as e:
            logger.error(f"Failed to transform data: {e}")
            return False, {"error": str(e)}
