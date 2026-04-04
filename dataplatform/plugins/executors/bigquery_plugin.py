import logging

logger = logging.getLogger(__name__)


class BigQueryExecutor:
    """Google BigQuery executor for cloud data warehouse operations."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute BigQuery operations.
        
        config = {
            "operation": "query" | "load" | "export",
            "project_id": "my-project",
            "dataset_id": "my_dataset",
            "table_id": "my_table",
            "credentials_path": "/path/to/credentials.json",
            ...
        }
        """
        try:
            operation = config.get("operation", "query")

            if operation == "query":
                return self._query_data(config)
            elif operation == "load":
                return self._load_data(config)
            elif operation == "export":
                return self._export_data(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except ImportError:
            logger.warning("google-cloud-bigquery not installed. Install with: pip install google-cloud-bigquery")
            return False, {"error": "google-cloud-bigquery not installed"}
        except Exception as e:
            logger.error(f"BigQuery error: {e}")
            return False, {"error": str(e)}

    def _query_data(self, config: dict) -> tuple[bool, dict]:
        """Execute BigQuery SQL query."""
        try:
            from google.cloud import bigquery

            credentials_path = config.get("credentials_path")
            project_id = config.get("project_id")
            query = config.get("sql")

            if not query:
                return False, {"error": "sql is required"}

            # Initialize client
            if credentials_path:
                client = bigquery.Client(project=project_id, credentials=credentials_path)
            else:
                client = bigquery.Client(project=project_id)

            # Execute query
            query_job = client.query(query)
            results = query_job.result()

            # Convert to list of dicts
            rows = [dict(row) for row in results]

            logger.info(f"✓ Executed BigQuery query: {len(rows)} rows")

            return True, {
                "project": project_id,
                "rows_count": len(rows),
                "data": rows[:100]  # Return first 100 rows
            }

        except ImportError:
            logger.warning("google-cloud-bigquery not installed")
            return False, {"error": "google-cloud-bigquery not installed"}
        except Exception as e:
            logger.error(f"Failed to query: {e}")
            return False, {"error": str(e)}

    def _load_data(self, config: dict) -> tuple[bool, dict]:
        """Load data into BigQuery table."""
        try:
            from google.cloud import bigquery

            credentials_path = config.get("credentials_path")
            project_id = config.get("project_id")
            dataset_id = config.get("dataset_id")
            table_id = config.get("table_id")
            source_file = config.get("source_file")

            if not all([project_id, dataset_id, table_id, source_file]):
                return False, {"error": "project_id, dataset_id, table_id, source_file required"}

            # Initialize client
            if credentials_path:
                client = bigquery.Client(project=project_id, credentials=credentials_path)
            else:
                client = bigquery.Client(project=project_id)

            table_id = f"{project_id}.{dataset_id}.{table_id}"

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = True

            with open(source_file, "rb") as source_file_obj:
                load_job = client.load_table_from_file(
                    source_file_obj,
                    table_id,
                    job_config=job_config
                )

            load_job.result()

            logger.info(f"✓ Loaded data into {table_id}")

            return True, {
                "table": table_id,
                "rows_loaded": load_job.output_rows
            }

        except ImportError:
            logger.warning("google-cloud-bigquery not installed")
            return False, {"error": "google-cloud-bigquery not installed"}
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False, {"error": str(e)}

    def _export_data(self, config: dict) -> tuple[bool, dict]:
        """Export BigQuery table to GCS."""
        try:
            from google.cloud import bigquery

            credentials_path = config.get("credentials_path")
            project_id = config.get("project_id")
            dataset_id = config.get("dataset_id")
            table_id = config.get("table_id")
            destination_uri = config.get("destination_uri")  # gs://bucket/path

            if not all([project_id, dataset_id, table_id, destination_uri]):
                return False, {"error": "project_id, dataset_id, table_id, destination_uri required"}

            # Initialize client
            if credentials_path:
                client = bigquery.Client(project=project_id, credentials=credentials_path)
            else:
                client = bigquery.Client(project=project_id)

            table_id = f"{project_id}.{dataset_id}.{table_id}"

            job_config = bigquery.ExtractJobConfig()
            job_config.destination_format = bigquery.DestinationFormat.CSV

            extract_job = client.extract_table(
                table_id,
                destination_uri,
                job_config=job_config
            )

            extract_job.result()

            logger.info(f"✓ Exported {table_id} to {destination_uri}")

            return True, {
                "table": table_id,
                "destination": destination_uri
            }

        except ImportError:
            logger.warning("google-cloud-bigquery not installed")
            return False, {"error": "google-cloud-bigquery not installed"}
        except Exception as e:
            logger.error(f"Failed to export: {e}")
            return False, {"error": str(e)}
