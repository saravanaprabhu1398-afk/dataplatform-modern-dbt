import subprocess
import os
import shutil
from pathlib import Path
from dataplatform.plugins.base import TransformerPlugin


class DbtTransformer(TransformerPlugin):
    def execute(self, config: dict) -> bool:
        """Execute dbt transformations."""
        try:
            # Assume dbt project is in current directory or specified
            dbt_project_dir = config.get("dbt_project_dir", ".")

            # Find dbt executable
            dbt_cmd = shutil.which("dbt")
            if not dbt_cmd:
                # Try common locations
                possible_paths = [
                    "/usr/local/bin/dbt",
                    "/opt/homebrew/bin/dbt",
                    os.path.expanduser("~/.local/bin/dbt"),
                    os.path.expanduser("~/Library/Python/3.9/bin/dbt"),
                    os.path.expanduser("~/Library/Python/3.10/bin/dbt"),
                    os.path.expanduser("~/Library/Python/3.11/bin/dbt"),
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        dbt_cmd = path
                        break

            if not dbt_cmd:
                print("dbt command not found. Please install dbt-core or ensure it's in PATH")
                return False

            # Run dbt run
            result = subprocess.run(
                [dbt_cmd, "run"],
                cwd=dbt_project_dir,
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                print("DBT transformation completed successfully")
                print(result.stdout)
                return True
            else:
                print(f"DBT transformation failed: {result.stderr}")
                return False

        except FileNotFoundError:
            print("dbt command not found. Please install dbt-core")
            return False
        except Exception as e:
            print(f"DBT execution error: {e}")
            return False