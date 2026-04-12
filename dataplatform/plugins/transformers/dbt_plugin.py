import logging
import os
import shutil
import subprocess
from typing import Optional, Tuple, Dict, Any

from dataplatform.plugins.base import TransformerPlugin

logger = logging.getLogger(__name__)

_SUPPORTED_OPERATIONS = {"run", "test", "compile", "seed", "snapshot", "docs", "ls"}
_SELECT_SUPPORTED = {"run", "test", "compile", "ls"}


class DbtTransformer(TransformerPlugin):
    def execute(self, config: dict) -> Tuple[bool, Dict[str, Any]]:
        """Execute a dbt operation.

        Supported operations: run, test, compile, seed, snapshot, docs, ls.
        Config keys:
          - operation (str): dbt subcommand, default "run"
          - project_dir / dbt_project_dir (str): path to dbt project, default "."
          - profiles_dir (str, optional): path to profiles directory
          - select (str, optional): model selector (for run/test/compile/ls)
        """
        operation = config.get("operation", "run")
        if operation not in _SUPPORTED_OPERATIONS:
            msg = f"Unsupported dbt operation '{operation}'. Supported: {sorted(_SUPPORTED_OPERATIONS)}"
            logger.error(msg)
            return False, {"error": msg}

        dbt_cmd = self._find_dbt_binary()
        if not dbt_cmd:
            msg = "dbt command not found. Install dbt-core or ensure it is in PATH."
            logger.error(msg)
            return False, {"error": msg}

        project_dir = config.get("project_dir") or config.get("dbt_project_dir", ".")
        profiles_dir: Optional[str] = config.get("profiles_dir")
        select: Optional[str] = config.get("select")

        cmd = [dbt_cmd, operation]

        if operation == "docs":
            cmd.append("generate")

        if profiles_dir:
            cmd += ["--profiles-dir", profiles_dir]

        if select and operation in _SELECT_SUPPORTED:
            cmd += ["--select", select]

        return self._run_dbt_command(cmd, project_dir)

    def _find_dbt_binary(self) -> Optional[str]:
        """Locate the dbt executable on the system."""
        dbt_cmd = shutil.which("dbt")
        if dbt_cmd:
            return dbt_cmd

        possible_paths = [
            "/usr/local/bin/dbt",
            "/opt/homebrew/bin/dbt",
            os.path.expanduser("~/.local/bin/dbt"),
            os.path.expanduser("~/Library/Python/3.9/bin/dbt"),
            os.path.expanduser("~/Library/Python/3.10/bin/dbt"),
            os.path.expanduser("~/Library/Python/3.11/bin/dbt"),
            os.path.expanduser("~/Library/Python/3.12/bin/dbt"),
        ]
        for path in possible_paths:
            if os.path.exists(path):
                return path

        return None

    def _run_dbt_command(self, cmd: list, cwd: str) -> Tuple[bool, Dict[str, Any]]:
        """Run a dbt CLI command and return (success, result_dict)."""
        try:
            result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
            success = result.returncode == 0
            if success:
                logger.info(f"dbt command succeeded: {' '.join(cmd)}")
            else:
                logger.error(f"dbt command failed (rc={result.returncode}): {result.stderr[:500]}")
            return success, {
                "command": " ".join(cmd),
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        except FileNotFoundError as exc:
            msg = f"dbt binary not found during execution: {exc}"
            logger.error(msg)
            return False, {"error": msg}
        except Exception as exc:
            msg = f"dbt execution error: {exc}"
            logger.error(msg)
            return False, {"error": msg}
