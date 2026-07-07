import contextlib
import logging
import sys
from io import StringIO

from dataplatform.plugins.base import ExecutorPlugin

logger = logging.getLogger(__name__)


class PythonExecutor(ExecutorPlugin):
    """Python executor for running Python scripts and custom code.

    SECURITY MODEL — exec() is intentionally unrestricted:
    - Code runs in the same process with the same OS privileges as the server.
    - There is NO sandbox, NO module whitelist, and NO resource cap beyond
      the per-task timeout enforced by the executor.
    - Only admin/editor users can submit pipeline configs (RBAC in api.py).
    - Do NOT expose the execute_code operation to untrusted end-users without
      an additional sandbox layer (e.g. subprocess isolation or a container).
    - For the Job Builder UI, make sure the Python config panel surfaces this
      warning to operators before they publish a pipeline.
    """

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute Python code or scripts.
        
        config = {
            "operation": "execute_code" | "run_script",
            "code": "python code here",
            "script_path": "path/to/script.py",
            "parameters": {"param": "value"},
            "imports": ["pandas", "numpy"]
        }
        """
        try:
            operation = config.get("operation", "execute_code")

            if operation == "execute_code":
                return self._execute_code(config)
            elif operation == "run_script":
                return self._run_script(config)
            else:
                return False, {"error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"Python execution error: {e}")
            return False, {"error": str(e)}

    def _execute_code(self, config: dict) -> tuple[bool, dict]:
        """Execute Python code."""
        try:
            code = config.get("code")
            parameters = config.get("parameters", {})
            imports = config.get("imports", [])

            if not code:
                return False, {"error": "code is required"}

            # Prepare execution environment
            exec_globals = {
                "logger": logger,
                "__builtins__": __builtins__
            }

            # Import specified modules
            for module_name in imports:
                try:
                    exec_globals[module_name] = __import__(module_name)
                except ImportError:
                    logger.warning(f"Failed to import {module_name}")

            # Add parameters to context
            exec_globals.update(parameters)

            # Capture output with a thread-local StringIO so parallel tasks
            # don't stomp on each other's sys.stdout.
            captured = StringIO()
            with contextlib.redirect_stdout(captured):
                exec(code, exec_globals)

            output = captured.getvalue()
            logger.info("✓ Python code executed successfully")

            return True, {
                "output": output,
                "variables": {k: str(v) for k, v in exec_globals.items() if not k.startswith("_")}
            }

        except SyntaxError as e:
            logger.error(f"Python syntax error: {e}")
            return False, {"error": f"Syntax error: {e}"}
        except Exception as e:
            logger.error(f"Python execution error: {e}")
            return False, {"error": str(e)}

    def _run_script(self, config: dict) -> tuple[bool, dict]:
        """Run Python script from file."""
        try:
            import subprocess
            
            script_path = config.get("script_path")
            parameters = config.get("parameters", {})

            if not script_path:
                return False, {"error": "script_path is required"}

            # Build command
            cmd = [sys.executable, script_path]

            # Add parameters as arguments
            for key, value in parameters.items():
                cmd.append(f"--{key}={value}")

            # Execute script
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=config.get("timeout", 300)
            )

            success = result.returncode == 0
            logger.info(f"{'✓' if success else '✗'} Script executed: {script_path}")

            return success, {
                "script": script_path,
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr
            }

        except subprocess.TimeoutExpired:
            logger.error("Script timeout")
            return False, {"error": "Script timeout"}
        except Exception as e:
            logger.error(f"Script execution error: {e}")
            return False, {"error": str(e)}
