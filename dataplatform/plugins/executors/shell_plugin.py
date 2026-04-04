import subprocess
import logging
import os

logger = logging.getLogger(__name__)


class ShellExecutor:
    """Shell/Bash command executor for running shell scripts and commands."""

    def execute(self, config: dict) -> tuple[bool, dict]:
        """
        Execute shell commands.
        
        config = {
            "command": "bash script.sh",
            "shell": "bash" (default),
            "cwd": "/path/to/workdir",
            "env": {"VAR": "value"},
            "timeout": 300
        }
        """
        try:
            command = config.get("command")
            shell = config.get("shell", "bash")
            cwd = config.get("cwd")
            env = config.get("env", {})
            timeout = config.get("timeout", 300)

            if not command:
                return False, {"error": "command is required"}

            # Merge with existing environment
            exec_env = os.environ.copy()
            exec_env.update(env)

            logger.info(f"Executing shell command: {command}")

            result = subprocess.run(
                command,
                shell=True,
                cwd=cwd,
                env=exec_env,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            success = result.returncode == 0
            status_msg = "✓ Command executed successfully" if success else f"✗ Command failed with code {result.returncode}"
            logger.info(status_msg)

            output = result.stdout.strip()
            error = result.stderr.strip()

            return success, {
                "command": command,
                "return_code": result.returncode,
                "stdout": output,
                "stderr": error
            }

        except subprocess.TimeoutExpired:
            logger.error(f"Command timeout after {timeout} seconds")
            return False, {"error": f"Command timeout after {timeout} seconds"}
        except Exception as e:
            logger.error(f"Shell execution error: {e}")
            return False, {"error": str(e)}
