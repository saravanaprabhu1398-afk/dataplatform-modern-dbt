import subprocess
import logging
import os
import shlex

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
            cwd = config.get("cwd")
            env = config.get("env", {})
            timeout = config.get("timeout", 300)
            use_shell = bool(config.get("use_shell", False))

            if not command:
                return False, {"error": "command is required"}

            # Merge with existing environment
            exec_env = os.environ.copy()
            exec_env.update(env)

            logger.info(f"Executing shell command: {command}")

            if isinstance(command, str):
                command_to_run = command if use_shell else shlex.split(command)
            elif isinstance(command, list):
                command_to_run = command
            else:
                return False, {"error": "command must be a string or list"}

            result = subprocess.run(
                command_to_run,
                shell=use_shell,
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
                "command": command if isinstance(command, str) else " ".join(command),
                "return_code": result.returncode,
                "stdout": output,
                "stderr": error,
                "used_shell": use_shell,
            }

        except subprocess.TimeoutExpired:
            logger.error(f"Command timeout after {timeout} seconds")
            return False, {"error": f"Command timeout after {timeout} seconds"}
        except Exception as e:
            logger.error(f"Shell execution error: {e}")
            return False, {"error": str(e)}
