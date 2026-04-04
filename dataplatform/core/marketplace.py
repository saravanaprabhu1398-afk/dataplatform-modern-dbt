import requests
import subprocess
import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import json

logger = logging.getLogger(__name__)


class PluginRegistry:
    """Simple plugin registry for available plugins."""

    def __init__(self):
        self.plugins = {
            "duckdb": {
                "name": "DuckDB Executor",
                "type": "executor",
                "description": "Execute data processing with DuckDB",
                "package": "duckdb",
                "installed": True  # Built-in
            },
            "dbt": {
                "name": "DBT Transformer",
                "type": "transformer",
                "description": "Transform data using DBT",
                "package": "dbt-core",
                "installed": True  # Built-in
            },
            "pandas": {
                "name": "Pandas Executor",
                "type": "executor",
                "description": "Execute data processing with Pandas",
                "package": "pandas",
                "installed": False
            },
            "spark": {
                "name": "Spark Executor",
                "type": "executor",
                "description": "Execute data processing with Apache Spark",
                "package": "pyspark",
                "installed": False
            }
        }

    def list_plugins(self) -> Dict[str, Dict]:
        """List all available plugins."""
        return self.plugins

    def get_plugin(self, name: str) -> Optional[Dict]:
        """Get plugin information."""
        return self.plugins.get(name)

    def is_installed(self, name: str) -> bool:
        """Check if plugin is installed."""
        plugin = self.get_plugin(name)
        if not plugin:
            return False

        if plugin["installed"]:
            return True

        # Check if package is actually installed
        try:
            __import__(plugin["package"].replace("-", "_"))
            return True
        except ImportError:
            return False

    def install_plugin(self, name: str) -> bool:
        """Install a plugin."""
        plugin = self.get_plugin(name)
        if not plugin:
            logger.error(f"Plugin {name} not found in registry")
            return False

        if self.is_installed(name):
            logger.info(f"Plugin {name} is already installed")
            return True

        try:
            logger.info(f"Installing plugin {name}...")

            # Install package
            result = subprocess.run([
                sys.executable, "-m", "pip", "install", plugin["package"]
            ], capture_output=True, text=True)

            if result.returncode == 0:
                plugin["installed"] = True
                logger.info(f"Plugin {name} installed successfully")
                return True
            else:
                logger.error(f"Failed to install plugin {name}: {result.stderr}")
                return False

        except Exception as e:
            logger.error(f"Error installing plugin {name}: {e}")
            return False


# Global registry instance
registry = PluginRegistry()


def get_registry() -> PluginRegistry:
    """Get the global plugin registry."""
    return registry