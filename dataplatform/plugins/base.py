from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple


class Plugin(ABC):
    """Base class for all plugins."""

    @abstractmethod
    def execute(self, config: Dict[str, Any]) -> Tuple[bool, Any]:
        """Execute the plugin with given config.

        Returns a (success, data) tuple. success is True on success,
        False on failure. data is plugin-specific output (dict, list, etc.)
        or None.
        """
        pass


class ExecutorPlugin(Plugin):
    """Base class for executor plugins."""
    pass


class TransformerPlugin(Plugin):
    """Base class for transformer plugins."""
    pass