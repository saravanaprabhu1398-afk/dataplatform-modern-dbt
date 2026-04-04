from abc import ABC, abstractmethod
from typing import Dict, Any


class Plugin(ABC):
    """Base class for all plugins."""

    @abstractmethod
    def execute(self, config: Dict[str, Any]) -> bool:
        """Execute the plugin with given config."""
        pass


class ExecutorPlugin(Plugin):
    """Base class for executor plugins."""
    pass


class TransformerPlugin(Plugin):
    """Base class for transformer plugins."""
    pass