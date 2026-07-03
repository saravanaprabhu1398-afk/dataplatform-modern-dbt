import networkx as nx
from typing import Dict, List
from dataplatform.core.config import Task


class DAGBuilder:
    def __init__(self, tasks: List[Task]):
        # Key by task.name — the stable canonical identifier used by executors.
        self.tasks: Dict[str, Task] = {task.name: task for task in tasks}
        # Alias map: both task.id and task.name → task.name so that
        # depends_on references using either id or name resolve correctly.
        self._alias: Dict[str, str] = {}
        for task in tasks:
            self._alias[task.name] = task.name
            if task.id:
                self._alias[task.id] = task.name
        self.graph = nx.DiGraph()

    def _canonical(self, ref: str) -> str:
        """Translate a depends_on ref (id or name) to the canonical task.name key."""
        return self._alias.get(ref, ref)

    def build(self) -> nx.DiGraph:
        """Build DAG from tasks and dependencies."""
        for task_name in self.tasks:
            self.graph.add_node(task_name)

        for task_name, task in self.tasks.items():
            if task.depends_on:
                for dep in task.depends_on:
                    self.graph.add_edge(self._canonical(dep), task_name)

        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Circular dependency detected in tasks")

        return self.graph

    def get_execution_order(self) -> List[str]:
        """Get tasks in topological order."""
        return list(nx.topological_sort(self.graph))

    def get_execution_waves(self) -> List[List[str]]:
        """Return tasks grouped into independent waves for concurrent execution.

        Each inner list contains tasks that have no dependency on each other
        and can run in parallel. Tasks within a wave only depend on tasks from
        earlier waves.
        """
        return [list(generation) for generation in nx.topological_generations(self.graph)]
