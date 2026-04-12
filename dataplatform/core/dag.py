import networkx as nx
from typing import List
from dataplatform.core.config import Task


class DAGBuilder:
    def __init__(self, tasks: List[Task]):
        self.tasks = {(task.id or task.name): task for task in tasks}
        self.graph = nx.DiGraph()

    def build(self) -> nx.DiGraph:
        """Build DAG from tasks and dependencies."""
        # Add nodes
        for task_name in self.tasks:
            self.graph.add_node(task_name)

        # Add edges based on dependencies
        for task_key, task in self.tasks.items():
            if task.depends_on:
                for dep in task.depends_on:
                    self.graph.add_edge(dep, task_key)

        # Check for cycles
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