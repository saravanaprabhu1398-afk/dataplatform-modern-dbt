import networkx as nx
from typing import List
from dataplatform.core.config import Task


class DAGBuilder:
    def __init__(self, tasks: List[Task]):
        self.tasks = {task.name: task for task in tasks}
        self.graph = nx.DiGraph()

    def build(self) -> nx.DiGraph:
        """Build DAG from tasks and dependencies."""
        # Add nodes
        for task_name in self.tasks:
            self.graph.add_node(task_name)

        # Add edges based on dependencies
        for task in self.tasks.values():
            if task.depends_on:
                for dep in task.depends_on:
                    self.graph.add_edge(dep, task.name)

        # Check for cycles
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Circular dependency detected in tasks")

        return self.graph

    def get_execution_order(self) -> List[str]:
        """Get tasks in topological order."""
        return list(nx.topological_sort(self.graph))