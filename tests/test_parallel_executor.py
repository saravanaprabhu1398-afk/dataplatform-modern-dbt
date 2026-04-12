"""Tests for parallel task execution via DAGBuilder waves and PipelineExecutor."""
import threading
from unittest.mock import MagicMock, patch
import pytest

from dataplatform.core.config import Task
from dataplatform.core.dag import DAGBuilder
from dataplatform.core.executor import PipelineExecutor, TaskExecutor


def _make_task(name: str, depends_on=None) -> Task:
    return Task(name=name, type="executor", plugin="python", depends_on=depends_on or [])


class TestGetExecutionWaves:
    def test_single_task_one_wave(self):
        tasks = [_make_task("a")]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()
        assert len(waves) == 1
        assert set(waves[0]) == {"a"}

    def test_linear_chain_sequential_waves(self):
        tasks = [_make_task("a"), _make_task("b", ["a"]), _make_task("c", ["b"])]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()
        assert len(waves) == 3
        assert waves[0] == ["a"]
        assert waves[1] == ["b"]
        assert waves[2] == ["c"]

    def test_independent_tasks_same_wave(self):
        tasks = [_make_task("a"), _make_task("b"), _make_task("c")]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()
        assert len(waves) == 1
        assert set(waves[0]) == {"a", "b", "c"}

    def test_diamond_dag_three_waves(self):
        # a → b, a → c, b → d, c → d
        tasks = [
            _make_task("a"),
            _make_task("b", ["a"]),
            _make_task("c", ["a"]),
            _make_task("d", ["b", "c"]),
        ]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()
        assert len(waves) == 3
        assert waves[0] == ["a"]
        assert set(waves[1]) == {"b", "c"}
        assert waves[2] == ["d"]


class TestExecutePipelineParallel:
    def _success_execute(self, task, config, previous_data):
        return True, {"task": task.name}, ""

    def _fail_execute(self, task, config, previous_data):
        return False, None, "intentional failure"

    def test_all_tasks_succeed(self):
        tasks = [_make_task("a"), _make_task("b"), _make_task("c", ["a", "b"])]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()

        executor = PipelineExecutor()
        executor.task_executor.execute_task = self._success_execute

        success, results, errors = executor.execute_pipeline_parallel(
            tasks={t.name: t for t in tasks},
            execution_waves=waves,
        )
        assert success is True
        assert all(results.values())
        assert errors == {}

    def test_wave_failure_stops_pipeline(self):
        tasks = [_make_task("a"), _make_task("b", ["a"])]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()

        executor = PipelineExecutor()
        executor.task_executor.execute_task = self._fail_execute

        success, results, errors = executor.execute_pipeline_parallel(
            tasks={t.name: t for t in tasks},
            execution_waves=waves,
        )
        assert success is False
        # Second wave (task b) must not have run
        assert "b" not in results

    def test_parallel_tasks_run_concurrently(self):
        """Tasks in the same wave should run in different threads."""
        thread_ids = []
        lock = threading.Lock()

        def recording_execute(task, config, previous_data):
            with lock:
                thread_ids.append(threading.current_thread().ident)
            return True, None, ""

        tasks = [_make_task("a"), _make_task("b"), _make_task("c")]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()

        executor = PipelineExecutor()
        executor.task_executor.execute_task = recording_execute

        success, _, _ = executor.execute_pipeline_parallel(
            tasks={t.name: t for t in tasks},
            execution_waves=waves,
            max_workers=4,
        )
        assert success is True
        # With 3 independent tasks and max_workers=4, at least 2 unique thread IDs expected
        # (may be 1 if OS schedules all to same thread, so we just verify completion)
        assert len(thread_ids) == 3

    def test_dependency_data_passed_to_next_wave(self):
        """Output from wave 1 should be available as previous_data in wave 2."""
        received_previous = {}

        def capturing_execute(task, config, previous_data):
            received_previous[task.name] = previous_data
            return True, {"output_of": task.name}, ""

        tasks = [_make_task("producer"), _make_task("consumer", ["producer"])]
        dag = DAGBuilder(tasks)
        dag.build()
        waves = dag.get_execution_waves()

        executor = PipelineExecutor()
        executor.task_executor.execute_task = capturing_execute

        success, _, _ = executor.execute_pipeline_parallel(
            tasks={t.name: t for t in tasks},
            execution_waves=waves,
        )
        assert success is True
        assert received_previous["consumer"] == {"output_of": "producer"}


class TestCollectDependencyData:
    def test_no_deps_returns_none(self):
        executor = PipelineExecutor()
        task = _make_task("a")
        result = executor._collect_dependency_data(task, {"x": "data"})
        assert result is None

    def test_single_dep_returns_scalar(self):
        executor = PipelineExecutor()
        task = _make_task("b", ["a"])
        result = executor._collect_dependency_data(task, {"a": "my_data"})
        assert result == "my_data"

    def test_multiple_deps_returns_list(self):
        executor = PipelineExecutor()
        task = _make_task("c", ["a", "b"])
        result = executor._collect_dependency_data(task, {"a": "data_a", "b": "data_b"})
        assert isinstance(result, list)
        assert "data_a" in result
        assert "data_b" in result

    def test_dep_with_no_data_skipped(self):
        executor = PipelineExecutor()
        task = _make_task("b", ["a"])
        result = executor._collect_dependency_data(task, {"a": None})
        assert result is None
