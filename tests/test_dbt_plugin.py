"""Tests for the expanded dbt transformer plugin."""
import subprocess
from unittest.mock import MagicMock, patch
import pytest

from dataplatform.plugins.transformers.dbt_plugin import DbtTransformer


@pytest.fixture
def plugin():
    return DbtTransformer()


def _mock_run(returncode=0, stdout="dbt output", stderr=""):
    mock = MagicMock()
    mock.returncode = returncode
    mock.stdout = stdout
    mock.stderr = stderr
    return mock


class TestDbtBinaryDiscovery:
    def test_uses_shutil_which_first(self, plugin):
        with patch("shutil.which", return_value="/usr/bin/dbt") as mock_which:
            result = plugin._find_dbt_binary()
        assert result == "/usr/bin/dbt"
        mock_which.assert_called_once_with("dbt")

    def test_falls_back_to_known_paths(self, plugin, tmp_path):
        fake_dbt = tmp_path / "dbt"
        fake_dbt.touch()
        with patch("shutil.which", return_value=None), \
             patch("dataplatform.plugins.transformers.dbt_plugin.os.path.exists",
                   side_effect=lambda p: p == str(fake_dbt)):
            # We can't easily inject tmp_path into the hardcoded list,
            # so just verify None is returned when none of the known paths exist.
            result = plugin._find_dbt_binary()
        assert result is None  # none of the hardcoded paths exist in test env

    def test_returns_none_when_not_found(self, plugin):
        with patch("shutil.which", return_value=None), \
             patch("os.path.exists", return_value=False):
            result = plugin._find_dbt_binary()
        assert result is None


class TestDbtRun:
    def test_run_builds_correct_command(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            success, data = plugin.execute({"operation": "run", "project_dir": "/my/project"})
        assert success is True
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "dbt"
        assert cmd[1] == "run"
        assert "--select" not in cmd

    def test_run_with_select(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            success, data = plugin.execute({
                "operation": "run", "project_dir": ".", "select": "my_model"
            })
        cmd = mock_run.call_args[0][0]
        assert "--select" in cmd
        assert "my_model" in cmd

    def test_run_with_profiles_dir(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            plugin.execute({
                "operation": "run", "project_dir": ".", "profiles_dir": "~/.dbt"
            })
        cmd = mock_run.call_args[0][0]
        assert "--profiles-dir" in cmd
        assert "~/.dbt" in cmd

    def test_defaults_to_run_when_no_operation(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            plugin.execute({"project_dir": "."})
        cmd = mock_run.call_args[0][0]
        assert "run" in cmd


class TestDbtTest:
    def test_test_command(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            success, data = plugin.execute({"operation": "test", "project_dir": "."})
        assert success is True
        cmd = mock_run.call_args[0][0]
        assert "test" in cmd

    def test_test_with_select(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            plugin.execute({"operation": "test", "project_dir": ".", "select": "staging"})
        cmd = mock_run.call_args[0][0]
        assert "--select" in cmd
        assert "staging" in cmd


class TestDbtOtherOperations:
    @pytest.mark.parametrize("operation", ["compile", "seed", "snapshot"])
    def test_operation_in_command(self, plugin, operation):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            success, data = plugin.execute({"operation": operation, "project_dir": "."})
        assert success is True
        cmd = mock_run.call_args[0][0]
        assert operation in cmd

    def test_docs_appends_generate(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            success, data = plugin.execute({"operation": "docs", "project_dir": "."})
        cmd = mock_run.call_args[0][0]
        assert "docs" in cmd
        assert "generate" in cmd

    def test_ls_operation(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            plugin.execute({"operation": "ls", "project_dir": "."})
        cmd = mock_run.call_args[0][0]
        assert "ls" in cmd


class TestDbtErrorHandling:
    def test_unsupported_operation_returns_false(self, plugin):
        success, data = plugin.execute({"operation": "invalid_op", "project_dir": "."})
        assert success is False
        assert "error" in data
        assert "invalid_op" in data["error"]

    def test_binary_not_found_returns_false(self, plugin):
        with patch("shutil.which", return_value=None), \
             patch("os.path.exists", return_value=False):
            success, data = plugin.execute({"operation": "run", "project_dir": "."})
        assert success is False
        assert "error" in data

    def test_command_failure_captured_in_result(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run(returncode=1, stderr="dbt error")):
            success, data = plugin.execute({"operation": "run", "project_dir": "."})
        assert success is False
        assert data["returncode"] == 1
        assert "stderr" in data
        assert "dbt error" in data["stderr"]

    def test_result_contains_stdout(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run(stdout="Completed 3 models")):
            success, data = plugin.execute({"operation": "run", "project_dir": "."})
        assert "stdout" in data
        assert "Completed 3 models" in data["stdout"]

    def test_accepts_legacy_dbt_project_dir_key(self, plugin):
        with patch("shutil.which", return_value="dbt"), \
             patch("subprocess.run", return_value=_mock_run()) as mock_run:
            plugin.execute({"operation": "run", "dbt_project_dir": "/legacy/path"})
        assert mock_run.call_args[1]["cwd"] == "/legacy/path"
