from dataplatform.plugins.executors.shell_plugin import ShellExecutor


def test_shell_executor_uses_safe_split_by_default():
    executor = ShellExecutor()
    success, result = executor.execute({"command": "echo hello"})

    assert success is True
    assert result["stdout"] == "hello"
    assert result["used_shell"] is False
