"""The worker CLI has to expose the lease controls, or they cannot be tuned.

``run_worker_loop`` grew ``lease_seconds`` and ``max_attempts`` with the
lease/fencing work, and a deployment that cannot set them is stuck with
defaults that may not suit its task runtimes.
"""
from typer.testing import CliRunner

import dataplatform.core.queue_worker as queue_worker
from dataplatform.cli.main import app

runner = CliRunner()


def _capture(monkeypatch):
    captured = {}

    def fake_loop(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(queue_worker, "run_worker_loop", fake_loop)
    return captured


def test_lease_options_reach_the_worker_loop(monkeypatch):
    captured = _capture(monkeypatch)

    result = runner.invoke(
        app, ["worker", "--once", "--lease-seconds", "300", "--max-attempts", "5"]
    )

    assert result.exit_code == 0, result.output
    assert captured["lease_seconds"] == 300
    assert captured["max_attempts"] == 5


def test_defaults_are_left_to_the_worker_loop(monkeypatch):
    # Not passing them must mean "use the library default", not "override with
    # a copy of it that can drift".
    captured = _capture(monkeypatch)

    result = runner.invoke(app, ["worker", "--once"])

    assert result.exit_code == 0, result.output
    assert "lease_seconds" not in captured
    assert "max_attempts" not in captured
    assert captured["poll_interval"] == 2.0


def test_poll_interval_still_passes_through(monkeypatch):
    captured = _capture(monkeypatch)

    result = runner.invoke(app, ["worker", "--once", "--poll-interval", "7.5"])

    assert result.exit_code == 0, result.output
    assert captured["poll_interval"] == 7.5
