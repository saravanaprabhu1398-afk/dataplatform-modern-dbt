"""Browser tests for the dashboard.

Nothing in the Python suite touches the UI, so three real bugs shipped and were
found by hand: a lineage graph that never rendered, a health pill hardcoded to
HEALTHY, and stat cards collapsed into a column by a Tailwind class with no
base rule. Each of those is one assertion here.

The tests are written to be boring on purpose. A flaky check teaches people to
ignore checks, so there are no bare sleeps: the server is polled until healthy,
and every assertion waits on a condition rather than a duration.

Skipped when Playwright is not installed, so the default suite stays fast and
dependency-free. CI installs it and asserts they actually ran.
"""
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

# Playwright's sync API runs its own event loop, which leaves a later test that
# calls asyncio.run() with "cannot be called from a running event loop". These
# therefore run in their own pytest process: deselected by default in
# pyproject.toml, and selected explicitly with -m browser.
pytestmark = pytest.mark.browser

pytest.importorskip("playwright.sync_api", reason="playwright is not installed")

from playwright.sync_api import Error as PlaywrightError  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

USERNAME = "browser-test-admin"
PASSWORD = "browser-test-password"
SESSION_SECRET = "browser-tests-session-secret-at-least-32-chars"

# Every page a signed-in user can reach from the sidebar.
PAGES = [
    "index.html", "job_builder.html", "monitoring.html", "lineage.html",
    "alerts.html", "catalog.html", "costs.html", "deployments.html",
    "templates.html", "generator.html", "git_integration.html", "admin.html",
]

# Console noise that is not the application's fault and would make the check
# flaky rather than useful.
IGNORABLE = (
    "favicon",
    "net::ERR_INTERNET_DISCONNECTED",
    "cdnjs.cloudflare.com",      # icon font, blocked in sandboxed CI
    "fonts.googleapis.com",
    "fonts.gstatic.com",
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _seed(database_path: str) -> None:
    """Give the pages something real to render."""
    os.environ["DATABASE_PATH"] = database_path
    import dataplatform.core.database as db

    db._initialized = False
    db._DB_PATH = Path(database_path)
    db._engine = None
    db.init_db()

    edges = [
        ("load_orders", "reads_from", "postgres://prod/public.orders"),
        ("load_orders", "writes_to", "duckdb://local/stg_orders"),
        ("daily_revenue", "reads_from", "duckdb://local/stg_orders"),
        ("daily_revenue", "writes_to", "duckdb://local/daily_revenue"),
        ("finance_export", "reads_from", "duckdb://local/daily_revenue"),
        ("finance_export", "writes_to", "s3://finance/export.parquet"),
    ]
    for task, direction, uri in edges:
        db.save_lineage_record("seed-run", "revenue_pipeline", task, direction, uri)


@pytest.fixture(scope="session")
def server(tmp_path_factory):
    """A real instance of the app, polled until it answers."""
    workdir = tmp_path_factory.mktemp("browser")
    database_path = str(workdir / "platform.db")
    _seed(database_path)

    port = _free_port()
    environment = dict(os.environ)
    # A realistic instance: the deployments page validates the selected
    # pipeline on load, and with no pipelines at all it asks the API to
    # validate nothing.
    pipelines = workdir / "pipelines"
    pipelines.mkdir(exist_ok=True)
    (pipelines / "seed_pipeline.yaml").write_text(
        "pipeline_name: seed_pipeline\n"
        "schedule:\n  hour: \"6\"\n  minute: \"0\"\n"
        "tasks:\n"
        "  - name: noop\n"
        "    id: noop\n"
        "    type: executor\n"
        "    plugin: python\n"
        "    config:\n"
        "      operation: execute_code\n"
        "      code: |\n"
        "        result = {\"ok\": True}\n"
    )

    environment.update(
        DATABASE_PATH=database_path,
        DATAPLATFORM_USERNAME=USERNAME,
        DATAPLATFORM_PASSWORD=PASSWORD,
        DATAPLATFORM_SESSION_SECRET=SESSION_SECRET,
        DATAPLATFORM_OBSERVABILITY_AUTO_COLLECT="false",
        PIPELINES_PATH=str(pipelines),
    )

    process = subprocess.Popen(
        [sys.executable, "-m", "dataplatform.cli.main", "serve",
         "--host", "127.0.0.1", "--port", str(port)],
        env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    base_url = "http://127.0.0.1:{0}".format(port)
    deadline = time.time() + 60
    while time.time() < deadline:
        if process.poll() is not None:
            pytest.fail("server exited early:\n{0}".format(process.stdout.read()))
        try:
            import urllib.request

            with urllib.request.urlopen(base_url + "/health", timeout=1) as response:
                if response.status == 200:
                    break
        except Exception:
            time.sleep(0.25)
    else:
        process.terminate()
        pytest.fail("server did not become healthy within 60s")

    yield base_url

    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()


@pytest.fixture(scope="session")
def browser():
    with sync_playwright() as playwright:
        try:
            instance = playwright.chromium.launch()
        except PlaywrightError as exc:
            pytest.skip("chromium is not installed: {0}".format(exc))
        yield instance
        instance.close()


@pytest.fixture()
def page(browser, server):
    """A signed-in page with a fixed viewport and console errors collected."""
    context = browser.new_context(viewport={"width": 1280, "height": 860})
    page = context.new_page()

    errors = []
    page.on("console", lambda message: (
        errors.append(message.text) if message.type == "error" else None
    ))
    page.on("pageerror", lambda exc: errors.append(str(exc)))
    page.console_errors = errors  # type: ignore[attr-defined]

    page.goto(server + "/static/login.html", wait_until="domcontentloaded")
    response = page.evaluate(
        """async ({u, p}) => {
            const r = await fetch('/login', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({username: u, password: p}),
            });
            return r.status;
        }""",
        {"u": USERNAME, "p": PASSWORD},
    )
    assert response == 200, "login failed with status {0}".format(response)
    errors.clear()

    yield page
    context.close()


def _real_errors(page) -> list:
    return [
        text for text in page.console_errors
        if not any(ignorable in text for ignorable in IGNORABLE)
    ]


@pytest.mark.parametrize("filename", PAGES)
def test_page_loads_without_console_errors(page, server, filename):
    """A page that throws on load is broken however good it looks."""
    page.goto("{0}/static/{1}".format(server, filename), wait_until="networkidle")
    assert _real_errors(page) == [], "{0} logged errors: {1}".format(
        filename, _real_errors(page)
    )


def test_lineage_graph_renders_nodes(page, server):
    """The regression this suite exists for: the graph rendered nothing.

    /lineage returns edges keyed from/to and d3.forceLink wants source/target,
    so the simulation threw and the canvas stayed blank.
    """
    page.goto(server + "/static/lineage.html", wait_until="networkidle")
    page.wait_for_function(
        "() => document.querySelectorAll('#graph-svg circle, #graph-svg rect').length > 2",
        timeout=15000,
    )

    shapes = page.evaluate(
        "() => document.querySelectorAll('#graph-svg circle, #graph-svg rect').length"
    )
    view_box = page.get_attribute("#graph-svg", "viewBox")
    width = float((view_box or "0 0 0 0").split()[2])

    assert shapes > 2, "the lineage graph drew nothing"
    assert width > 0, "the graph was drawn into a zero-width canvas"
    assert _real_errors(page) == []


def test_health_pill_reflects_the_data(page, server):
    """It used to be static markup that read HEALTHY whatever the metrics said."""
    page.goto(server + "/static/monitoring.html", wait_until="networkidle")
    page.wait_for_function("() => typeof applyHealthPills === 'function'", timeout=15000)

    states = page.evaluate(
        """() => {
            const read = () => document.getElementById('pillSuccess').textContent.trim();
            const out = {};
            applyHealthPills(0, 0);     out.none = read();
            applyHealthPills(8, 7);     out.mostlyFailing = read();
            applyHealthPills(100, 10);  out.degraded = read();
            applyHealthPills(100, 1);   out.healthy = read();
            return out;
        }"""
    )

    assert states == {
        "none": "NO DATA",
        "mostlyFailing": "CRITICAL",
        "degraded": "WATCH",
        "healthy": "HEALTHY",
    }


def test_deployment_stat_cards_share_a_row(page, server):
    """They stacked because grid-cols-4 has no base rule in the compiled CSS."""
    page.goto(server + "/static/deployments.html", wait_until="networkidle")
    page.wait_for_selector("main section.grid .panel-card")

    tops = page.evaluate(
        """() => [...document.querySelectorAll('main section.grid > div')]
                  .map(el => Math.round(el.getBoundingClientRect().top))"""
    )

    assert len(tops) >= 4, "expected four stat cards, found {0}".format(len(tops))
    assert len(set(tops[:4])) == 1, "stat cards are stacked, not in a row: {0}".format(tops)


def test_dark_mode_actually_changes_the_dashboard(page, server):
    """The toggle used to flip tokens the dashboard did not consume."""
    page.goto(server + "/static/index.html", wait_until="networkidle")
    background = "() => getComputedStyle(document.body).backgroundColor"

    before = page.evaluate(background)
    # The app records the choice in localStorage and stamps dp-dark / dp-light
    # on the root element; there is no data-theme attribute.
    page.evaluate(
        """() => {
            const root = document.documentElement;
            root.classList.remove('dp-light');
            root.classList.add('dp-dark');
        }"""
    )
    page.wait_for_function(
        "(prev) => getComputedStyle(document.body).backgroundColor !== prev",
        arg=before, timeout=5000,
    )

    assert page.evaluate(background) != before
