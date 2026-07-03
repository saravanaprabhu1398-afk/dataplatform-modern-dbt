import logging
import os
import re
import tempfile
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from pydantic import BaseModel, Field

from dataplatform.core.config import PipelineConfig, load_config

logger = logging.getLogger(__name__)


class PipelineGenerationRequestModel(BaseModel):
    input_text: str = Field(..., min_length=1)


PLUGIN_ALIASES = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "postgre sql": "postgres",
    "postgre-sql": "postgres",
    "mysql": "mysql",
    "my sql": "mysql",
    "duckdb": "duckdb",
    "duck db": "duckdb",
    "snowflake": "snowflake",
    "snow flake": "snowflake",
    "api": "api",
    "rest": "api",
    "rest api": "api",
    "http api": "api",
    "http": "api",
    "webhook": "api",
    "shell": "shell",
    "bash": "shell",
    "sh": "shell",
    "python": "python",
    "py": "python",
    "dbt": "dbt",
    "spark": "spark",
    "kafka": "kafka",
    "email": "email",
    "bigquery": "bigquery",
    "big query": "bigquery",
    "csv": "file",
    "json": "file",
    "parquet": "file",
    "txt": "file",
    "file": "file",
    "files": "file",
    "s3": "file",
}

TASK_TYPE_ALIASES = {
    "executor": "executor",
    "execute": "executor",
    "ingest": "executor",
    "loader": "executor",
    "load": "executor",
    "extract": "executor",
    "export": "executor",
    "transformer": "transformer",
    "transform": "transformer",
    "model": "transformer",
    "modeling": "transformer",
}

OPERATION_KEYWORDS = [
    "extract",
    "ingest",
    "load",
    "transform",
    "validate",
    "export",
    "sync",
    "fetch",
    "run",
    "execute",
    "stage",
    "publish",
    "report",
]

SEGMENT_CONNECTORS = [
    "then",
    "next",
    "after that",
    "afterwards",
    "followed by",
    "finally",
    "lastly",
    "subsequently",
]

TASK_HINT_WORDS = set(OPERATION_KEYWORDS) | {
    "step",
    "task",
    "dbt",
    "python",
    "postgres",
    "postgresql",
    "mysql",
    "duckdb",
    "snowflake",
    "api",
    "http",
    "shell",
    "spark",
    "kafka",
    "email",
    "file",
    "csv",
    "json",
    "parquet",
}

PLUGIN_CONFIG_TEMPLATES: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {
    ("duckdb", "load"): {"file_path": "data/input.csv", "show_columns": True},
    ("duckdb", "query"): {"file_path": "data/input.csv", "sql": "SELECT * FROM data"},
    ("duckdb", "validate"): {
        "file_path": "data/input.csv",
        "checks": [{"name": "no_nulls", "sql": "SELECT COUNT(*) FROM data WHERE id IS NULL", "expect": 0}],
    },
    ("duckdb", "aggregate"): {
        "file_path": "data/input.csv",
        "group_by": ["category"],
        "metrics": [{"column": "amount", "function": "sum", "alias": "total_amount"}],
    },
    ("duckdb", "transform"): {
        "file_path": "data/input.csv",
        "columns": [{"name": "new_col", "sql": "col * 1.0"}],
    },
    ("postgres", "query"): {
        "connection": {"host": "localhost", "port": 5432, "database": "mydb", "user": "user", "password": "${POSTGRES_PASSWORD}"},
        "sql": "SELECT * FROM my_table",
    },
    ("postgres", "load"): {
        "connection": {"host": "localhost", "port": 5432, "database": "mydb", "user": "user", "password": "${POSTGRES_PASSWORD}"},
        "table_name": "target_table",
        "file_path": "data/input.csv",
    },
    ("postgres", "execute"): {
        "connection": {"host": "localhost", "port": 5432, "database": "mydb", "user": "user", "password": "${POSTGRES_PASSWORD}"},
        "sql": "INSERT INTO my_table VALUES (...)",
    },
    ("mysql", "query"): {
        "connection": {"host": "localhost", "port": 3306, "database": "mydb", "user": "user", "password": "${MYSQL_PASSWORD}"},
        "sql": "SELECT * FROM my_table",
    },
    ("mysql", "load"): {
        "connection": {"host": "localhost", "port": 3306, "database": "mydb", "user": "user", "password": "${MYSQL_PASSWORD}"},
        "table_name": "target_table",
        "file_path": "data/input.csv",
    },
    ("mysql", "execute"): {
        "connection": {"host": "localhost", "port": 3306, "database": "mydb", "user": "user", "password": "${MYSQL_PASSWORD}"},
        "sql": "UPDATE my_table SET col = val",
    },
    ("snowflake", "load_to_snowflake"): {
        "snowflake_config": {
            "account": "your_account.snowflakecomputing.com",
            "user": "${SNOWFLAKE_USER}",
            "password": "${SNOWFLAKE_PASSWORD}",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
        },
        "table_name": "target_table",
    },
    ("snowflake", "load"): {
        "snowflake_config": {
            "account": "your_account.snowflakecomputing.com",
            "user": "${SNOWFLAKE_USER}",
            "password": "${SNOWFLAKE_PASSWORD}",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
        },
        "table_name": "target_table",
    },
    ("bigquery", "query"): {
        "project_id": "my-gcp-project",
        "dataset_id": "my_dataset",
        "sql": "SELECT * FROM `my-gcp-project.my_dataset.my_table`",
    },
    ("bigquery", "load"): {
        "project_id": "my-gcp-project",
        "dataset_id": "my_dataset",
        "table_id": "my_table",
        "source_file": "data/input.csv",
    },
    ("api", "GET"): {
        "method": "GET",
        "url": "https://api.example.com/endpoint",
        "headers": {"Authorization": "Bearer ${API_TOKEN}"},
        "params": {},
    },
    ("api", "POST"): {
        "method": "POST",
        "url": "https://api.example.com/endpoint",
        "headers": {"Authorization": "Bearer ${API_TOKEN}", "Content-Type": "application/json"},
        "json": {"key": "value"},
    },
    ("api", "fetch"): {
        "method": "GET",
        "url": "https://api.example.com/endpoint",
        "headers": {"Authorization": "Bearer ${API_TOKEN}"},
        "params": {},
    },
    ("kafka", "publish"): {
        "brokers": ["localhost:9092"],
        "topic": "my_topic",
        "message": {"event": "data_ready"},
    },
    ("kafka", "subscribe"): {
        "brokers": ["localhost:9092"],
        "topic": "my_topic",
        "group_id": "my_consumer_group",
        "max_messages": 100,
    },
    ("spark", "submit"): {
        "spark_master": "spark://localhost:7077",
        "app_name": "MySparkApp",
        "input_path": "data/input.parquet",
        "output_path": "data/output.parquet",
    },
    ("python", "execute_code"): {"code": "result = 'hello from python'\nprint(result)"},
    ("python", "run_script"): {
        "script_path": "scripts/my_script.py",
        "parameters": {"input": "data/input.csv"},
    },
    ("shell", "execute"): {"command": "bash scripts/my_script.sh", "cwd": ".", "timeout": 300},
    ("file", "read"): {"file_path": "data/input.csv"},
    ("file", "write"): {"file_path": "data/output.csv", "content": ""},
    ("file", "merge"): {
        "source_files": ["data/part1.csv", "data/part2.csv"],
        "destination": "data/merged.csv",
    },
    ("email", "send"): {
        "smtp_server": "smtp.gmail.com",
        "smtp_port": 587,
        "sender_email": "${EMAIL_SENDER}",
        "sender_password": "${EMAIL_PASSWORD}",
        "recipients": ["recipient@example.com"],
        "subject": "Pipeline Notification",
        "body": "Pipeline execution completed.",
    },
    ("dbt", "run"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
    ("dbt", "test"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
    ("dbt", "compile"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
    ("dbt", "seed"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
    ("dbt", "snapshot"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
    ("dbt", "docs"): {"project_dir": "dbt_project", "profiles_dir": "~/.dbt"},
}


# Maps operation synonyms to canonical operations used as template keys
_OPERATION_ALIASES: Dict[str, str] = {
    "extract": "query",
    "fetch": "query",
    "ingest": "load",
    "export": "load",
    "stage": "load",
    "sync": "load",
    "transform": "transform",
    "report": "query",
}


def _get_config_template(plugin: str, operation: Optional[str]) -> Dict[str, Any]:
    """Return a config template dict for the given (plugin, operation) pair.

    Falls back to an aliased operation when the exact pair is missing, then
    falls back to the first available template for the plugin.
    """
    key = (plugin, operation)
    if key in PLUGIN_CONFIG_TEMPLATES:
        return dict(PLUGIN_CONFIG_TEMPLATES[key])

    # Try resolving an operation alias
    if operation and operation in _OPERATION_ALIASES:
        aliased_op = _OPERATION_ALIASES[operation]
        alias_key = (plugin, aliased_op)
        if alias_key in PLUGIN_CONFIG_TEMPLATES:
            return dict(PLUGIN_CONFIG_TEMPLATES[alias_key])

    # Fall back to any template available for this plugin
    for (p, _op), template in PLUGIN_CONFIG_TEMPLATES.items():
        if p == plugin:
            return dict(template)

    return {}


def _normalize_text(input_text: str) -> str:
    text = unicodedata.normalize("NFKC", input_text or "")
    replacements = {
        "\r\n": "\n",
        "\r": "\n",
        "\u2022": "- ",
        "\u25e6": "- ",
        "\u2043": "- ",
        "\u00b7": "- ",
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\t": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"[ ]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _slugify(value: str, default: str = "generated_pipeline") -> str:
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or default


def _split_lines(text: str) -> List[str]:
    return [line.strip(" -•*0123456789.)(").strip() for line in text.splitlines() if line.strip()]


def _split_instruction_segments(text: str) -> List[str]:
    working_text = text
    for connector in SEGMENT_CONNECTORS:
        pattern = rf"(?:,?\s+|\s+){re.escape(connector)}\s+"
        working_text = re.sub(pattern, ". ", working_text, flags=re.IGNORECASE)

    working_text = re.sub(r"\bfirst\b[:,]?\s*", "", working_text, flags=re.IGNORECASE)
    raw_segments = re.split(r"[\n.;]+", working_text)

    segments: List[str] = []
    for segment in raw_segments:
        cleaned = segment.strip(" ,-")
        if cleaned:
            segments.append(cleaned)
    return segments


def _detect_plugin(text: str) -> Optional[str]:
    lowered = f" {text.lower()} "
    for alias, canonical in sorted(PLUGIN_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if f" {alias} " in lowered or lowered.strip() == alias or alias in lowered:
            return canonical
    return None


def _detect_task_type(text: str, plugin: Optional[str]) -> str:
    lowered = text.lower()
    for alias, canonical in TASK_TYPE_ALIASES.items():
        if alias in lowered:
            return canonical
    if plugin == "dbt":
        return "transformer"
    return "executor"


def _detect_operation(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword in OPERATION_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def _extract_schedule(text: str) -> Optional[Dict[str, str]]:
    lowered = text.lower()

    every_minutes = re.search(r"every\s+(\d+)\s+minutes?", lowered)
    if every_minutes:
        return {"minute": f"*/{every_minutes.group(1)}"}

    every_hours = re.search(r"every\s+(\d+)\s+hours?", lowered)
    if every_hours:
        return {"minute": "0", "hour": f"*/{every_hours.group(1)}"}

    daily = re.search(r"(daily|every day)(?:\s+at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?)?", lowered)
    if daily:
        hour = 0
        minute = "0"
        if daily.group(2):
            hour = int(daily.group(2))
            if daily.group(3):
                minute = daily.group(3)
            meridiem = daily.group(4)
            if meridiem == "pm" and hour < 12:
                hour += 12
            if meridiem == "am" and hour == 12:
                hour = 0
        return {"minute": str(minute), "hour": str(hour)}

    hourly = re.search(r"(hourly|every hour)", lowered)
    if hourly:
        return {"minute": "0"}

    weekly = re.search(r"(weekly|every week)(?:\s+on\s+([a-z]+))?", lowered)
    if weekly:
        schedule = {"minute": "0", "hour": "0"}
        if weekly.group(2):
            schedule["day_of_week"] = weekly.group(2)[:3]
        return schedule

    monthly = re.search(r"(monthly|every month)", lowered)
    if monthly:
        return {"minute": "0", "hour": "0", "day": "1"}

    cron_like = re.search(r'minute\s*[:=]\s*[\'"]?([^,\'"\s]+)', lowered)
    if cron_like:
        schedule: Dict[str, str] = {"minute": cron_like.group(1)}
        for key in ["hour", "day", "month", "day_of_week"]:
            match = re.search(rf'{key}\s*[:=]\s*[\'"]?([^,\'"\s]+)', lowered)
            if match:
                schedule[key] = match.group(1)
        return schedule

    return None


def _extract_pipeline_name(text: str) -> Optional[str]:
    patterns = [
        r"pipeline name\s*[:\-]\s*([^\n]+)",
        r"pipeline\s*[:\-]\s*([^\n]+)",
        r"name\s*[:\-]\s*([^\n]+)",
        r"called\s+([a-zA-Z0-9 _-]+)",
        r"(?:build|create|run|generate)\s+(?:an?\s+)?(.+?)\s+pipeline\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip(" .:")
            if candidate:
                return _slugify(candidate)
    return None


def _extract_description(lines: List[str], pipeline_name: str) -> str:
    for line in lines:
        lowered = line.lower()
        if line and pipeline_name.replace("_", " ") not in lowered and "schedule" not in lowered:
            if len(line.split()) >= 4:
                return line[:200]
    return "Generated from free-form text"


def _extract_file_path(text: str) -> Optional[str]:
    match = re.search(r"([a-zA-Z0-9_./-]+\.(csv|json|parquet|sql|txt))", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _extract_dependencies(line: str, known_task_names: List[str]) -> List[str]:
    dependencies: List[str] = []
    lowered = line.lower()

    explicit = re.findall(r"(?:depends on|after|following)\s+([a-zA-Z0-9_,\sand-]+)", lowered)
    for group in explicit:
        for part in re.split(r",| and ", group):
            dependency = _slugify(part.strip(), default="")
            if dependency and dependency in known_task_names and dependency not in dependencies:
                dependencies.append(dependency)

    return dependencies


def _is_task_segment(segment: str) -> bool:
    lowered = segment.lower()
    return any(word in lowered for word in TASK_HINT_WORDS)


def _derive_task_name(line: str, index: int, operation: Optional[str], plugin: Optional[str]) -> str:
    explicit_task_name = re.search(r"(?:task|step)\s*\d*\s*[:\-]\s*([^\n]+)", line, flags=re.IGNORECASE)
    if explicit_task_name:
        return _slugify(explicit_task_name.group(1), default=f"task_{index}")

    cleaned_line = re.sub(
        r"\b(with|using|via|through|into|to|from|on|in|for|the|a|an|and|data|results?)\b",
        " ",
        line,
        flags=re.IGNORECASE,
    )
    cleaned_line = re.sub(r"\s+", " ", cleaned_line).strip()
    slug_source = cleaned_line or line
    slug = _slugify(slug_source, default=f"task_{index}")
    parts = slug.split("_")

    if operation and operation not in parts:
        parts.insert(0, operation)
    if plugin and plugin not in parts and len(parts) < 4:
        parts.append(plugin)

    return "_".join(parts[:5]) or f"task_{index}"


def _build_task_from_line(line: str, index: int) -> Optional[Dict[str, Any]]:
    plugin = _detect_plugin(line)
    operation = _detect_operation(line)
    task_type = _detect_task_type(line, plugin)

    if not plugin and not operation and len(line.split()) < 2:
        return None

    task_name = _derive_task_name(line, index, operation, plugin)

    return {
        "name": task_name,
        "type": task_type,
        "plugin": plugin or "python",
        "operation": operation,
        "config": _get_config_template(plugin or "python", operation),
        "retries": 0,
    }


def _extract_tasks(text: str, warnings: List[str]) -> List[Dict[str, Any]]:
    candidate_lines = [segment for segment in _split_instruction_segments(text) if _is_task_segment(segment)]

    tasks: List[Dict[str, Any]] = []
    seen_names = set()

    for index, line in enumerate(candidate_lines, start=1):
        task = _build_task_from_line(line, index)
        if not task:
            continue

        base_name = task["name"]
        suffix = 1
        while task["name"] in seen_names:
            suffix += 1
            task["name"] = f"{base_name}_{suffix}"

        seen_names.add(task["name"])
        tasks.append(task)

    if not tasks:
        warnings.append("No explicit tasks detected; created a single default python task.")
        tasks.append({
            "name": "default_task",
            "type": "executor",
            "plugin": "python",
            "operation": None,
            "config": {},
            "retries": 0,
        })

    known_task_names = [task["name"] for task in tasks]
    for index, task in enumerate(tasks):
        source_line = candidate_lines[index] if index < len(candidate_lines) else ""
        dependencies = _extract_dependencies(source_line, known_task_names)

        if not dependencies and index > 0:
            dependencies = [tasks[index - 1]["name"]]

        if dependencies:
            task["depends_on"] = dependencies

    return tasks


def _cleanup_task_fields(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_tasks: List[Dict[str, Any]] = []
    for task in tasks:
        cleaned_task = {
            "name": _slugify(task.get("name", ""), default="task"),
            "type": task.get("type") or _detect_task_type("", task.get("plugin")),
            "plugin": task.get("plugin") or "python",
            "retries": int(task.get("retries", 0) or 0),
        }
        if task.get("operation"):
            cleaned_task["operation"] = task["operation"]
        if task.get("config") is not None:
            cleaned_task["config"] = task["config"]
        if task.get("depends_on"):
            cleaned_task["depends_on"] = [_slugify(dep, default="") for dep in task["depends_on"] if dep]
        cleaned_tasks.append(cleaned_task)
    return cleaned_tasks


_LLM_SYSTEM_PROMPT = """\
You are a data pipeline YAML generator. Given a natural-language description of a data \
pipeline, output ONLY a valid YAML pipeline configuration inside a ```yaml ... ``` fence. \
No explanation, no prose outside the fence.

## Schema

pipeline_name: (required) string in slug format, e.g. my_pipeline
description:   (optional) string
team:          (optional) string
tasks:         (required) list of at least one task; each task has:
  name:       (required) unique slug, e.g. extract_orders
  type:       (required) "executor" or "transformer"
  plugin:     (required) one of: postgres, mysql, duckdb, snowflake, bigquery, dbt,
              spark, kafka, python, shell, api, email, file
  operation:  (optional) e.g. query, load, transform, run, execute_code
  config:     (optional) dict of plugin-specific settings
  retries:    (optional) integer, default 0
  depends_on: (optional) list of upstream task names (must reference existing task names)
schedule:     (optional) dict; allowed keys: minute, hour, day, month, day_of_week (all strings)
sla:          (optional) dict with max_duration_minutes (float)
tags:         (optional) list of strings

## Example

Input: "Daily ETL: extract orders from postgres, transform with dbt, load to snowflake"

Output:
```yaml
pipeline_name: daily_etl
description: Daily ETL pipeline
tasks:
  - name: extract_orders
    type: executor
    plugin: postgres
    operation: query
    config:
      sql: SELECT * FROM orders
    retries: 0
  - name: transform_dbt
    type: transformer
    plugin: dbt
    operation: run
    config:
      project_dir: dbt_project
      profiles_dir: ~/.dbt
    retries: 0
    depends_on:
      - extract_orders
  - name: load_snowflake
    type: executor
    plugin: snowflake
    operation: load
    config:
      table_name: orders
    retries: 0
    depends_on:
      - transform_dbt
schedule:
  minute: "0"
  hour: "0"
tags:
  - etl
  - sales
```
"""


def _extract_yaml_from_response(text: str) -> Optional[str]:
    """Extract the YAML block from an LLM response string.

    Looks for a ```yaml...``` fenced block first; falls back to treating the
    entire stripped response as YAML if it starts with 'pipeline_name:'.
    """
    fenced = re.search(r"```(?:yaml)?\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    stripped = text.strip()
    if re.match(r"pipeline_name\s*:", stripped, re.IGNORECASE):
        return stripped
    return None


def _try_llm_generate(input_text: str) -> Optional[Dict[str, Any]]:
    """Attempt to generate a pipeline config using an LLM.

    Tries Anthropic first (ANTHROPIC_API_KEY), then OpenAI (OPENAI_API_KEY).
    Returns a fully-formed result dict on success, or None to signal fallback.
    """
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    openai_key = os.environ.get("OPENAI_API_KEY")

    if not anthropic_key and not openai_key:
        logger.debug("No LLM API key configured (ANTHROPIC_API_KEY / OPENAI_API_KEY); skipping LLM path.")
        return None

    raw_response: Optional[str] = None

    # ── Anthropic path ────────────────────────────────────────────────────────
    if anthropic_key and raw_response is None:
        try:
            import anthropic  # type: ignore[import]
            client = anthropic.Anthropic(api_key=anthropic_key, timeout=12.0)
            message = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=2048,
                system=_LLM_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": input_text}],
            )
            raw_response = message.content[0].text
            logger.debug("LLM response received from Anthropic (%d chars).", len(raw_response))
        except ImportError:
            logger.debug("anthropic package not installed; skipping Anthropic LLM path.")
        except Exception as exc:
            logger.warning("Anthropic LLM call failed (%s); will try fallback.", exc)

    # ── OpenAI path ───────────────────────────────────────────────────────────
    if openai_key and raw_response is None:
        try:
            import openai  # type: ignore[import]
            client = openai.OpenAI(api_key=openai_key, timeout=12.0)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=2048,
                messages=[
                    {"role": "system", "content": _LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": input_text},
                ],
            )
            raw_response = response.choices[0].message.content
            logger.debug("LLM response received from OpenAI (%d chars).", len(raw_response or ""))
        except ImportError:
            logger.debug("openai package not installed; skipping OpenAI LLM path.")
        except Exception as exc:
            logger.warning("OpenAI LLM call failed (%s); will try fallback.", exc)

    if raw_response is None:
        return None

    # ── Extract YAML from the response ────────────────────────────────────────
    yaml_text = _extract_yaml_from_response(raw_response)
    if not yaml_text:
        logger.warning("LLM response contained no recognisable YAML block; falling back.")
        return None

    # ── Validate via load_config (write to a temp file, load, delete) ─────────
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".yaml")
    try:
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
                fh.write(yaml_text)
            tmp_fd = None  # fdopen owns it now
            parsed_config = load_config(tmp_path)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    except Exception as val_exc:
        logger.warning("LLM-generated YAML failed validation (%s); falling back to regex.", val_exc)
        if tmp_fd is not None:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
        return None

    return {
        "yaml_content": yaml_text,
        "parsed_config": parsed_config.model_dump(exclude_none=True),
        "warnings": [],
        "detected_language": "llm-generated",
        "nlp_summary": [],
        "generated_by": "llm",
    }


def generate_pipeline_yaml_from_text(input_text: str) -> Dict[str, Any]:
    """
    Primary entry point for pipeline generation from free text.

    Priority:
    1. LLM path — Anthropic (ANTHROPIC_API_KEY) or OpenAI (OPENAI_API_KEY)
    2. NLP engine (nlp_generator.py)
    3. Legacy regex fallback

    All paths return the same dict shape; ``generated_by`` is ``"llm"`` when the
    LLM was used and ``"regex"`` otherwise.
    """
    # ── LLM path (primary) ───────────────────────────────────────────────────
    try:
        llm_result = _try_llm_generate(input_text)
        if llm_result is not None:
            return llm_result
    except Exception as llm_exc:  # pragma: no cover — belt-and-suspenders
        logger.warning("LLM generate wrapper raised unexpectedly (%s); continuing to NLP.", llm_exc)

    # ── NLP path (secondary) ─────────────────────────────────────────────────
    try:
        from dataplatform.core.nlp_generator import generate_from_text as _nlp_generate
        result = _nlp_generate(input_text)
        result["generated_by"] = "regex"
        return result
    except Exception as nlp_exc:  # pragma: no cover
        logger.warning("NLP generator failed (%s); falling back to legacy regex parser.", nlp_exc)

    # ── Legacy regex fallback ─────────────────────────────────────────────────
    normalized_text = _normalize_text(input_text)
    warnings: List[str] = ["NLP parser unavailable; used legacy regex engine."]

    if not normalized_text:
        normalized_text = "generated pipeline"
        warnings.append("Input text was empty after normalization; used default content.")

    pipeline_name = _extract_pipeline_name(normalized_text) or "generated_pipeline"
    if pipeline_name == "generated_pipeline":
        warnings.append("Could not infer pipeline name; used default pipeline_name='generated_pipeline'.")

    lines = _split_lines(normalized_text)
    description = _extract_description(lines, pipeline_name)
    file_path = _extract_file_path(normalized_text)
    if not file_path:
        warnings.append("No file path detected; file_path was omitted.")

    tasks = _cleanup_task_fields(_extract_tasks(normalized_text, warnings))
    schedule = _extract_schedule(normalized_text)
    if not schedule:
        warnings.append("No schedule detected; schedule was omitted.")

    config_payload: Dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "description": description,
        "tasks": tasks,
    }
    if file_path:
        config_payload["file_path"] = file_path
    if schedule:
        config_payload["schedule"] = schedule

    parsed_config = PipelineConfig(**config_payload)
    yaml_content = yaml.safe_dump(
        parsed_config.model_dump(exclude_none=True),
        sort_keys=False,
        default_flow_style=False,
    )

    return {
        "yaml_content": yaml_content,
        "parsed_config": parsed_config.model_dump(exclude_none=True),
        "warnings": warnings,
        "detected_language": "english-like-free-text",
        "nlp_summary": [],
        "generated_by": "regex",
    }


def save_generated_pipeline(yaml_content: str, filename: str) -> str:
    workspace_root = Path(__file__).resolve().parent.parent.parent
    pipelines_dir = workspace_root / "pipelines"
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    safe_name = _slugify(Path(filename).stem, default="generated_pipeline")
    final_path = pipelines_dir / f"{safe_name}.yaml"

    parsed_yaml = yaml.safe_load(yaml_content)
    if not isinstance(parsed_yaml, dict):
        raise ValueError("Generated YAML must contain a valid pipeline configuration object.")

    validated_config = PipelineConfig(**parsed_yaml)

    with open(final_path, "w", encoding="utf-8") as file_handle:
        yaml.safe_dump(
            validated_config.model_dump(exclude_none=True),
            file_handle,
            sort_keys=False,
            default_flow_style=False,
        )

    load_config(str(final_path))
    return str(final_path)
