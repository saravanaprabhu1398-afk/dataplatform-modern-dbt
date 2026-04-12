"""
NLP Pipeline Generator
======================
Converts free-text pipeline descriptions into structured task configs using
linguistic pattern matching — no external NLP libraries required.

Key improvements over the regex-only approach
---------------------------------------------
* Sentence-level intent extraction (verb + subject + source + target)
* Source / target system detection via preposition anchors (FROM / INTO / TO)
* Rich verb→operation mapping covering 50+ ETL verbs
* Smart, readable task naming  ("extract_orders_postgres" not slug soup)
* Table / schema / dbt-model name extraction
* Parallel-wave detection ("simultaneously", "in parallel")
* Config auto-population with detected table names / file paths / SQL
* Lineage inference from source/target
* Richer schedule parsing ("twice daily", "every business day", "at noon")
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Knowledge bases
# ─────────────────────────────────────────────────────────────────────────────

# (priority-ordered) plugin name patterns
PLUGIN_PATTERNS: List[Tuple[str, str]] = [
    ("postgres",   r"\bpostgre?s(?:ql)?\b"),
    ("mysql",      r"\bmysql\b"),
    ("duckdb",     r"\bduckdb\b"),
    ("snowflake",  r"\bsnowflake\b"),
    ("bigquery",   r"\bbig[\s\-]?query\b"),
    ("dbt",        r"\bdbt\b"),
    ("spark",      r"\bspark\b"),
    ("kafka",      r"\bkafka\b"),
    ("python",     r"\bpython\b"),
    ("shell",      r"\b(?:bash|shell|sh)\b"),
    ("api",        r"\b(?:rest\s*)?api\b|\bhttp[s]?\b|\bwebhook\b|\brest\b"),
    ("email",      r"\bemail\b|\bsmtp\b|\b(?:send\s+)?mail\b"),
    ("file",       r"\b(?:csv|json|parquet|xlsx?|txt|file)\b|\bs3://|\bs3\b"),
]

# Verb → (canonical_operation, task_type)
VERB_OPS: Dict[str, Tuple[str, str]] = {
    # ── Extract / read ──────────────────────────────────────────────────────
    "extract":     ("query",        "executor"),
    "pull":        ("query",        "executor"),
    "fetch":       ("query",        "executor"),
    "read":        ("query",        "executor"),
    "get":         ("query",        "executor"),
    "retrieve":    ("query",        "executor"),
    "query":       ("query",        "executor"),
    "select":      ("query",        "executor"),
    "ingest":      ("load",         "executor"),
    "import":      ("load",         "executor"),
    "scrape":      ("query",        "executor"),
    "collect":     ("query",        "executor"),
    # ── Load / write ────────────────────────────────────────────────────────
    "load":        ("load",         "executor"),
    "push":        ("load",         "executor"),
    "write":       ("load",         "executor"),
    "insert":      ("load",         "executor"),
    "save":        ("load",         "executor"),
    "store":       ("load",         "executor"),
    "upload":      ("load",         "executor"),
    "sink":        ("load",         "executor"),
    "dump":        ("load",         "executor"),
    "output":      ("load",         "executor"),
    "copy":        ("load",         "executor"),
    "move":        ("load",         "executor"),
    "sync":        ("load",         "executor"),
    "replicate":   ("load",         "executor"),
    "stage":       ("load",         "executor"),
    "export":      ("load",         "executor"),
    "migrate":     ("load",         "executor"),
    # ── Transform ───────────────────────────────────────────────────────────
    "transform":   ("transform",    "executor"),
    "clean":       ("transform",    "executor"),
    "cleanse":     ("transform",    "executor"),
    "process":     ("transform",    "executor"),
    "enrich":      ("transform",    "executor"),
    "deduplicate": ("transform",    "executor"),
    "dedup":       ("transform",    "executor"),
    "normalize":   ("transform",    "executor"),
    "standardize": ("transform",    "executor"),
    "compute":     ("transform",    "executor"),
    "calculate":   ("transform",    "executor"),
    "derive":      ("transform",    "executor"),
    "flatten":     ("transform",    "executor"),
    "parse":       ("transform",    "executor"),
    "format":      ("transform",    "executor"),
    "filter":      ("transform",    "executor"),
    "join":        ("transform",    "executor"),
    "merge":       ("transform",    "executor"),
    "combine":     ("transform",    "executor"),
    "append":      ("transform",    "executor"),
    "pivot":       ("transform",    "executor"),
    "unpivot":     ("transform",    "executor"),
    "reshape":     ("transform",    "executor"),
    "cast":        ("transform",    "executor"),
    # ── Aggregate ───────────────────────────────────────────────────────────
    "aggregate":   ("aggregate",    "executor"),
    "group":       ("aggregate",    "executor"),
    "summarize":   ("aggregate",    "executor"),
    "summarise":   ("aggregate",    "executor"),
    "rollup":      ("aggregate",    "executor"),
    "roll":        ("aggregate",    "executor"),
    "count":       ("aggregate",    "executor"),
    "sum":         ("aggregate",    "executor"),
    # ── Validate ────────────────────────────────────────────────────────────
    "validate":    ("validate",     "executor"),
    "check":       ("validate",     "executor"),
    "verify":      ("validate",     "executor"),
    "assert":      ("validate",     "executor"),
    "monitor":     ("validate",     "executor"),
    "audit":       ("validate",     "executor"),
    "scan":        ("validate",     "executor"),
    "test":        ("test",         "executor"),
    # ── dbt / model ─────────────────────────────────────────────────────────
    "run":         ("run",          "executor"),
    "execute":     ("execute",      "executor"),
    "build":       ("run",          "transformer"),
    "compile":     ("compile",      "transformer"),
    "model":       ("run",          "transformer"),
    "seed":        ("seed",         "transformer"),
    "snapshot":    ("snapshot",     "transformer"),
    # ── Notify / send ───────────────────────────────────────────────────────
    "notify":      ("send",         "executor"),
    "alert":       ("send",         "executor"),
    "send":        ("send",         "executor"),
    "email":       ("send",         "executor"),
    "publish":     ("publish",      "executor"),
    # ── Other ───────────────────────────────────────────────────────────────
    "archive":     ("execute",      "executor"),
    "backup":      ("execute",      "executor"),
    "profile":     ("load",         "executor"),
    "report":      ("query",        "executor"),
    "generate":    ("execute_code", "executor"),
    "create":      ("execute",      "executor"),
    "schedule":    ("execute",      "executor"),
}

# Preposition anchors
_SOURCE_PREP_RE  = re.compile(
    r"\b(?:from|out of|extract from|read from|pull from|source[d]? (?:from|in)|coming from)\s+", re.I)
_TARGET_PREP_RE  = re.compile(
    r"\b(?:into|to|load into|push to|write to|store in|insert into|save to|"
    r"destination[:]?\s*|target[:]?\s*)\s+", re.I)
_USING_PREP_RE   = re.compile(
    r"\b(?:using|with|via|through|by)\s+", re.I)

# Table / schema patterns
_SCHEMA_TABLE_RE = re.compile(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", re.I)
_QUOTED_NAME_RE  = re.compile(r'["\`]([a-z_][a-z0-9_]*)["\`]', re.I)
_TABLE_KW_RE     = re.compile(r"\btables?\s*[:\-]?\s*([a-z_][a-z0-9_]*(?:\s*,\s*[a-z_][a-z0-9_]*)*)", re.I)

# dbt model list  "models: stg_orders, int_orders_enriched, fct_daily_sales"
_DBT_MODEL_RE    = re.compile(r"\bmodels?\s*[:\-]\s*([^\n.;]+)", re.I)
_DBT_SELECT_RE   = re.compile(r"\bselect\s*[:\-]\s*([^\n.;]+)", re.I)

# File path / URI
_FILE_PATH_RE    = re.compile(
    r"(?:s3://[^\s,;]+|gs://[^\s,;]+|[a-zA-Z0-9_./-]+\.(?:csv|json|parquet|xlsx?|txt|sql))", re.I)

# Retry / timeout hints
_RETRY_RE   = re.compile(r"retri(?:es|able)?\s*[:\-]?\s*(\d+)|retry\s+(\d+)\s+times?", re.I)
_TIMEOUT_RE = re.compile(r"timeout\s*[:\-]?\s*(\d+)\s*(s|sec|second|m|min|minute)?", re.I)

# Parallel indicators
_PARALLEL_RE = re.compile(
    r"\bin\s+parallel\b|\bsimultaneously\b|\bconcurrently\b|\bat the same time\b|\bparallel\b", re.I)

# Stop words to drop from task name slugification
_STOP = frozenset({
    "and", "or", "the", "a", "an", "of", "in", "on", "for", "with", "is",
    "are", "be", "been", "being", "it", "its", "all", "each", "some", "by",
    "as", "at", "this", "that", "these", "those", "we", "our", "their",
    "will", "should", "need", "also", "then", "data", "result", "results",
    "records", "rows", "row", "file", "files", "table", "tables", "database",
    "db", "source", "target", "into", "from", "to", "using", "via",
})

# Schedule enrichments
_SCHEDULE_PATTERNS: List[Tuple[re.Pattern, Dict[str, str]]] = [
    (re.compile(r"every\s+(\d+)\s+minutes?",              re.I), {"_template": "every_n_minutes"}),
    (re.compile(r"every\s+(\d+)\s+hours?",                re.I), {"_template": "every_n_hours"}),
    (re.compile(r"twice\s+daily|every\s+12\s+hours?",     re.I), {"minute": "0", "hour": "*/12"}),
    (re.compile(r"every\s+(half[\s-]hour|30\s+min)",      re.I), {"minute": "*/30"}),
    (re.compile(r"at\s+noon|midday",                      re.I), {"minute": "0", "hour": "12"}),
    (re.compile(r"at\s+midnight",                         re.I), {"minute": "0", "hour": "0"}),
    (re.compile(r"every\s+(?:business\s+day|weekday)",    re.I), {"minute": "0", "hour": "8", "day_of_week": "0-4"}),
    (re.compile(r"(daily|every\s+day).*?at\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)?", re.I), {"_template": "daily_at"}),
    (re.compile(r"(daily|every\s+day)",                   re.I), {"minute": "0", "hour": "0"}),
    (re.compile(r"(hourly|every\s+hour)",                 re.I), {"minute": "0"}),
    (re.compile(r"(weekly|every\s+week)(?:.*?on\s+([a-z]+))?", re.I), {"_template": "weekly"}),
    (re.compile(r"(monthly|every\s+month)",               re.I), {"minute": "0", "hour": "0", "day": "1"}),
    (re.compile(r"(quarterly|every\s+quarter)",           re.I), {"minute": "0", "hour": "0", "day": "1", "month": "*/3"}),
    (re.compile(r"minute\s*[:=]\s*['\"]?([^,'\"\s]+)",   re.I), {"_template": "cron_kv"}),
]

_DAY_MAP = {
    "monday": "0", "mon": "0", "tuesday": "1", "tue": "1",
    "wednesday": "2", "wed": "2", "thursday": "3", "thu": "3",
    "friday": "4", "fri": "4", "saturday": "5", "sat": "5",
    "sunday": "6", "sun": "6",
}

# ─────────────────────────────────────────────────────────────────────────────
# Data structure
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ParsedIntent:
    raw_text:      str
    verb:          str               = ""
    operation:     str               = ""
    task_type:     str               = "executor"
    subjects:      List[str]         = field(default_factory=list)
    source_plugin: Optional[str]     = None
    target_plugin: Optional[str]     = None
    using_plugin:  Optional[str]     = None
    tables:        List[str]         = field(default_factory=list)
    dbt_models:    List[str]         = field(default_factory=list)
    file_path:     Optional[str]     = None
    is_parallel:   bool              = False
    retries:       int               = 0
    timeout:       Optional[int]     = None
    extra:         Dict[str, Any]    = field(default_factory=dict)   # free attrs


# ─────────────────────────────────────────────────────────────────────────────
# Core helpers
# ─────────────────────────────────────────────────────────────────────────────

def _slugify(value: str, default: str = "task") -> str:
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or default


def _detect_plugin_in(text: str) -> Optional[str]:
    """Return the first matching canonical plugin name found in *text*."""
    for canonical, pattern in PLUGIN_PATTERNS:
        if re.search(pattern, text, re.I):
            return canonical
    return None


def _all_plugins_in(text: str) -> List[str]:
    """Return all canonical plugin names found (preserving priority order)."""
    found, seen = [], set()
    for canonical, pattern in PLUGIN_PATTERNS:
        if canonical not in seen and re.search(pattern, text, re.I):
            found.append(canonical)
            seen.add(canonical)
    return found


def _extract_noun_after(prep_re: re.Pattern, text: str) -> Optional[str]:
    """Return the short noun phrase immediately following a preposition pattern."""
    m = prep_re.search(text)
    if not m:
        return None
    remainder = text[m.end():].strip()
    # take up to the next preposition-like boundary or punctuation
    stop = re.search(r"\b(?:and|or|then|into|from|to|using|with|via|,|;|\bwhere\b|\bwhen\b)\b", remainder, re.I)
    phrase = remainder[: stop.start()].strip() if stop else remainder[:40].strip()
    phrase = re.sub(r"[^a-zA-Z0-9 _.\-]", "", phrase).strip()
    return phrase or None


def _extract_tables(text: str) -> List[str]:
    tables: List[str] = []
    # schema.table
    for match in _SCHEMA_TABLE_RE.finditer(text):
        tables.append(f"{match.group(1)}.{match.group(2)}")
    # back-tick / double-quoted names
    for match in _QUOTED_NAME_RE.finditer(text):
        tables.append(match.group(1))
    # "table: X, Y"
    m = _TABLE_KW_RE.search(text)
    if m:
        for part in re.split(r"[,\s]+", m.group(1)):
            p = part.strip()
            if p:
                tables.append(p)
    return list(dict.fromkeys(tables))  # deduplicate, preserve order


def _extract_dbt_models(text: str) -> List[str]:
    for pat in (_DBT_MODEL_RE, _DBT_SELECT_RE):
        m = pat.search(text)
        if m:
            parts = [p.strip() for p in re.split(r"[,\s]+", m.group(1)) if p.strip()]
            return parts
    return []


def _extract_file_path(text: str) -> Optional[str]:
    m = _FILE_PATH_RE.search(text)
    return m.group(0) if m else None


def _extract_retries(text: str) -> int:
    m = _RETRY_RE.search(text)
    if m:
        return int(m.group(1) or m.group(2) or 0)
    return 0


def _extract_timeout(text: str) -> Optional[int]:
    m = _TIMEOUT_RE.search(text)
    if not m:
        return None
    val = int(m.group(1))
    unit = (m.group(2) or "s").lower()
    if unit in ("m", "min", "minute"):
        val *= 60
    return val


def _detect_verb(text: str) -> Tuple[str, str, str]:
    """Return (raw_verb, operation, task_type) for the dominant verb in text."""
    lowered = text.lower()
    # Longest-match first so "deduplicate" beats "duplicate"
    for verb in sorted(VERB_OPS, key=len, reverse=True):
        # word-boundary check
        if re.search(rf"\b{re.escape(verb)}\b", lowered):
            op, ttype = VERB_OPS[verb]
            return verb, op, ttype
    return "", "", "executor"


def _subject_words(sentence: str, verb: str, source_phrase: Optional[str],
                   target_phrase: Optional[str]) -> List[str]:
    """Extract the key noun-phrase words from a sentence for naming."""
    # Remove source/target phrases to isolate the subject
    cleaned = sentence
    if source_phrase:
        cleaned = cleaned.replace(source_phrase, " ")
    if target_phrase:
        cleaned = cleaned.replace(target_phrase, " ")
    if verb:
        cleaned = re.sub(rf"\b{re.escape(verb)}\b", " ", cleaned, flags=re.I)

    # Remove stop words and short words
    words = re.split(r"[^a-zA-Z0-9]+", cleaned)
    return [w.lower() for w in words if w.lower() not in _STOP and len(w) > 2]


# ─────────────────────────────────────────────────────────────────────────────
# Sentence segmentation
# ─────────────────────────────────────────────────────────────────────────────

_CONNECTOR_RE = re.compile(
    r"(?:,?\s+|\s+)(?:then|next|after that|afterwards|followed by|finally|lastly|subsequently)\s+",
    re.I,
)
_BULLET_CLEAN_RE = re.compile(r"^[\s\-•*►▸◦‣·]+|^\d+[.)]\s*")


def _segment_sentences(text: str) -> List[str]:
    """
    Split free text into task-level sentences using:
    1. Bullet / numbered list items
    2. Sequential connectors (then, next, finally …)
    3. Hard sentence boundaries (. ; \\n)
    """
    # Normalize unicode + whitespace
    text = unicodedata.normalize("NFKC", text or "")
    text = re.sub(r"\r\n|\r", "\n", text)
    text = re.sub(r"\t", " ", text)
    text = re.sub(r"[ ]{2,}", " ", text)

    # Replace connectors with sentence breaks
    text = _CONNECTOR_RE.sub(". ", text)

    # Split on line breaks, bullets, semicolons, and sentence-ending periods
    raw = re.split(r"\n+|(?<=\w)\.\s+|;\s*", text)

    segments: List[str] = []
    for seg in raw:
        seg = _BULLET_CLEAN_RE.sub("", seg).strip()
        if len(seg.split()) >= 2:
            segments.append(seg)
    return segments


# ─────────────────────────────────────────────────────────────────────────────
# Intent extraction
# ─────────────────────────────────────────────────────────────────────────────

def _parse_intent(sentence: str) -> ParsedIntent:
    """Convert one sentence into a ParsedIntent."""
    intent = ParsedIntent(raw_text=sentence)

    # ── Parallel flag ─────────────────────────────────────────────────────
    intent.is_parallel = bool(_PARALLEL_RE.search(sentence))

    # ── Verb / operation / task_type ──────────────────────────────────────
    intent.verb, intent.operation, intent.task_type = _detect_verb(sentence)

    # ── Preposition-based source / target ─────────────────────────────────
    src_phrase = _extract_noun_after(_SOURCE_PREP_RE, sentence)
    tgt_phrase = _extract_noun_after(_TARGET_PREP_RE, sentence)
    use_phrase = _extract_noun_after(_USING_PREP_RE, sentence)

    intent.source_plugin = _detect_plugin_in(src_phrase or "") if src_phrase else None
    intent.target_plugin = _detect_plugin_in(tgt_phrase or "") if tgt_phrase else None
    intent.using_plugin  = _detect_plugin_in(use_phrase or "") if use_phrase else None

    # ── Fallback: scan whole sentence for plugins ──────────────────────────
    all_plugins = _all_plugins_in(sentence)
    if not intent.source_plugin and not intent.target_plugin:
        if len(all_plugins) >= 2:
            # Assume first = source, second = target (common pattern)
            intent.source_plugin = all_plugins[0]
            intent.target_plugin = all_plugins[1]
        elif len(all_plugins) == 1:
            # Single plugin: infer role from operation
            plug = all_plugins[0]
            if intent.operation in ("query", "validate", "load"):
                intent.source_plugin = plug
            else:
                intent.target_plugin = plug
    elif not intent.source_plugin and all_plugins:
        intent.source_plugin = all_plugins[0]
    elif not intent.target_plugin and len(all_plugins) > 1:
        for p in all_plugins:
            if p != intent.source_plugin:
                intent.target_plugin = p
                break

    # ── Tables ────────────────────────────────────────────────────────────
    intent.tables = _extract_tables(sentence)

    # ── dbt models ───────────────────────────────────────────────────────
    if intent.source_plugin == "dbt" or intent.target_plugin == "dbt" or "dbt" in sentence.lower():
        intent.dbt_models = _extract_dbt_models(sentence)

    # ── File path ─────────────────────────────────────────────────────────
    intent.file_path = _extract_file_path(sentence)

    # ── Retries / timeout ─────────────────────────────────────────────────
    intent.retries = _extract_retries(sentence)
    intent.timeout = _extract_timeout(sentence)

    # ── Subject words (for naming) ────────────────────────────────────────
    intent.subjects = _subject_words(sentence, intent.verb, src_phrase, tgt_phrase)

    return intent


# ─────────────────────────────────────────────────────────────────────────────
# Task name generation
# ─────────────────────────────────────────────────────────────────────────────

def _make_task_name(intent: ParsedIntent, index: int) -> str:
    parts: List[str] = []

    # 1. Start with the verb (operation word), prefer the raw verb
    if intent.verb:
        parts.append(intent.verb[:12])

    # 2. Add up to 2 meaningful subject words
    for w in intent.subjects[:2]:
        if w not in parts and w not in _STOP:
            parts.append(w[:12])

    # 3. Add source system (most specific context)
    if intent.source_plugin and intent.source_plugin not in parts:
        parts.append(intent.source_plugin)

    # 4. Add target if different and space allows
    if intent.target_plugin and intent.target_plugin not in parts and len(parts) < 5:
        parts.append(intent.target_plugin)

    # 5. Fallback
    if not parts:
        parts = [f"task_{index}"]

    return "_".join(parts[:5])


# ─────────────────────────────────────────────────────────────────────────────
# Config population
# ─────────────────────────────────────────────────────────────────────────────

# Import at call time to avoid circular import
def _get_template(plugin: str, operation: Optional[str]) -> Dict[str, Any]:
    from dataplatform.core.pipeline_generator import _get_config_template
    return _get_config_template(plugin, operation)


def _build_config(intent: ParsedIntent) -> Dict[str, Any]:
    """
    Build a plugin config dict from intent, populating it with detected
    table names, file paths, SQL, and dbt models wherever possible.
    """
    primary_plugin = (
        intent.target_plugin or intent.source_plugin or intent.using_plugin or "python"
    )
    # For operations that explicitly operate on the source, use source plugin
    if intent.operation in ("query", "validate", "load") and intent.source_plugin:
        primary_plugin = intent.source_plugin

    config = _get_template(primary_plugin, intent.operation)

    # ── Populate file_path ────────────────────────────────────────────────
    fp = intent.file_path
    if fp and "file_path" in config:
        config["file_path"] = fp
    if fp and "source_file" in config:
        config["source_file"] = fp

    # ── Populate table name ───────────────────────────────────────────────
    if intent.tables:
        first_table = intent.tables[0]
        if "table_name" in config:
            config["table_name"] = first_table
        if "sql" in config and "my_table" in str(config["sql"]):
            config["sql"] = f"SELECT * FROM {first_table} LIMIT 1000"
        # BigQuery table_id
        if "table_id" in config:
            config["table_id"] = first_table

    # ── Populate SQL for duckdb query with tables ─────────────────────────
    if primary_plugin == "duckdb" and intent.tables and "sql" in config:
        config["sql"] = f"SELECT * FROM {intent.tables[0]} LIMIT 1000"

    # ── Populate dbt select ───────────────────────────────────────────────
    if primary_plugin == "dbt" and intent.dbt_models:
        config["select"] = " ".join(intent.dbt_models)
        config.pop("select", None)  # remove None placeholder
        config["select"] = " ".join(intent.dbt_models)

    # ── dbt-specific target config ─────────────────────────────────────────
    if intent.target_plugin == "dbt" or intent.source_plugin == "dbt":
        config = _get_template("dbt", intent.operation or "run")
        if intent.dbt_models:
            config["select"] = " ".join(intent.dbt_models)

    return config


# ─────────────────────────────────────────────────────────────────────────────
# Lineage inference
# ─────────────────────────────────────────────────────────────────────────────

def _build_lineage(intent: ParsedIntent) -> Optional[Dict[str, Any]]:
    lineage: Dict[str, Any] = {}

    def _uri(plugin: Optional[str], table: Optional[str] = None) -> Optional[str]:
        if not plugin:
            return None
        if plugin == "file":
            return f"file://{intent.file_path or 'data/input'}"
        if plugin in ("postgres", "mysql"):
            t = table or (intent.tables[0] if intent.tables else "table")
            return f"{plugin}://host/{t}"
        if plugin == "snowflake":
            t = table or (intent.tables[0] if intent.tables else "table")
            return f"snowflake://account/MY_DB/PUBLIC/{t}"
        if plugin == "bigquery":
            t = table or (intent.tables[0] if intent.tables else "table")
            return f"bigquery://project/dataset/{t}"
        if plugin == "dbt":
            return "dbt://models"
        if plugin == "kafka":
            return "kafka://broker/topic"
        return None

    src_uri = _uri(intent.source_plugin)
    tgt_uri = _uri(intent.target_plugin)
    if intent.file_path:
        file_uri = f"file://{intent.file_path}"
        if not src_uri:
            src_uri = file_uri
        elif not tgt_uri:
            tgt_uri = file_uri

    if src_uri:
        lineage["reads_from"] = [src_uri]
    if tgt_uri:
        lineage["writes_to"] = [tgt_uri]

    return lineage if lineage else None


# ─────────────────────────────────────────────────────────────────────────────
# Schedule parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_schedule(text: str) -> Optional[Dict[str, str]]:
    lowered = text.lower()
    for pat, template in _SCHEDULE_PATTERNS:
        m = pat.search(lowered)
        if not m:
            continue

        tmpl = template.get("_template")

        if tmpl == "every_n_minutes":
            return {"minute": f"*/{m.group(1)}"}

        if tmpl == "every_n_hours":
            return {"minute": "0", "hour": f"*/{m.group(1)}"}

        if tmpl == "daily_at":
            hour = int(m.group(2))
            minute = m.group(3) or "0"
            meridiem = (m.group(4) or "").lower()
            if meridiem == "pm" and hour < 12:
                hour += 12
            elif meridiem == "am" and hour == 12:
                hour = 0
            return {"minute": str(minute), "hour": str(hour)}

        if tmpl == "weekly":
            sched: Dict[str, str] = {"minute": "0", "hour": "0", "day_of_week": "0"}
            if m.lastindex and m.lastindex >= 2 and m.group(2):
                day_name = m.group(2).strip().lower()[:3]
                sched["day_of_week"] = _DAY_MAP.get(day_name, "0")
            return sched

        if tmpl == "cron_kv":
            sched = {"minute": m.group(1)}
            for key in ("hour", "day", "month", "day_of_week"):
                km = re.search(rf"{key}\s*[:=]\s*['\"]?([^,'\"\s]+)", lowered)
                if km:
                    sched[key] = km.group(1)
            return sched

        # Static template (no dynamic groups needed)
        return {k: v for k, v in template.items() if not k.startswith("_")}

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline name extraction
# ─────────────────────────────────────────────────────────────────────────────

_NAME_PATTERNS = [
    r"pipeline\s+name\s*[:\-]\s*([^\n]+)",
    r"pipeline\s*[:\-]\s*([^\n]+)",
    r"name\s*[:\-]\s*([^\n]+)",
    r"called\s+([a-zA-Z0-9 _\-]+)",
    r"(?:build|create|generate)\s+(?:an?\s+)?(.+?)\s+pipeline\b",
    r"(.+?)\s+(?:data\s+)?pipeline\b",
]

def extract_pipeline_name(text: str) -> Optional[str]:
    for pattern in _NAME_PATTERNS:
        m = re.search(pattern, text, re.I)
        if m:
            candidate = m.group(1).strip(" .:")
            slug = _slugify(candidate, default="")
            if slug and len(slug) >= 3:
                return slug[:64]
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Tags inference
# ─────────────────────────────────────────────────────────────────────────────

_TAG_RULES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"\b(?:sales|revenue|order|ecommerce|e-commerce)\b", re.I), "sales"),
    (re.compile(r"\b(?:finance|financial|accounting|ledger|billing)\b", re.I), "finance"),
    (re.compile(r"\b(?:marketing|campaign|ads?|click|impression)\b", re.I), "marketing"),
    (re.compile(r"\b(?:analytics|kpi|metric|dashboard|report)\b", re.I), "analytics"),
    (re.compile(r"\b(?:etl|ingest|extract|pipeline)\b", re.I), "etl"),
    (re.compile(r"\b(?:dbt|transform|model)\b", re.I), "dbt"),
    (re.compile(r"\b(?:validate|quality|check|assert)\b", re.I), "data_quality"),
    (re.compile(r"\b(?:user|customer|profile|crm)\b", re.I), "customer"),
    (re.compile(r"\b(?:log|event|stream|kafka)\b", re.I), "streaming"),
    (re.compile(r"\b(?:ml|machine\s+learning|model|train|predict)\b", re.I), "ml"),
    (re.compile(r"\b(?:daily|hourly|scheduled|cron)\b", re.I), "scheduled"),
    (re.compile(r"\b(?:snowflake|bigquery|redshift|warehouse)\b", re.I), "data_warehouse"),
]

def infer_tags(text: str) -> List[str]:
    tags: List[str] = []
    for pat, tag in _TAG_RULES:
        if pat.search(text) and tag not in tags:
            tags.append(tag)
    return tags[:6]  # cap to 6 tags


# ─────────────────────────────────────────────────────────────────────────────
# Main generate function
# ─────────────────────────────────────────────────────────────────────────────

def generate_from_text(input_text: str) -> Dict[str, Any]:
    """
    Parse *input_text* using NLP pattern matching and return a dict with:
      yaml_content, parsed_config, warnings, detected_language,
      nlp_summary (new — intent breakdown per task)
    """
    warnings: List[str] = []
    nlp_summary: List[Dict[str, Any]] = []

    # ── Normalise ─────────────────────────────────────────────────────────
    text = unicodedata.normalize("NFKC", input_text or "").strip()
    if not text:
        text = "generated pipeline"
        warnings.append("Input was empty; used placeholder content.")

    # ── Pipeline name ─────────────────────────────────────────────────────
    pipeline_name = extract_pipeline_name(text)
    if not pipeline_name:
        pipeline_name = "generated_pipeline"
        warnings.append("Could not infer pipeline name; defaulted to 'generated_pipeline'.")

    # ── Schedule ──────────────────────────────────────────────────────────
    schedule = parse_schedule(text)
    if not schedule:
        warnings.append("No schedule found in description; schedule omitted.")

    # ── Tags ──────────────────────────────────────────────────────────────
    tags = infer_tags(text)

    # ── Segment sentences ─────────────────────────────────────────────────
    sentences = _segment_sentences(text)

    # Filter to sentences that look task-like
    task_sentences = [
        s for s in sentences
        if _detect_verb(s)[0]  # has a recognizable verb
        or any(re.search(pat, s, re.I) for _, pat in PLUGIN_PATTERNS)
    ]

    if not task_sentences:
        # Fall back to all non-trivial sentences
        task_sentences = [s for s in sentences if len(s.split()) >= 3]

    if not task_sentences:
        task_sentences = [text]
        warnings.append("Could not segment distinct tasks; treating entire input as one task.")

    # ── Build intents ─────────────────────────────────────────────────────
    intents: List[ParsedIntent] = [_parse_intent(s) for s in task_sentences]

    # ── Remove schedule-only sentences that produced no meaningful intent ──
    meaningful: List[ParsedIntent] = []
    for intent in intents:
        # Skip if the raw sentence is purely a schedule description
        if not intent.verb and not intent.source_plugin and not intent.target_plugin:
            if re.search(r"\b(?:daily|hourly|weekly|schedule|cron)\b", intent.raw_text, re.I):
                continue
        meaningful.append(intent)

    if not meaningful:
        meaningful = intents  # keep all if filter removed everything

    # ── Convert intents → tasks ───────────────────────────────────────────
    tasks: List[Dict[str, Any]] = []
    seen_names: set = set()

    # Group parallel intents: sentences marked is_parallel share the same wave
    # (They get the same depends_on as the preceding task)
    parallel_group_start: Optional[int] = None

    for idx, intent in enumerate(meaningful):
        name_base = _make_task_name(intent, idx + 1)
        name = _slugify(name_base, default=f"task_{idx + 1}")
        # Ensure uniqueness
        suffix = 1
        original_name = name
        while name in seen_names:
            suffix += 1
            name = f"{original_name}_{suffix}"
        seen_names.add(name)

        primary_plugin = (
            intent.target_plugin or intent.source_plugin or intent.using_plugin or "python"
        )
        # Adjust plugin for operation context
        if intent.operation in ("query", "validate") and intent.source_plugin:
            primary_plugin = intent.source_plugin
        if intent.operation in ("send",) and intent.using_plugin == "email":
            primary_plugin = "email"

        # Correct task_type for dbt
        task_type = intent.task_type
        if primary_plugin == "dbt":
            task_type = "transformer"

        config = _build_config(intent)
        lineage = _build_lineage(intent)

        task: Dict[str, Any] = {
            "name": name,
            "id": name,
            "type": task_type,
            "plugin": primary_plugin,
            "retries": intent.retries,
        }
        if intent.operation:
            task["config"] = {"operation": intent.operation, **config}
        else:
            task["config"] = config

        if intent.timeout:
            task["timeout"] = intent.timeout

        if lineage:
            task["lineage"] = lineage

        # Dependency wiring
        if intent.is_parallel and idx > 0:
            # Share same depends_on as previous task
            prev_deps = tasks[idx - 1].get("depends_on") if tasks else None
            if prev_deps:
                task["depends_on"] = prev_deps
            elif idx >= 2:
                task["depends_on"] = [tasks[idx - 2]["name"]]
        elif idx > 0:
            task["depends_on"] = [tasks[-1]["name"]]

        tasks.append(task)

        # NLP summary entry (for UI display)
        nlp_summary.append({
            "sentence": intent.raw_text[:120],
            "verb":     intent.verb,
            "operation": intent.operation,
            "source":   intent.source_plugin,
            "target":   intent.target_plugin,
            "tables":   intent.tables,
            "models":   intent.dbt_models,
            "parallel": intent.is_parallel,
        })

    if not tasks:
        tasks = [{
            "name": "default_task",
            "id": "default_task",
            "type": "executor",
            "plugin": "python",
            "config": {"operation": "execute_code", "code": "print('Hello from pipeline')"},
            "retries": 0,
        }]
        warnings.append("No tasks could be extracted; created a default Python task.")

    # ── Assemble pipeline config ──────────────────────────────────────────
    from dataplatform.core.config import PipelineConfig
    import yaml

    config_payload: Dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "description": f"Auto-generated from: {text[:160].strip()}",
        "tasks": tasks,
    }
    if schedule:
        config_payload["schedule"] = schedule
    if tags:
        config_payload["tags"] = tags

    # Validate via Pydantic (may raise — caller catches)
    parsed_config = PipelineConfig(**config_payload)
    yaml_content = yaml.safe_dump(
        parsed_config.model_dump(exclude_none=True),
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )

    return {
        "yaml_content": yaml_content,
        "parsed_config": parsed_config.model_dump(exclude_none=True),
        "warnings": warnings,
        "detected_language": "nlp-pattern-matching",
        "nlp_summary": nlp_summary,
    }
