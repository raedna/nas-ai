"""
core/retrieval/analytics.py
===========================
Guarded local text-to-SQL for aggregate / metadata questions that semantic
retrieval can't answer — "how many files in the collection", "how many fits
files", "how many tickets closed in December 2025", "documents by type", etc.

Design goals (per project constraints):
  * No hardcoding — the schema (tables, columns, per-collection payload keys and
    distinct type/filetype values) is introspected live from the DB and handed
    to the LLM, so it stays file-agnostic as collections change.
  * Fully local — qwen (via LM Studio) generates the SQL; nothing leaves the box.
  * Safe — the generated SQL is validated (SELECT-only, single statement,
    whitelisted tables, no dangerous functions) and executed read-only with a
    statement timeout and row cap.

Public surface:
    maybe_run_analytics(collection, question) -> dict | None
        Router hook. Returns an answer dict when the question is a genuine
        metadata/aggregate query, else None so the caller falls back to the
        normal discovery / semantic path.
    run_analytics(collection, question) -> dict
        Force the analytics path (used by the SQL Inspector tab).
    generate_sql(question, collection) -> dict
        {is_analytics, sql, explanation} — generation only, no execution.
    validate_sql(sql) -> (ok: bool, reason: str)
    run_readonly_sql(sql, ...) -> (columns, rows)
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from core.db import fetchall, get_conn

# Tables the LLM is allowed to query. Everything else is rejected by validation.
WHITELIST = {
    "chunks", "files", "collections", "enum_values",
    "concept_vectors", "cross_links", "background_tasks",
    "chat_sessions", "chat_messages", "answer_feedback",
    "collection_vocab", "schemas", "sql_snippets",
}

# Statements / functions that must never appear in a generated query.
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|"
    r"copy|merge|call|vacuum|analyze|reindex|cluster|lock|comment|"
    r"pg_sleep|pg_read_file|pg_read_binary_file|lo_import|lo_export|dblink)\b",
    re.IGNORECASE,
)

_MAX_ROWS = 500
_TIMEOUT_MS = 8000


# ---------------------------------------------------------------------------
# Schema introspection (live, no hardcoding)
# ---------------------------------------------------------------------------
def _table_columns() -> Dict[str, List[str]]:
    try:
        rows = fetchall(
            """SELECT table_name, column_name
               FROM information_schema.columns
               WHERE table_schema = 'public' AND table_name = ANY(%s)
               ORDER BY table_name, ordinal_position""",
            (list(WHITELIST),),
        )
    except Exception:
        return {}
    out: Dict[str, List[str]] = {}
    for r in rows:
        out.setdefault(r["table_name"], []).append(r["column_name"])
    return out


def _distinct(sql: str, params: Tuple) -> List[str]:
    try:
        return [str(r["v"]) for r in fetchall(sql, params) if r.get("v") is not None]
    except Exception:
        return []


def _collection_profile(collection: Optional[str]) -> Dict[str, List[str]]:
    """Per-collection payload keys + distinct type/filetype values, so the LLM can
    map natural-language terms ('fits files', 'closed tickets') to real columns."""
    if not collection:
        return {}
    keys = _distinct(
        """SELECT DISTINCT k AS v FROM chunks, LATERAL jsonb_object_keys(payload) AS k
           WHERE collection_name = %s AND payload IS NOT NULL LIMIT 80""",
        (collection,),
    )
    doc_types = _distinct(
        "SELECT DISTINCT doc_type AS v FROM chunks WHERE collection_name = %s LIMIT 30",
        (collection,),
    )
    source_types = _distinct(
        "SELECT DISTINCT source_type AS v FROM chunks WHERE collection_name = %s LIMIT 30",
        (collection,),
    )
    filetypes = _distinct(
        "SELECT DISTINCT filetype AS v FROM files WHERE collection_name = %s LIMIT 30",
        (collection,),
    )
    return {
        "payload_keys": sorted(keys),
        "doc_types": sorted(doc_types),
        "source_types": sorted(source_types),
        "filetypes": sorted(filetypes),
    }


def schema_context(collection: Optional[str]) -> str:
    """Compact schema description for the SQL-generation prompt."""
    cols = _table_columns()
    lines = ["Tables and columns (PostgreSQL, schema=public):"]
    for t in sorted(cols):
        lines.append(f"  {t}({', '.join(cols[t])})")
    lines += [
        "",
        "Notes:",
        "  - chunks holds one row per ingested chunk; a single document/file can "
        "produce MANY chunk rows (entity rows). For file/document counts use the "
        "files table, or COUNT(DISTINCT source_file) on chunks.",
        "  - Structured per-record fields live in chunks.payload (jsonb); access "
        "them with payload->>'KeyName'. enum_values holds expanded value lists.",
        "  - File extension can be taken from files.filetype or from the end of "
        "files.file_path / chunks.source_file.",
    ]
    if collection:
        prof = _collection_profile(collection)
        lines += ["", f"Profile for collection '{collection}':"]
        if prof.get("filetypes"):
            lines.append(f"  files.filetype values: {', '.join(prof['filetypes'])}")
        if prof.get("doc_types"):
            lines.append(f"  chunks.doc_type values: {', '.join(prof['doc_types'])}")
        if prof.get("source_types"):
            lines.append(f"  chunks.source_type values: {', '.join(prof['source_types'])}")
        if prof.get("payload_keys"):
            lines.append(f"  chunks.payload keys: {', '.join(prof['payload_keys'])}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# SQL generation (local LLM, structured output)
# ---------------------------------------------------------------------------
_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "analytics_sql",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "is_analytics": {"type": "boolean"},
                "sql": {"type": "string"},
                "explanation": {"type": "string"},
            },
            "required": ["is_analytics", "sql", "explanation"],
            "additionalProperties": False,
        },
    },
}


def generate_sql(question: str, collection: Optional[str]) -> Dict[str, Any]:
    """Ask the local LLM to translate the question into a read-only SELECT.
    Returns {is_analytics, sql, explanation}. is_analytics=False means the
    question is not a metadata/aggregate DB query and should fall back."""
    from core.local_llm_client import call_local_llm_json

    ctx = schema_context(collection)
    if collection:
        scope_rule = (
            f"  - A collection is selected ('{collection}'). By DEFAULT scope the "
            f"query to it with WHERE collection_name = '{collection}'. Only query "
            "across all collections if the question explicitly says 'all "
            "collections', 'across collections', 'in total', or 'everywhere'.\n"
        )
    else:
        scope_rule = (
            "  - No specific collection is selected; query across all collections "
            "(do not add a collection_name filter unless the question names one).\n"
        )
    system = (
        "You translate natural-language questions into a SINGLE read-only "
        "PostgreSQL SELECT query against the schema below.\n\n"
        f"{ctx}\n\n"
        "Set is_analytics=true ONLY for questions answered by counting / "
        "aggregating / filtering RECORDS, FILES, or DOCUMENTS — for example: "
        "'how many files in the collection', 'how many fits files', "
        "'how many documents about recon', 'count records by type', "
        "'how many tickets were closed in December 2025', 'files per collection'.\n"
        "Set is_analytics=false for: a single-record lookup, a definition, a "
        "how-to/procedure, or a 'which/what tags|fields contain <text>' field-"
        "content search. In those cases return is_analytics=false and sql=\"\".\n\n"
        "Rules for the SQL when is_analytics=true:\n"
        "  - Exactly one statement, SELECT only. No INSERT/UPDATE/DELETE/DDL.\n"
        f"  - Only use these tables: {', '.join(sorted(WHITELIST))}.\n"
        "  - Access jsonb fields as payload->>'Key'. Cast dates with "
        "(payload->>'Key')::date and filter ranges with >= and <.\n"
        f"{scope_rule}"
        "  - For counts of files/documents prefer the files table or "
        "COUNT(DISTINCT source_file), not COUNT(*) over chunks.\n"
        "  - Use ILIKE for case-insensitive text matching.\n"
        "  - Add a LIMIT (<=200) to non-aggregate listing queries.\n\n"
        "Return only the JSON object."
    )
    try:
        result = call_local_llm_json(
            system, question, temperature=0.0, response_format=_RESPONSE_FORMAT
        )
    except Exception:
        result = None
    if not isinstance(result, dict):
        return {"is_analytics": False, "sql": "", "explanation": "generation failed"}
    return {
        "is_analytics": bool(result.get("is_analytics")),
        "sql": str(result.get("sql") or "").strip(),
        "explanation": str(result.get("explanation") or "").strip(),
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def _strip_sql(sql: str) -> str:
    # Drop line comments and trailing semicolon/whitespace.
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql.strip().rstrip(";").strip()


def validate_sql(sql: str) -> Tuple[bool, str]:
    """Enforce: non-empty, single statement, SELECT/WITH only, no forbidden
    keywords, only whitelisted tables referenced."""
    s = _strip_sql(sql)
    if not s:
        return False, "empty SQL"
    if ";" in s:
        return False, "multiple statements are not allowed"
    if not re.match(r"^\s*(with|select)\b", s, re.IGNORECASE):
        return False, "only SELECT/WITH queries are allowed"
    if _FORBIDDEN.search(s):
        return False, "query contains a forbidden keyword or function"
    # Every table referenced after FROM/JOIN must be whitelisted (CTE names allowed).
    cte_names = set(re.findall(r"(\w+)\s+AS\s*\(", s, re.IGNORECASE))
    refs = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)", s, re.IGNORECASE)
    for ref in refs:
        name = ref.split(".")[-1].lower()
        if name in cte_names:
            continue
        if name not in WHITELIST:
            return False, f"table '{ref}' is not allowed"
    return True, "ok"


def _ensure_limit(sql: str) -> str:
    s = _strip_sql(sql)
    low = s.lower()
    is_aggregate = bool(re.search(r"\bcount\s*\(|\bsum\s*\(|\bavg\s*\(|"
                                  r"\bmin\s*\(|\bmax\s*\(|\bgroup\s+by\b", low))
    if not is_aggregate and " limit " not in f" {low} ":
        s += f" LIMIT {_MAX_ROWS}"
    return s


# ---------------------------------------------------------------------------
# Read-only execution
# ---------------------------------------------------------------------------
def run_readonly_sql(sql: str, timeout_ms: int = _TIMEOUT_MS,
                     max_rows: int = _MAX_ROWS) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Execute a validated SELECT in a read-only transaction with a statement
    timeout, fetch up to max_rows, then roll back (never commits)."""
    import psycopg2.extras as _extras
    with get_conn() as conn:
        with conn.cursor(cursor_factory=_extras.RealDictCursor) as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            cur.execute(sql)
            rows = cur.fetchmany(max_rows)
            cols = [d.name for d in cur.description] if cur.description else []
        conn.rollback()
    return cols, [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Result formatting
# ---------------------------------------------------------------------------
def format_result_text(cols: List[str], rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No matching records."
    # Single scalar (e.g. a COUNT) — answer plainly.
    if len(rows) == 1 and len(cols) == 1:
        return f"**{rows[0][cols[0]]}**"
    # Small markdown table.
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows[:50]:
        out.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    if len(rows) > 50:
        out.append(f"\n_…{len(rows) - 50} more rows_")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_analytics(collection: Optional[str], question: str) -> Dict[str, Any]:
    """Full pipeline: generate -> validate -> execute -> format. Always returns a
    dict (with an 'error' key on failure). Used directly by the SQL Inspector."""
    gen = generate_sql(question, collection)
    if not gen.get("is_analytics") or not gen.get("sql"):
        return {"method": "analytics_sql", "is_analytics": False,
                "reason": gen.get("explanation", "not an analytics question"),
                "sql": gen.get("sql", ""), "result": None}
    ok, reason = validate_sql(gen["sql"])
    if not ok:
        return {"method": "analytics_sql", "is_analytics": True, "error": reason,
                "reason": gen.get("explanation", ""), "sql": gen["sql"], "result": None}
    final_sql = _ensure_limit(gen["sql"])
    try:
        cols, rows = run_readonly_sql(final_sql)
    except Exception as exc:
        return {"method": "analytics_sql", "is_analytics": True, "error": str(exc),
                "reason": gen.get("explanation", ""), "sql": final_sql, "result": None}
    return {
        "method": "analytics_sql",
        "is_analytics": True,
        "reason": gen.get("explanation", ""),
        "sql": final_sql,
        "columns": cols,
        "rows": rows,
        "result": format_result_text(cols, rows),
    }


def maybe_run_analytics(collection: Optional[str], question: str) -> Optional[Dict[str, Any]]:
    """Router hook: return an answer dict only for genuine analytics questions
    that produce a valid, executable query; otherwise None to fall back."""
    res = run_analytics(collection, question)
    if not res.get("is_analytics"):
        return None
    if res.get("error") or res.get("result") is None:
        return None
    return res
