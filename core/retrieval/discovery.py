"""
core/retrieval/discovery.py
============================
Discovery and count/list query retrieval.
Handles:
  - Intent detection (answer vs discovery_count vs discovery_list)
  - Collection item discovery (broad search)
  - Structured role matching (field-specific queries)
  - Distinct value discovery
  - Comparison query detection

Replaces core/discovery_engine.py.
Key improvement: BM25 pre-filter replaces scroll(limit=5000).
All database access goes through db_retrieval.py.
No Qdrant imports.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.field_map_loader import load_field_maps
from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    expand_terms_with_synonyms,
    load_doc_query_hints,
)
from core.schema_loader import load_collection_schemas
from core.retrieval.db_retrieval import (
    search_bm25,
    scroll_collection,
    Point,
)

QUERY_TERMS_PATH = Path(__file__).resolve().parents[2] / "config" / "query_terms.json"


# ---------------------------------------------------------------------------
# Query terms loader
# ---------------------------------------------------------------------------
def load_query_terms() -> Dict[str, Any]:
    if QUERY_TERMS_PATH.exists():
        with open(QUERY_TERMS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


# ---------------------------------------------------------------------------
# Intent matching helpers
# ---------------------------------------------------------------------------
def _matches_any_term(q: str, terms: List[str]) -> bool:
    for term in terms or []:
        term_norm = normalize_simple_text(term)
        if term_norm and re.search(rf"\b{re.escape(term_norm)}\b", q):
            return True
    return False


def _matches_any_phrase(q: str, phrases: List[str]) -> bool:
    q = normalize_simple_text(q)
    for phrase in phrases or []:
        phrase_norm = normalize_simple_text(phrase)
        if phrase_norm and phrase_norm in q:
            return True
    return False

def llm_detect_intent(question: str) -> Dict[str, str]:
    """
    Use LLaMA 8B to classify query intent and extract role/target.
    Replaces field_maps.json keyword matching for role detection.
    Falls back to detect_ask_intent if LLM unavailable.
    """
    try:
        from core.local_llm_client import call_local_llm_json

        system_prompt = (
            "You are a query intent classifier for a knowledge retrieval system. "
            "Classify the user query and extract structured search intent.\n\n"
            "Return only JSON with these fields:\n"
            "- mode: one of 'answer', 'discovery_list', 'discovery_count', 'comparison'\n"
            "- reason: brief reason for the mode\n"
            "- role: the payload field to search — one of 'primary_name', 'description', "
            "'identifier', 'type', 'enum_value', 'aliases', or null if not applicable\n"
            "- target: the specific value or substring to search for within that role field, "
            "or null if not applicable\n\n"
            "Intent modes:\n"
            "- 'answer': single record lookup, specific question, procedural/how-to question, "
            "OR incident/error question (e.g. 'what is tag 22', 'what tag is exec broker', "
            "'what tag is order quantity', 'which tag is X', 'sftp folder for gsact.txt', "
            "'what is tidal', 'how to troubleshoot X', 'steps for X', 'how to do X', "
            "'how does X work', 'error for X', 'X failed', 'issue with X', 'problem with X'). "
            "NOTE: singular 'tag' or 'field' = answer (one record). Plural 'tags'/'fields' = discovery_list. "
            "'steps for X', 'how to X', 'how does X work' are ALWAYS answer, never discovery_list.\n"
            "- 'discovery_list': queries expecting MULTIPLE DIFFERENT records "
            "(e.g. 'what files does Goldman send', 'all sftp folders', 'what tags contain price', "
            "'what fields contain ask price', 'what fields are in category X', "
            "'list all goldman files', 'what tags contain broker', "
            "'what are the Moore notes', 'which notes are about X', "
            "'show me notes in category Y', 'what notes relate to Z', "
            "'show me all notes about Moore', 'show all X', 'give me all notes on Y', "
            "'which tags have order in their name', 'which tags have ID in their name'). "
            "Cues: plural subject, the word 'all', or 'show me'/'list'/'give me' + topic. "
            "NOT for procedural/how-to/steps/error/incident questions.\n"
            "- 'discovery_count': counting query "
            "(e.g. 'how many files does Goldman have', 'how many tags contain price', "
            "'how many notes are in X')\n"
            "- 'comparison': comparing two or more items\n\n"
            "Role/target extraction examples:\n"
            "- 'which tags have order in their name' -> role: primary_name, target: order\n"
            "- 'which tags have ID in their name' -> role: primary_name, target: ID\n"
            "- 'what tags contain price' -> role: primary_name, target: price\n"
            "- 'what fields are in category airlines' -> role: type, target: airlines\n"
            "- 'what string fields are available' -> role: type, target: string\n"
            "- 'what values can tag 22 have' -> role: enum_value, target: 22\n"
            "- 'what is tag 22' -> role: null, target: null\n"
            "- 'show me all notes about Moore' -> role: null, target: null\n"
            "- 'steps for manual file loading in recon' -> role: null, target: null\n\n"
            "Return only JSON, no other text."
        )

        result = call_local_llm_json(system_prompt, question, temperature=0.0)

        if isinstance(result, dict) and "mode" in result:
            mode = result["mode"]
            if mode in {"answer", "discovery_list", "discovery_count", "comparison"}:
                return {
                    "mode": mode,
                    "reason": result.get("reason", "llm classification"),
                    "role": result.get("role") or None,
                    "target": result.get("target") or None,
                }

    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"LLM intent detection failed: {e}")

    # Fallback to rule-based
    return detect_ask_intent(question)

# ---------------------------------------------------------------------------
# Intent detection
# Unchanged from discovery_engine.py -- no DB access
# ---------------------------------------------------------------------------
def detect_ask_intent(question: str) -> Dict[str, str]:
    """
    Detect the intent of a question.
    Returns dict with 'mode' and 'reason'.
    Modes: answer / discovery_count / discovery_list / comparison
    """
    q = normalize_simple_text(question)
    words = [w for w in q.split() if w]

    if not words:
        return {"mode": "answer", "reason": "empty query"}

    hints = load_doc_query_hints()
    query_terms_cfg = load_query_terms()
    intent_routing = query_terms_cfg.get("intent_routing", {})

    if _matches_any_phrase(q, intent_routing.get("answer_patterns", [])):
        return {"mode": "answer", "reason": "answer-pattern override from query_terms"}

    if _matches_any_phrase(q, intent_routing.get("discovery_patterns", [])):
        return {"mode": "discovery_list", "reason": "discovery-pattern override from query_terms"}

    intent_rules = [
        {
            "mode": "discovery_count",
            "reason": "count-style query detected",
            "terms_key": "discovery_count_terms",
            "requires_question_word": False,
        },
        {
            "mode": "discovery_list",
            "reason": "list/show/find query detected",
            "terms_key": "discovery_list_terms",
            "requires_question_word": False,
        },
        {
            "mode": "comparison",
            "reason": "comparison query detected",
            "terms_key": "comparison_query_terms",
            "requires_question_word": False,
        },
        {
            "mode": "discovery_list",
            "reason": "distinct-values query detected",
            "terms_key": "distinct_value_query_terms",
            "requires_question_word": True,
        },
    ]

    question_words = set(hints.get("question_words", []))
    has_question_word = any(w in question_words for w in words)

    for rule in intent_rules:
        if rule.get("requires_question_word") and not has_question_word:
            continue

        if _matches_any_term(q, hints.get(rule["terms_key"], [])):
            if rule["mode"] == "discovery_list":
                ambiguous_terms = intent_routing.get("ambiguous_discovery_terms", [])
                if _matches_any_term(q, ambiguous_terms):
                    return {
                        "mode": "answer",
                        "reason": "ambiguous discovery term treated as answer from query_terms",
                    }

            return {"mode": rule["mode"], "reason": rule["reason"]}

    # Ambiguous terms (contain/contains/has/have) without a value indicator
    # are discovery list queries, not reverse enum lookups
    ambiguous_discovery = {"contain", "contains"}
    value_indicators = {"value", "values", "allowed", "valid", "option", "options"}
    has_value_indicator = any(
        re.search(rf"\b{re.escape(v)}\b", q) for v in value_indicators
    )
    if not has_value_indicator:
        if any(re.search(rf"\b{re.escape(t)}\b", q) for t in ambiguous_discovery):
            return {"mode": "discovery_list", "reason": "contain/contains without value indicator treated as discovery list"}

    return {"mode": "answer", "reason": "default answer mode"}


# ---------------------------------------------------------------------------
# Role field resolution
# ---------------------------------------------------------------------------
def resolve_payload_fields_for_role(
    collection_name: str,
    requested_role: str,
) -> List[str]:
    """Resolve which payload fields correspond to a requested role."""
    role = str(requested_role or "").strip()
    if not role:
        return []

    fields = []
    seen = set()

    def add(field):
        field = str(field or "").strip()
        if field and field not in seen:
            seen.add(field)
            fields.append(field)

    normalized_role_fields = {
        "identifier": ["identifier"],
        "primary_name": ["primary_name"],
        "description": ["description", "text"],
    }

    for field in normalized_role_fields.get(role, []):
        add(field)

    schemas = load_collection_schemas(collection_name)
    for schema in schemas.values():
        for field in schema.get(role, []) or []:
            add(field)
            add(field.lower())

    return fields


# ---------------------------------------------------------------------------
# Scoring functions
# Unchanged from discovery_engine.py -- pure Python, no DB access
# ---------------------------------------------------------------------------
def score_discovery_payload(payload: Dict, question: str) -> float:
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    words = [w for w in q_norm.split() if w and w not in stopwords]
    expanded_words = expand_terms_with_synonyms(words)

    primary_name = str(payload.get("primary_name") or "")
    description = str(payload.get("description") or payload.get("text") or "")
    identifier = str(payload.get("identifier") or "")
    source_file = str(payload.get("source_file") or "")
    type_value = str(payload.get("type") or "")

    name_norm = normalize_simple_text(primary_name)
    desc_norm = normalize_simple_text(description)
    file_norm = normalize_simple_text(source_file)
    id_norm = normalize_simple_text(identifier)
    type_norm = normalize_simple_text(type_value)

    score = 0.0

    if q_norm and q_norm == name_norm:
        score += 100.0
    elif q_norm and q_norm in name_norm:
        score += 25.0
    elif q_norm and q_norm in desc_norm:
        score += 10.0
    elif q_norm and q_norm in file_norm:
        score += 8.0
    elif q_norm and q_norm == id_norm:
        score += 40.0

    score += sum(8.0 for w in words if w in name_norm)
    score += sum(2.0 for w in words if w in desc_norm)
    score += sum(1.5 for w in words if w in file_norm)
    score += sum(3.0 for w in words if w == id_norm)
    score += sum(6.0 for w in words if w in type_norm)
    score += sum(2.0 for w in expanded_words if w in name_norm)
    score += sum(0.5 for w in expanded_words if w in desc_norm)

    score += score_metadata_fields(payload, question)
    score += score_structured_payload_metadata(payload, question)

    return score


def score_structured_payload_metadata(payload: Dict, question: str) -> float:
    if infer_doc_type(payload) != "structured":
        return 0.0

    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))
    words = [w for w in q_norm.split() if w and w not in stopwords]

    if not q_norm or not words:
        return 0.0

    ignore_keys = {"text", "description", "related_source_files", "related_file_paths", "related_image_targets"}
    score = 0.0

    for key, value in (payload or {}).items():
        if key in ignore_keys or value in [None, "", [], {}]:
            continue

        if isinstance(value, list):
            value_text = " ".join(str(v) for v in value)
        elif isinstance(value, dict):
            value_text = " ".join(str(v) for v in value.values())
        else:
            value_text = str(value)

        key_norm = normalize_simple_text(key)
        value_norm = normalize_simple_text(value_text)

        if not value_norm:
            continue

        if q_norm == value_norm:
            score += 30.0
        elif q_norm in value_norm:
            score += 12.0

        score += sum(2.0 for w in words if w in value_norm)
        score += sum(0.5 for w in words if w in key_norm)

    return score


def score_metadata_fields(payload: Dict, question: str) -> float:
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))
    words = [w for w in q_norm.split() if w and w not in stopwords]

    if not words:
        return 0.0

    ignore_keys = {"text", "description", "payload", "vector"}
    score = 0.0

    for key, value in (payload or {}).items():
        if key in ignore_keys or value in [None, "", [], {}]:
            continue

        key_norm = normalize_simple_text(key)
        if isinstance(value, list):
            value_text = " ".join(str(v) for v in value)
        elif isinstance(value, dict):
            value_text = " ".join(str(v) for v in value.values())
        else:
            value_text = str(value)

        value_norm = normalize_simple_text(value_text)
        if not value_norm:
            continue

        if q_norm and q_norm == value_norm:
            score += 40.0
        elif q_norm and q_norm in value_norm:
            score += 12.0

        score += sum(2.0 for w in words if w in value_norm)
        score += sum(1.0 for w in words if w in key_norm)

    return score


# ---------------------------------------------------------------------------
# Preview helper
# ---------------------------------------------------------------------------
def preview_text_for_payload(payload: Dict, max_len: int = 220) -> str:
    text = str(
        payload.get("description")
        or payload.get("text")
        or payload.get("ocr_text")
        or ""
    ).strip().replace("\n", " ")

    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def dedupe_discovery_results(results: List[Dict]) -> List[Dict]:
    deduped = []
    seen = set()

    for item in results or []:
        doc_type = str(item.get("doc_type") or "").strip().lower()
        identifier = str(item.get("identifier") or "").strip()
        primary_name = normalize_simple_text(item.get("primary_name") or "")
        source_file = str(item.get("source_file") or "").strip()
        source_type = str(item.get("source_type") or "").strip().lower()

        # Document/note results: the real entity is the note (source_file), not the
        # per-chunk synthetic identifier. Dedupe by note so one row per note.
        is_doc = source_type == "doc" or doc_type in {"narrative", "mixed", "procedural", "entity_row"}

        if is_doc and source_file:
            key = f"doc|file:{source_file}"
        elif identifier:
            key = f"{doc_type}|id:{identifier}"
        elif primary_name:
            key = f"{doc_type}|name:{primary_name}|file:{source_file}"
        else:
            key = f"{doc_type}|file:{source_file}|preview:{normalize_simple_text(item.get('preview') or '')[:120]}"

        if key in seen:
            continue

        seen.add(key)
        deduped.append(item)

    return deduped


# ---------------------------------------------------------------------------
# Main discovery function
# Replaces: scroll(limit=5000) + Python scoring
# New: BM25 pre-filter + Python scoring on smaller set
# ---------------------------------------------------------------------------

def llm_extract_search_terms(question: str) -> str:
    """
    Use LLaMA 8B to extract the key search terms from a discovery query.
    E.g. 'what recon files does Goldman send' -> 'Goldman'
    Falls back to basic normalization if LLM unavailable.
    """
    try:
        from core.local_llm_client import call_local_llm_json
        system_prompt = (
            "Extract ONLY the most specific entity name or topic from the user query. "
            "Remove ALL of: question words, verbs, generic nouns (fields, files, tags, records, data, recon, list). "
            "Keep ONLY: company names, identifiers, specific topics, abbreviations. "
            "Return a single short phrase, not a list, not comma-separated. "
            "Examples: 'what recon files does Goldman send' -> 'Goldman'. "
            "'what fields contain ask price' -> 'ask price'. "
            "'what tags contain broker' -> 'broker'. "
            "'what fields are in category airlines' -> 'airlines'. "
            "Return JSON: {\"terms\": \"single short phrase\"}"
        )
        # Structured output guarantees the {"terms": "..."} shape — without it some
        # models (e.g. qwen) return a different key and we'd fall back to the whole
        # question, which websearch_to_tsquery then ANDs into zero matches.
        _rf = {"type": "json_schema", "json_schema": {
            "name": "search_terms", "strict": True,
            "schema": {"type": "object",
                       "properties": {"terms": {"type": "string"}},
                       "required": ["terms"], "additionalProperties": False}}}
        result = call_local_llm_json(system_prompt, question, temperature=0.0,
                                     response_format=_rf)
        if isinstance(result, dict) and result.get("terms"):
            terms = result["terms"]
            if isinstance(terms, list):
                terms = terms[0] if terms else ""
            terms = str(terms).split(",")[0].strip()
            if terms:
                return terms

    except Exception:
        pass
    return _fallback_search_terms(question)


def _fallback_search_terms(question: str) -> str:
    """Strip question/generic words so a failed LLM extraction doesn't AND the whole
    query into zero BM25 matches. Used when the LLM is unavailable or returns nothing."""
    hints = load_doc_query_hints()
    stop = {w.lower() for w in hints.get("stopwords", [])} | {
        "what", "which", "who", "where", "how", "many", "much", "is", "are", "the",
        "a", "an", "do", "does", "have", "has", "in", "of", "for", "to", "with",
        "contain", "contains", "containing", "list", "show", "all", "available",
        "field", "fields", "file", "files", "tag", "tags", "record", "records",
        "data", "category", "name", "named", "called",
        "there", "any", "me", "give", "get", "find", "can", "please", "and", "or",
    }
    words = [w for w in normalize_simple_text(question).split() if w and w not in stop]
    return " ".join(words) if words else normalize_simple_text(question)

def discover_collection_items(
    collection_name: str,
    question: str,
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Broad discovery search across all records in a collection.
    Replaces scroll(limit=5000) + Python scoring.
    Now uses BM25 pre-filter for efficiency.
    """
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    search_query = llm_extract_search_terms(question)

    bm25_results = search_bm25(
        collection_name=collection_name,
        query=search_query,
        limit=500,
    )

    # Get accurate total from PostgreSQL — not filtered by Python scoring
    from core.retrieval.db_retrieval import fetchall
    count_rows = fetchall(
        "SELECT COUNT(*) as n FROM chunks WHERE collection_name = %s AND nlp_text_tsv @@ websearch_to_tsquery('english', %s)",
        (collection_name, search_query)
    )
    bm25_total = count_rows[0]["n"] if count_rows else 0

    scored = []
    for p in bm25_results:
        payload = p.payload or {}
        score = score_discovery_payload(payload, question)
        if score > 0:
            scored.append((score, payload))

    scored.sort(key=lambda x: x[0], reverse=True)

    results = []
    for score, payload in scored[:limit]:
        results.append({
            "score": score,
            "doc_type": infer_doc_type(payload),
            "identifier": payload.get("identifier"),
            "primary_name": payload.get("primary_name"),
            "source_type": payload.get("source_type"),
            "source_file": payload.get("source_file"),
            "preview": preview_text_for_payload(payload),
            "payload": payload,
        })

    results = dedupe_discovery_results(results)

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    print(">>> DEBUG discover: bm25_total=", bm25_total, "| results=", len(results))
    return {"total_matches": bm25_total, "results": results}

    #return {"total_matches": len(results), "results": results}
    return {"total_matches": bm25_total, "results": results}


# ---------------------------------------------------------------------------
# Structured role match discovery
# ---------------------------------------------------------------------------
def _try_float(val: Any) -> Optional[float]:
    try:
        return float(str(val).strip())
    except Exception:
        return None


def _try_datetime(val: Any) -> Optional[datetime]:
    s = str(val).strip()
    if not s or s in ["None", "N/A"]:
        return None
    candidates = [
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
        "%Y%m%d-%H%M%S", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


def discover_structured_role_matches(
    collection_name: str,
    question: str,
    requested_role: str,
    target_text: str,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    """Find structured records where a role field matches filter criteria."""
    payload_fields = resolve_payload_fields_for_role(collection_name, requested_role)
    if not payload_fields:
        return None

    field_maps = load_field_maps()
    parsed_filter = parse_structured_filter_query(question, requested_role, field_maps)
    operator = parsed_filter.get("operator", "contains")

    raw_query_value = str(parsed_filter.get("value") or target_text or "").strip()
    q_norm = normalize_simple_text(raw_query_value)

    if not raw_query_value:
        return None

    query_num = _try_float(raw_query_value)
    query_dt = _try_datetime(raw_query_value)

    # BM25 pre-filter on structured records
    candidates = search_bm25(
        collection_name=collection_name,
        query=raw_query_value,
        doc_type="structured",
        limit=500,
    )

    results = []
    seen = set()

    for p in candidates:
        payload = p.payload or {}

        matched = False
        for field_name in payload_fields:
            raw_val = payload.get(field_name)
            if raw_val in [None, "", [], {}]:
                continue

            raw_text = str(raw_val).strip()
            field_norm = normalize_simple_text(raw_text)
            field_num = _try_float(raw_val)
            field_dt = _try_datetime(raw_val)

            if operator == "contains":
                if q_norm in field_norm:
                    matched = True
            elif operator == "eq":
                if query_dt and field_dt and field_dt.date() == query_dt.date():
                    matched = True
                elif query_num is not None and field_num is not None and field_num == query_num:
                    matched = True
                elif q_norm == field_norm:
                    matched = True
            elif operator == "gt" and query_num is not None and field_num is not None:
                matched = field_num > query_num
            elif operator == "gte" and query_num is not None and field_num is not None:
                matched = field_num >= query_num
            elif operator == "lt" and query_num is not None and field_num is not None:
                matched = field_num < query_num
            elif operator == "lte" and query_num is not None and field_num is not None:
                matched = field_num <= query_num
            elif operator == "after" and query_dt and field_dt:
                matched = field_dt > query_dt
            elif operator == "before" and query_dt and field_dt:
                matched = field_dt < query_dt

            if matched:
                break

        if not matched:
            continue

        identifier = str(payload.get("identifier") or "").strip()
        primary_name = normalize_simple_text(payload.get("primary_name") or "")
        source_file = str(payload.get("source_file") or "").strip()

        key = f"id:{identifier}" if identifier else f"name:{primary_name}|file:{source_file}"
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "score": 100.0,
            "doc_type": infer_doc_type(payload),
            "identifier": payload.get("identifier"),
            "primary_name": payload.get("primary_name"),
            "source_type": payload.get("source_type"),
            "source_file": payload.get("source_file"),
            "preview": preview_text_for_payload(payload),
            "payload": payload,
        })

    for i, item in enumerate(results[:limit], start=1):
        item["rank"] = i

    return {"total_matches": len(results), "results": results[:limit]}


def discover_structured_role_distinct_values(
    collection_name: str,
    requested_role: str,
    limit: int = 200,
) -> Optional[Dict[str, Any]]:
    """Find all distinct values for a role field across structured records."""
    payload_fields = resolve_payload_fields_for_role(collection_name, requested_role)
    if not payload_fields:
        return None

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="structured",
        limit=5000,
    )

    value_rows = []
    seen_values = set()

    for p in points:
        payload = p.payload or {}

        for field_name in payload_fields:
            raw_val = payload.get(field_name)
            if raw_val in [None, "", [], {}]:
                continue

            value_text = str(raw_val).strip()
            value_norm = normalize_simple_text(value_text)
            if not value_norm or value_norm in seen_values:
                continue

            seen_values.add(value_norm)
            value_rows.append({
                "score": 100.0,
                "doc_type": infer_doc_type(payload),
                "identifier": payload.get("identifier"),
                "primary_name": payload.get("primary_name"),
                "source_type": payload.get("source_type"),
                "source_file": payload.get("source_file"),
                "preview": f"{field_name}: {value_text}",
                "payload": payload,
                "distinct_value": value_text,
                "distinct_field": field_name,
            })

    value_rows.sort(key=lambda x: normalize_simple_text(x["distinct_value"]))

    for i, item in enumerate(value_rows[:limit], start=1):
        item["rank"] = i

    return {"total_matches": len(value_rows), "results": value_rows[:limit]}


# ---------------------------------------------------------------------------
# Filter query parsing
# Unchanged from discovery_engine.py -- pure Python, no DB access
# ---------------------------------------------------------------------------
def extract_role_target_text(
    question: str,
    requested_role: str,
    field_maps: Dict,
) -> str:
    q_norm = normalize_simple_text(question)
    role_keywords = []

    for role, keywords in field_maps.items():
        if role == requested_role:
            role_keywords.extend(keywords)

    cleaned = q_norm
    for kw in sorted(set(role_keywords), key=len, reverse=True):
        kw_norm = normalize_simple_text(kw)
        if kw_norm:
            cleaned = cleaned.replace(kw_norm, " ")

    hints = load_doc_query_hints()
    noise = set(hints.get("discovery_noise_words", []))
    words = [w for w in cleaned.split() if w and w not in noise]
    return " ".join(words).strip()


def parse_structured_filter_query(
    question: str,
    requested_role: str,
    field_maps: Dict,
) -> Dict[str, str]:
    raw_q = str(question or "").strip()
    operator = "contains"
    value = ""

    hints = load_doc_query_hints()
    operator_rules = hints.get("structured_filter_operators", [])

    for rule in operator_rules:
        op = rule.get("operator")
        terms = rule.get("terms", [])
        for term in terms:
            term_norm = str(term or "").strip()
            if not term_norm:
                continue
            pattern = rf"\b{re.escape(term_norm)}\b\s*(.+)$"
            m = re.search(pattern, raw_q, flags=re.IGNORECASE)
            if m:
                operator = op
                value = m.group(1).strip()
                break
        if value:
            break

    if not value:
        value = extract_role_target_text(question, requested_role, field_maps)
        cleanup_terms = (
            hints.get("role_target_cleanup_terms", {})
            .get(requested_role, [])
        )
        for term in cleanup_terms:
            term_norm = normalize_simple_text(term)
            if term_norm:
                value = re.sub(rf"\b{re.escape(term_norm)}\b", " ", value, flags=re.IGNORECASE)
        value = " ".join(value.split())

    return {"role": requested_role, "operator": operator, "value": value}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def run_discovery_with_method(
    collection_name: str,
    question: str,
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Run discovery query and return results with method info.
    Entry point called from router.py.
    Role/target now extracted by LLM — no field_maps.json keyword matching.
    """
    intent = llm_detect_intent(question)
    q_norm = normalize_simple_text(question)

    requested_role = intent.get("role") or None
    target_text = intent.get("target") or None

    hints = load_doc_query_hints()
    distinct_value_query = (
        "what" in q_norm.split()
        and _matches_any_term(q_norm, hints.get("distinct_value_query_terms", []))
    )

    if requested_role:
        if intent["mode"] == "discovery_list" and distinct_value_query and requested_role == "enum_value":
            distinct_discovery = discover_structured_role_distinct_values(
                collection_name=collection_name,
                requested_role=requested_role,
                limit=limit,
            )
            if distinct_discovery is not None:
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured distinct values",
                    "result": distinct_discovery,
                }

        if target_text and requested_role == "primary_name" and len(target_text.split()) <= 2:
            field_maps = load_field_maps()
            structured_discovery = discover_structured_role_matches(
                collection_name=collection_name,
                question=question,
                requested_role=requested_role,
                target_text=target_text,
                limit=limit,
            )
            if structured_discovery is not None and structured_discovery.get("results"):
                if intent["mode"] == "discovery_count":
                    return {
                        "method": intent["mode"],
                        "reason": f"{intent['reason']} using structured field match",
                        "result": f"Found {structured_discovery.get('total_matches', len(structured_discovery.get('results', [])))} matching records.",
                    }
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured field match",
                    "result": structured_discovery,
                }
            elif structured_discovery is not None and not structured_discovery.get("results"):
                from core.retrieval.db_retrieval import fetchall
                fallback_rows = fetchall(
                    """SELECT payload FROM chunks
                       WHERE collection_name = %s
                       AND payload->>'primary_name' ILIKE %s
                       LIMIT %s""",
                    (collection_name, f"%{target_text}%", limit)
                )
                if fallback_rows:
                    fallback_results = []
                    for row in fallback_rows:
                        payload = row.get("payload") or {}
                        fallback_results.append({
                            "score": 10.0,
                            "doc_type": infer_doc_type(payload),
                            "identifier": payload.get("identifier"),
                            "primary_name": payload.get("primary_name"),
                            "source_type": payload.get("source_type"),
                            "source_file": payload.get("source_file"),
                            "preview": preview_text_for_payload(payload),
                            "payload": payload,
                        })
                    fallback_results = dedupe_discovery_results(fallback_results)
                    for i, item in enumerate(fallback_results, start=1):
                        item["rank"] = i
                    if intent["mode"] == "discovery_count":
                        return {
                            "method": intent["mode"],
                            "reason": f"{intent['reason']} using name substring match",
                            "result": f"Found {len(fallback_results)} matching records.",
                        }
                    return {
                        "method": intent["mode"],
                        "reason": f"{intent['reason']} using name substring match",
                        "result": {"total_matches": len(fallback_results), "results": fallback_results},
                    }

        if distinct_value_query and requested_role == "enum_value":
            distinct_discovery = discover_structured_role_distinct_values(
                collection_name=collection_name,
                requested_role=requested_role,
                limit=limit,
            )
            if distinct_discovery is not None:
                if intent["mode"] == "discovery_count":
                    _count = len(distinct_discovery) if isinstance(distinct_discovery, list) else distinct_discovery.get("total_matches", 0)
                    return {
                        "method": intent["mode"],
                        "reason": f"{intent['reason']} using structured distinct values",
                        "result": f"Found {_count} matching records.",
                    }
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured distinct values",
                    "result": distinct_discovery,
                }

        if requested_role == "enum_value":
            distinct_discovery = discover_structured_role_distinct_values(
                collection_name=collection_name,
                requested_role=requested_role,
                limit=limit,
            )
            if distinct_discovery is not None:
                if intent["mode"] == "discovery_count":
                    _count = len(distinct_discovery) if isinstance(distinct_discovery, list) else distinct_discovery.get("total_matches", 0)
                    return {
                        "method": intent["mode"],
                        "reason": f"{intent['reason']} using structured distinct values",
                        "result": f"Found {_count} matching records.",
                    }
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured distinct values",
                    "result": distinct_discovery,
                }

    discovery = discover_collection_items(collection_name, question, limit=limit)

    if intent["mode"] == "discovery_count":
        total = discovery.get("total_matches", 0)
        return {
            "method": intent["mode"],
            "reason": intent["reason"],
            "result": f"Found {total} matching records.",
        }

    return {
        "method": intent["mode"],
        "reason": intent["reason"],
        "result": discovery,
    }
