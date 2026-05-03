from __future__ import annotations

import json
import re
from pathlib import Path

from qdrant_client import QdrantClient

from core.system_config import load_system_config
from core.field_map_loader import load_field_maps
from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    expand_terms_with_synonyms,
    load_doc_query_hints,
)

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])


def _matches_any_term(q, terms):
    for term in terms or []:
        term_norm = normalize_simple_text(term)
        if term_norm and re.search(rf"\b{re.escape(term_norm)}\b", q):
            return True
    return False


def detect_ask_intent(question: str):
    q = normalize_simple_text(question)
    words = [w for w in q.split() if w]

    if not words:
        return {"mode": "answer", "reason": "empty query"}

    hints = load_doc_query_hints()

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
            return {
                "mode": rule["mode"],
                "reason": rule["reason"],
            }

    return {"mode": "answer", "reason": "default answer mode"}

def resolve_payload_fields_for_role(collection, requested_role):
    from core.schema_loader import load_collection_schemas

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

    # normalized payload roles
    normalized_role_fields = {
        "identifier": ["identifier"],
        "primary_name": ["primary_name"],
        "description": ["description", "text"],
    }

    for field in normalized_role_fields.get(role, []):
        add(field)

    schemas = load_collection_schemas(collection)

    for schema in schemas.values():
        for field in schema.get(role, []) or []:
            add(field)
            add(field.lower())

    return fields


def score_discovery_payload(payload, question: str):
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    words = [w for w in q_norm.split() if w and w not in stopwords]
    expanded_words = expand_terms_with_synonyms(words)

    primary_name = str(payload.get("primary_name") or "")
    description = str(payload.get("description") or payload.get("text") or "")
    identifier = str(payload.get("identifier") or "")
    source_file = str(payload.get("source_file") or "")

    name_norm = normalize_simple_text(primary_name)
    desc_norm = normalize_simple_text(description)
    file_norm = normalize_simple_text(source_file)
    id_norm = normalize_simple_text(identifier)

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

    score += sum(2.0 for w in expanded_words if w in name_norm)
    score += sum(0.5 for w in expanded_words if w in desc_norm)

    score += score_metadata_fields(payload, question)
    score += score_structured_payload_metadata(payload, question)

    return score

def score_structured_payload_metadata(payload, question: str):
    if infer_doc_type(payload) != "structured":
        return 0.0

    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))
    words = [w for w in q_norm.split() if w and w not in stopwords]

    if not q_norm or not words:
        return 0.0

    ignore_keys = {
        "text",
        "description",
        "related_source_files",
        "related_file_paths",
        "related_image_targets",
    }

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


def score_metadata_fields(payload, question: str):
    q_norm = normalize_simple_text(question)
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    words = [w for w in q_norm.split() if w and w not in stopwords]
    if not words:
        return 0.0

    ignore_keys = {
        "text", "description", "payload", "vector",
    }

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

        # direct whole-query match in metadata value
        if q_norm and q_norm == value_norm:
            score += 40.0
        elif q_norm and q_norm in value_norm:
            score += 12.0

        # term hits in metadata value
        score += sum(2.0 for w in words if w in value_norm)

        # reward metadata-field-name relevance too
        score += sum(1.0 for w in words if w in key_norm)

    return score

def preview_text_for_payload(payload, max_len=220):
    text = str(
        payload.get("description")
        or payload.get("text")
        or payload.get("ocr_text")
        or ""
    ).strip().replace("\n", " ")

    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


def dedupe_discovery_results(results):
    deduped = []
    seen = set()

    for item in results or []:
        payload = item.get("payload", {}) or {}

        doc_type = str(item.get("doc_type") or "").strip().lower()
        identifier = str(item.get("identifier") or "").strip()
        primary_name = normalize_simple_text(item.get("primary_name") or "")
        source_file = str(item.get("source_file") or "").strip()

        if identifier:
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


def discover_collection_items(collection, question, limit=200):
    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    scored = []

    for p in points:
        payload = p.payload or {}
        score = score_discovery_payload(payload, question)

        if score <= 0:
            continue

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
            "payload": payload
        })

    results = dedupe_discovery_results(results)

    for i, item in enumerate(results, start=1):
        item["rank"] = i

    return {
        "total_matches": len(results),
        "results": results
    }

def discover_structured_role_matches(collection, question, requested_role, target_text, limit=200):
    payload_fields = resolve_payload_fields_for_role(collection, requested_role)

    field_maps = load_field_maps()
    parsed_filter = parse_structured_filter_query(question, requested_role, field_maps)
    operator = parsed_filter.get("operator", "contains")

    raw_query_value = str(parsed_filter.get("value") or target_text or "").strip()
    q_norm = normalize_simple_text(raw_query_value)

    if not raw_query_value:
        return None

    def try_float(val):
        try:
            return float(str(val).strip())
        except Exception:
            return None

    def try_datetime(val):
        s = str(val).strip()
        if not s or s in ["None", "N/A"]:
            return None

        candidates = [
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y%m%d-%H%M%S",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
        ]

        from datetime import datetime
        for fmt in candidates:
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        return None

    query_num = try_float(raw_query_value)
    query_dt = try_datetime(raw_query_value)

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    results = []
    seen = set()

    for p in points:
        payload = p.payload or {}

        if infer_doc_type(payload) != "structured":
            continue

        matched = False

        for field_name in payload_fields:
            raw_val = payload.get(field_name)
            if raw_val in [None, "", [], {}]:
                continue

            raw_text = str(raw_val).strip()
            field_norm = normalize_simple_text(raw_text)

            field_num = try_float(raw_val)
            field_dt = try_datetime(raw_val)

            if operator == "contains":
                if q_norm in field_norm:
                    matched = True

            elif operator == "eq":
                if query_dt is not None and field_dt is not None:
                    if field_dt.date() == query_dt.date():
                        matched = True
                elif query_num is not None and field_num is not None:
                    if field_num == query_num:
                        matched = True
                elif q_norm == field_norm:
                    matched = True

            elif operator == "gt" and query_num is not None and field_num is not None:
                if field_num > query_num:
                    matched = True

            elif operator == "gte" and query_num is not None and field_num is not None:
                if field_num >= query_num:
                    matched = True

            elif operator == "lt" and query_num is not None and field_num is not None:
                if field_num < query_num:
                    matched = True

            elif operator == "lte" and query_num is not None and field_num is not None:
                if field_num <= query_num:
                    matched = True

            elif operator == "after" and query_dt is not None and field_dt is not None:
                if field_dt > query_dt:
                    matched = True

            elif operator == "before" and query_dt is not None and field_dt is not None:
                if field_dt < query_dt:
                    matched = True

            if matched:
                break

        if not matched:
            continue

        identifier = str(payload.get("identifier") or "").strip()
        primary_name = normalize_simple_text(payload.get("primary_name") or "")
        source_file = str(payload.get("source_file") or "").strip()

        if identifier:
            key = f"id:{identifier}"
        elif primary_name:
            key = f"name:{primary_name}|file:{source_file}"
        else:
            key = f"file:{source_file}"

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
            "payload": payload
        })

    for i, item in enumerate(results[:limit], start=1):
        item["rank"] = i

    return {
        "total_matches": len(results),
        "results": results[:limit]
    }

def extract_role_target_text(question, requested_role, field_maps):
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

def discover_structured_role_distinct_values(collection, requested_role, limit=200):
    payload_fields = resolve_payload_fields_for_role(collection, requested_role)

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    value_rows = []
    seen_values = set()

    for p in points:
        payload = p.payload or {}

        if infer_doc_type(payload) != "structured":
            continue

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

    return {
        "total_matches": len(value_rows),
        "results": value_rows[:limit]
    }

def parse_structured_filter_query(question, requested_role, field_maps):
    raw_q = str(question or "").strip()
    q_norm = normalize_simple_text(question)

    operator = "contains"
    value = ""

    patterns = [
        ("gte", r"\b(?:greater than or equal to|at least|>=)\b\s*(.+)$"),
        ("lte", r"\b(?:less than or equal to|at most|<=)\b\s*(.+)$"),
        ("gt", r"\b(?:greater than|more than|over|>)\b\s*(.+)$"),
        ("lt", r"\b(?:less than|under|below|<)\b\s*(.+)$"),
        ("after", r"\bafter\b\s*(.+)$"),
        ("before", r"\bbefore\b\s*(.+)$"),
        ("eq", r"\bon\b\s*(.+)$"),
    ]

    for op, pattern in patterns:
        m = re.search(pattern, raw_q, flags=re.IGNORECASE)
        if m:
            operator = op
            value = m.group(1).strip()
            break

    if not value:
        value = extract_role_target_text(question, requested_role, field_maps)

        if requested_role == "exposure":
            value = re.sub(r"\b(images|image|files|file|frames|frame)\b", " ", value, flags=re.IGNORECASE)
            value = " ".join(value.split())

    return {
        "role": requested_role,
        "operator": operator,
        "value": value,
    }

def run_discovery_with_method(collection, question, limit=200):
    intent = detect_ask_intent(question)

    field_maps = load_field_maps()
    q_norm = normalize_simple_text(question)

    requested_role = None
    for role, keywords in field_maps.items():
        for kw in keywords:
            kw_norm = normalize_simple_text(kw)
            if kw_norm and kw_norm in q_norm:
                requested_role = role
                break
        if requested_role:
            break

    hints = load_doc_query_hints()
    distinct_value_query = (
        "what" in q_norm.split()
        and _matches_any_term(q_norm, hints.get("distinct_value_query_terms", []))
    )

    if requested_role:
        if intent["mode"] == "discovery_list" and distinct_value_query:
            distinct_discovery = discover_structured_role_distinct_values(
                collection=collection,
                requested_role=requested_role,
                limit=limit
            )
            if distinct_discovery is not None:
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured distinct values",
                    "result": distinct_discovery
                }

        target_text = extract_role_target_text(question, requested_role, field_maps)

        if target_text:
            structured_discovery = discover_structured_role_matches(
                collection=collection,
                question=question,
                requested_role=requested_role,
                target_text=target_text,
                limit=limit
            )
            if structured_discovery is not None:
                return {
                    "method": intent["mode"],
                    "reason": f"{intent['reason']} using structured field match",
                    "result": structured_discovery
                }

        distinct_discovery = discover_structured_role_distinct_values(
            collection=collection,
            requested_role=requested_role,
            limit=limit
        )
        if distinct_discovery is not None:
            return {
                "method": intent["mode"],
                "reason": f"{intent['reason']} using structured distinct values",
                "result": distinct_discovery
            }

    discovery = discover_collection_items(collection, question, limit=limit)

    return {
        "method": intent["mode"],
        "reason": intent["reason"],
        "result": discovery
    }

