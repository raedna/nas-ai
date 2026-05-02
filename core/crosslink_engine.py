from __future__ import annotations

import json

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from core.system_config import load_system_config

cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])

import re

def extract_comparison_identifiers(question: str):
    q = question.lower()

    tag_matches = re.findall(r"\btag\s*(\d+)\b", q)
    if len(tag_matches) >= 2:
        return [tag_matches[0], tag_matches[1]]

    plain_numbers = re.findall(r"\b(\d{1,5})\b", q)
    unique = []
    for n in plain_numbers:
        if n not in unique:
            unique.append(n)
    if len(unique) >= 2 and "tag" in q:
        return [unique[0], unique[1]]

    return []

def pick_canonical_identifier_payload(points):
    best_payload = None
    best_score = -1

    for p in points or []:
        payload = p.payload or {}

        score = 0

        if payload.get("identifier") not in [None, ""]:
            score += 5
        if payload.get("primary_name") not in [None, ""]:
            score += 5
        if payload.get("description") not in [None, ""]:
            score += 3

        source_file = str(payload.get("source_file") or "").lower()
        primary_name = str(payload.get("primary_name") or "").strip()

        # prefer field-definition style files over message/event style rows
        if "fields_" in source_file or source_file.startswith("fields"):
            score += 10

        # prefer rows that look like real field names over sentence-like names
        if primary_name and " " not in primary_name and len(primary_name) <= 40:
            score += 3

        if score > best_score:
            best_score = score
            best_payload = payload

    return best_payload or {}

def reverse_lookup_by_enum_value(collection, search_text, limit=10):
    from core.query_router import infer_doc_type, normalize_simple_text

    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

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

        enum_values = payload.get("enum_values") or []
        if not enum_values:
            continue

        matched_enum = None

        for e in enum_values:
            if not isinstance(e, dict):
                continue

            enum_value = normalize_simple_text(e.get("enum_value"))
            enum_name = normalize_simple_text(e.get("enum_name"))
            enum_description = normalize_simple_text(e.get("description"))

            if q_norm in {enum_value, enum_name, enum_description}:
                matched_enum = e
                break

        if not matched_enum:
            continue

        link_keys = payload.get("link_keys") or []
        identifier = str(payload.get("identifier") or "").strip()
        namespace = str(payload.get("identifier_namespace") or "").strip()
        primary_name = normalize_simple_text(payload.get("primary_name") or "")

        if link_keys:
            key = "|".join(sorted(str(k) for k in link_keys))
        elif namespace and identifier:
            key = f"{namespace}:{identifier}"
        elif identifier:
            key = f"id:{identifier}"
        elif primary_name:
            key = f"name:{primary_name}"
        else:
            key = str(id(payload))

        if key in seen:
            continue

        seen.add(key)

        results.append({
            "identifier": payload.get("identifier"),
            "identifier_field": payload.get("identifier_field"),
            "identifier_namespace": payload.get("identifier_namespace"),
            "primary_name": payload.get("primary_name"),
            "description": payload.get("description"),
            "matched_enum": matched_enum,
            "score": 100.0,
            "payload": payload
        })

        if len(results) >= limit:
            break

    return results

def fetch_structured_points_by_primary_name(collection, search_text, limit=10):
    from core.query_router import infer_doc_type, normalize_simple_text

    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    matches = []

    for p in points:
        payload = p.payload or {}

        if infer_doc_type(payload) != "structured":
            continue

        primary_name = normalize_simple_text(payload.get("primary_name"))
        aliases = payload.get("aliases") or []
        alias_norms = [normalize_simple_text(a) for a in aliases]

        if q_norm == primary_name or q_norm in alias_norms:
            matches.append(p)

        if len(matches) >= limit:
            break

    return matches

def fetch_points_by_primary_name(collection, primary_name, limit=20):
    title_norm = primary_name.strip().lower()
    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        candidate = str(payload.get("primary_name") or "").strip().lower()
        if candidate == title_norm:
            matches.append(p)

    return matches[:limit]


def extract_comparison_primary_names(question: str):
    q = question.strip()

    patterns = [
        r"compare\s+(.+?)\s+and\s+(.+)",
        r"difference between\s+(.+?)\s+and\s+(.+)",
        r"compare\s+(.+?)\s+vs\s+(.+)",
        r"compare\s+(.+?)\s+versus\s+(.+)"
    ]

    for pattern in patterns:
        m = re.search(pattern, q, flags=re.IGNORECASE)
        if m:
            left = m.group(1).strip()
            right = m.group(2).strip()
            if left and right:
                return [left, right]

    return []


def compare_identifiers(collection, left_id, right_id):
    left_points = fetch_points_by_identifier(collection, left_id, limit=20)
    right_points = fetch_points_by_identifier(collection, right_id, limit=20)

    results = []
    if left_points:
        merged_left = merge_payloads_for_identifier(left_points, left_id)
        results.append({
            "identifier": merged_left.get("identifier"),
            "primary_name": merged_left.get("primary_name"),
            "description": merged_left.get("description"),
            "payload": merged_left
        })

    if right_points:
        merged_right = merge_payloads_for_identifier(right_points, right_id)
        results.append({
            "identifier": merged_right.get("identifier"),
            "primary_name": merged_right.get("primary_name"),
            "description": merged_right.get("description"),
            "payload": merged_right
        })

    return results


def compare_primary_names(collection, left_name, right_name):
    left_points = fetch_points_by_primary_name(collection, left_name, limit=20)
    right_points = fetch_points_by_primary_name(collection, right_name, limit=20)

    results = []

    if left_points:
        p = left_points[0].payload or {}
        results.append({
            "identifier": p.get("identifier"),
            "primary_name": p.get("primary_name"),
            "description": p.get("description"),
            "payload": p
        })

    if right_points:
        p = right_points[0].payload or {}
        results.append({
            "identifier": p.get("identifier"),
            "primary_name": p.get("primary_name"),
            "description": p.get("description"),
            "payload": p
        })

    return results


def run_comparison_query(collection, question):
    ids = extract_comparison_identifiers(question)
    if len(ids) == 2:
        results = compare_identifiers(collection, ids[0], ids[1])
        return {
            "method": "comparison",
            "reason": "two identifiers detected",
            "result": results
        }

    names = extract_comparison_primary_names(question)
    if len(names) == 2:
        results = compare_primary_names(collection, names[0], names[1])
        return {
            "method": "comparison",
            "reason": "two primary names detected",
            "result": results
        }

    return {
        "method": "comparison",
        "reason": "comparison query detected but no direct pair resolved",
        "result": []
    }

def fetch_points_by_identifier(collection, identifier, limit=20):
    results = client.query_points(
        collection_name=collection,
        query_filter=Filter(
            must=[
                FieldCondition(
                    key="identifier",
                    match=MatchValue(value=str(identifier))
                )
            ]
        ),
        limit=limit
    )
    return results.points

def fetch_points_by_identifier_namespace(collection, identifier, identifier_namespace, limit=20):
    points, _ = client.scroll(
        collection_name=collection,
        scroll_filter=Filter(
            must=[
                FieldCondition(
                    key="identifier",
                    match=MatchValue(value=str(identifier))
                ),
                FieldCondition(
                    key="identifier_namespace",
                    match=MatchValue(value=str(identifier_namespace))
                )
            ]
        ),
        limit=limit,
        with_payload=True,
        with_vectors=False
    )
    return points

def reverse_lookup_structured_by_requested_role(collection, search_text, requested_role, limit=10):
    from core.query_router import infer_doc_type, normalize_simple_text

    if requested_role != "primary_name":
        return []

    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

    points, _ = client.scroll(
        collection_name=collection,
        limit=5000,
        with_payload=True,
        with_vectors=False
    )

    scored = []
    seen = set()

    for p in points:
        payload = p.payload or {}

        if infer_doc_type(payload) != "structured":
            continue

        identifier = str(payload.get("identifier") or "").strip()
        primary_name = str(payload.get("primary_name") or "").strip()
        description = str(payload.get("description") or "").strip()
        aliases = payload.get("aliases") or []

        desc_norm = normalize_simple_text(description)
        alias_text = " ".join(str(a) for a in aliases)
        alias_norm = normalize_simple_text(alias_text)

        score = 0.0

        if q_norm and q_norm == desc_norm:
            score += 100.0
        elif q_norm and q_norm in desc_norm:
            score += 30.0

        words = [w for w in q_norm.split() if w]
        score += sum(5.0 for w in words if w in desc_norm)
        score += sum(2.0 for w in words if w in alias_norm)

        if score <= 0:
            continue

        key = identifier or normalize_simple_text(primary_name)
        if not key or key in seen:
            continue

        seen.add(key)

        full_points = fetch_points_by_identifier(collection, identifier, limit=20) if identifier else [p]
        merged_payload = merge_payloads_for_identifier(full_points, identifier) if identifier else payload

        scored.append((
            score,
            {
                "identifier": merged_payload.get("identifier"),
                "primary_name": merged_payload.get("primary_name"),
                "description": merged_payload.get("description"),
                "score": score,
                "payload": merged_payload
            }
        ))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:limit]]

def merge_payloads_for_identifier(points, identifier):
    canonical = pick_canonical_identifier_payload(points)

    merged_payload = {
        "identifier": str(identifier),
        "primary_name": canonical.get("primary_name"),
        "description": canonical.get("description"),
        "enum_values": [],
        "source_files": [],
        "related_identifiers": [],
        "source_type": canonical.get("source_type"),
        "doc_type": canonical.get("doc_type"),
        "subtype": canonical.get("subtype"),
        "source_file": canonical.get("source_file"),
    }

    seen_enums = set()
    seen_related = set()
    seen_sources = set()

    for p in points or []:
        payload = p.payload or {}

        for e in payload.get("enum_values", []) or []:
            key = str(e)
            if isinstance(e, dict):
                key = json.dumps(e, sort_keys=True)
            if key not in seen_enums:
                seen_enums.add(key)
                merged_payload["enum_values"].append(e)

        for rid in payload.get("related_identifiers", []) or []:
            rid_str = str(rid).strip()
            if rid_str and rid_str not in seen_related:
                seen_related.add(rid_str)
                merged_payload["related_identifiers"].append(rid_str)

        sf = payload.get("source_file")
        if sf and sf not in seen_sources:
            seen_sources.add(sf)
            merged_payload["source_files"].append(sf)

    return merged_payload

def expand_related_identifiers(collection, identifier, limit_per_identifier=10):
    base_points = fetch_points_by_identifier(collection, identifier, limit=20)
    if not base_points:
        return []

    base_payload = merge_payloads_for_identifier(base_points, identifier)
    related_ids = base_payload.get("related_identifiers", []) or []

    related_results = []
    for rid in related_ids:
        related_points = fetch_points_by_identifier(collection, rid, limit=limit_per_identifier)
        if not related_points:
            continue

        merged_related = merge_payloads_for_identifier(related_points, rid)
        related_results.append({
            "identifier": merged_related.get("identifier"),
            "primary_name": merged_related.get("primary_name"),
            "description": merged_related.get("description"),
            "score": 100.0,
            "payload": merged_related
        })

    return related_results