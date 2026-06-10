"""
core/retrieval/crosslink.py
============================
Cross-linking and relationship retrieval.
Handles:
  - Fetch by identifier / identifier+namespace
  - Fetch by link_key / related_link_key
  - Fetch by primary_name
  - Reverse enum lookup
  - Structured role lookup
  - Comparison queries (compare tag X and tag Y)
  - Payload merging across multiple points

Replaces core/crosslink_engine.py.
All database access goes through db_retrieval.py.
No Qdrant imports.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
)
from core.retrieval.db_retrieval import (
    get_by_identifier,
    get_by_identifier_namespace,
    get_by_primary_name,
    get_by_primary_name_contains,
    get_by_link_key,
    get_by_related_link_key,
    search_enum_values,
    scroll_collection,
    get_by_source_file,
    Point,
)


# ---------------------------------------------------------------------------
# Identifier extraction helpers
# ---------------------------------------------------------------------------
def extract_comparison_identifiers(question: str) -> List[str]:
    """Extract two identifiers from a comparison question."""
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


def extract_comparison_primary_names(question: str) -> List[str]:
    """Extract two primary names from a comparison question."""
    q = question.strip()
    patterns = [
        r"compare\s+(.+?)\s+and\s+(.+)",
        r"difference between\s+(.+?)\s+and\s+(.+)",
        r"compare\s+(.+?)\s+vs\s+(.+)",
        r"compare\s+(.+?)\s+versus\s+(.+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, q, flags=re.IGNORECASE)
        if m:
            left = m.group(1).strip()
            right = m.group(2).strip()
            if left and right:
                return [left, right]
    return []


# ---------------------------------------------------------------------------
# Canonical payload picker
# ---------------------------------------------------------------------------
def pick_canonical_identifier_payload(points: List[Point]) -> Dict[str, Any]:
    """
    From a list of points for the same identifier, pick the most canonical one.
    Prefers Fields files over Messages/Components, prefers short clean names.
    """
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

        if "fields_" in source_file or source_file.startswith("fields"):
            score += 10

        if primary_name and " " not in primary_name and len(primary_name) <= 40:
            score += 3

        if score > best_score:
            best_score = score
            best_payload = payload

    return best_payload or {}


# ---------------------------------------------------------------------------
# Fetch functions
# All replace client.scroll() + manual filtering
# ---------------------------------------------------------------------------
def fetch_points_by_identifier(
    collection_name: str,
    identifier: str,
    limit: int = 20,
) -> List[Point]:
    """Fetch all chunks with a given identifier value."""
    return get_by_identifier(
        collection_name=collection_name,
        identifier=str(identifier),
        limit=limit,
    )


def fetch_points_by_identifier_namespace(
    collection_name: str,
    identifier: str,
    identifier_namespace: str,
    limit: int = 20,
) -> List[Point]:
    """Fetch chunks by identifier + namespace."""
    return get_by_identifier_namespace(
        collection_name=collection_name,
        identifier=str(identifier),
        identifier_namespace=str(identifier_namespace),
        limit=limit,
    )


def fetch_points_by_link_key(
    collection_name: str,
    link_key: str,
    limit: int = 20,
) -> List[Point]:
    """Fetch chunks whose link_keys contains the given key."""
    return get_by_link_key(
        collection_name=collection_name,
        link_key=link_key,
        limit=limit,
    )


def fetch_points_related_to_link_key(
    collection_name: str,
    link_key: str,
    limit: int = 50,
) -> List[Point]:
    """Fetch chunks whose related_link_keys contains the given key."""
    return get_by_related_link_key(
        collection_name=collection_name,
        link_key=link_key,
        limit=limit,
    )


def fetch_points_by_primary_name(
    collection_name: str,
    primary_name: str,
    limit: int = 20,
) -> List[Point]:
    """Fetch chunks by exact primary_name match."""
    return get_by_primary_name(
        collection_name=collection_name,
        primary_name=primary_name,
        limit=limit,
    )


def fetch_structured_points_by_primary_name(
    collection_name: str,
    search_text: str,
    limit: int = 10,
) -> List[Point]:
    """Find structured records whose primary_name or aliases match search_text."""
    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="structured",
        limit=5000,
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        primary_name = normalize_simple_text(payload.get("primary_name"))
        aliases = payload.get("aliases") or []
        alias_norms = [normalize_simple_text(a) for a in aliases]

        if q_norm == primary_name or q_norm in alias_norms:
            matches.append(p)
            if len(matches) >= limit:
                break

    return matches


def fetch_structured_points_by_name_in_question(
    collection_name: str,
    question: str,
    limit: int = 10,
) -> List[Point]:
    """Find structured records whose primary_name appears in the question."""
    q_norm = normalize_simple_text(question)
    tokens = [t for t in q_norm.split() if t]
    if not tokens:
        return []

    spans = []
    for start in range(len(tokens)):
        for end in range(start + 1, len(tokens) + 1):
            spans.append(" ".join(tokens[start:end]))
    spans = sorted(set(spans), key=len, reverse=True)

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="structured",
        limit=5000,
    )

    matches = []
    for p in points:
        payload = p.payload or {}
        names = []

        primary_name = normalize_simple_text(payload.get("primary_name"))
        if primary_name:
            names.append(primary_name)

        for a in payload.get("aliases") or []:
            alias_norm = normalize_simple_text(a)
            if alias_norm:
                names.append(alias_norm)

        if any(name in spans for name in names):
            matches.append(p)
            if len(matches) >= limit:
                break

    return matches


# ---------------------------------------------------------------------------
# Reverse enum lookup
# Replaces: scroll(limit=5000) + manual enum_values JSONB scan
# Now uses normalized enum_values table
# ---------------------------------------------------------------------------
def reverse_lookup_by_enum_value(
    collection_name: str,
    search_text: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Find structured records that have a given enum value or name.
    Returns list of dicts with matched_enum info.
    """
    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

    points = search_enum_values(
        collection_name=collection_name,
        search_text=q_norm,
        limit=limit,
    )

    results = []
    seen = set()

    for p in points:
        payload = p.payload or {}
        matched_enum = payload.get("_matched_enum") or {}

        identifier = str(payload.get("identifier") or "").strip()
        namespace = str(payload.get("identifier_namespace") or "").strip()
        primary_name = normalize_simple_text(payload.get("primary_name") or "")

        link_keys = payload.get("link_keys") or []
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
            "payload": payload,
        })

    return results[:limit]


# ---------------------------------------------------------------------------
# Structured role lookup
# Replaces: scroll(limit=5000) + manual role scoring
# ---------------------------------------------------------------------------
def reverse_lookup_structured_by_requested_role(
    collection_name: str,
    search_text: str,
    requested_role: str,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Find structured records where a role field matches search_text."""
    if requested_role != "primary_name":
        return []

    q_norm = normalize_simple_text(search_text)
    if not q_norm:
        return []

    points = scroll_collection(
        collection_name=collection_name,
        doc_type="structured",
        limit=5000,
    )

    scored = []
    seen = set()

    for p in points:
        payload = p.payload or {}

        identifier = str(payload.get("identifier") or "").strip()
        primary_name = str(payload.get("primary_name") or "").strip()
        description = str(payload.get("description") or "").strip()
        aliases = payload.get("aliases") or []

        desc_norm = normalize_simple_text(description)
        alias_norm = normalize_simple_text(" ".join(str(a) for a in aliases))

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

        full_points = fetch_points_by_identifier(collection_name, identifier, limit=20) if identifier else [p]
        merged = merge_payloads_for_identifier(full_points, identifier) if identifier else payload

        scored.append((score, {
            "identifier": merged.get("identifier"),
            "primary_name": merged.get("primary_name"),
            "description": merged.get("description"),
            "score": score,
            "payload": merged,
        }))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:limit]]


# ---------------------------------------------------------------------------
# Payload merging
# ---------------------------------------------------------------------------
def merge_payloads_for_identifier(
    points: List[Point],
    identifier: str,
) -> Dict[str, Any]:
    """
    Merge multiple points for the same identifier into one canonical payload.
    Combines enum_values, source_files, related_identifiers from all points.
    """
    canonical = pick_canonical_identifier_payload(points)

    merged = {
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

        for e in payload.get("enum_values") or []:
            key = json.dumps(e, sort_keys=True) if isinstance(e, dict) else str(e)
            if key not in seen_enums:
                seen_enums.add(key)
                merged["enum_values"].append(e)

        for rid in payload.get("related_identifiers") or []:
            rid_str = str(rid).strip()
            if rid_str and rid_str not in seen_related:
                seen_related.add(rid_str)
                merged["related_identifiers"].append(rid_str)

        sf = payload.get("source_file")
        if sf and sf not in seen_sources:
            seen_sources.add(sf)
            merged["source_files"].append(sf)

    return merged


def expand_related_identifiers(
    collection_name: str,
    identifier: str,
    limit_per_identifier: int = 10,
) -> List[Dict[str, Any]]:
    """Fetch all records related to an identifier via related_identifiers."""
    base_points = fetch_points_by_identifier(collection_name, identifier, limit=20)
    if not base_points:
        return []

    base_payload = merge_payloads_for_identifier(base_points, identifier)
    related_ids = base_payload.get("related_identifiers") or []

    results = []
    for rid in related_ids:
        related_points = fetch_points_by_identifier(
            collection_name, rid, limit=limit_per_identifier
        )
        if not related_points:
            continue
        merged = merge_payloads_for_identifier(related_points, rid)
        results.append({
            "identifier": merged.get("identifier"),
            "primary_name": merged.get("primary_name"),
            "description": merged.get("description"),
            "score": 100.0,
            "payload": merged,
        })

    return results


# ---------------------------------------------------------------------------
# Comparison queries
# ---------------------------------------------------------------------------
def compare_identifiers(
    collection_name: str,
    left_id: str,
    right_id: str,
) -> List[Dict[str, Any]]:
    """Compare two records by identifier."""
    results = []

    for id_val in [left_id, right_id]:
        points = fetch_points_by_identifier(collection_name, id_val, limit=20)
        if points:
            merged = merge_payloads_for_identifier(points, id_val)
            results.append({
                "identifier": merged.get("identifier"),
                "primary_name": merged.get("primary_name"),
                "description": merged.get("description"),
                "payload": merged,
            })

    return results


def compare_primary_names(
    collection_name: str,
    left_name: str,
    right_name: str,
) -> List[Dict[str, Any]]:
    """Compare two records by primary_name."""
    results = []

    for name in [left_name, right_name]:
        points = fetch_points_by_primary_name(collection_name, name, limit=20)
        if points:
            p = points[0].payload or {}
            results.append({
                "identifier": p.get("identifier"),
                "primary_name": p.get("primary_name"),
                "description": p.get("description"),
                "payload": p,
            })

    return results


def run_comparison_query(
    collection_name: str,
    question: str,
) -> Dict[str, Any]:
    """Run a comparison query — detects identifiers or names and compares them."""
    ids = extract_comparison_identifiers(question)
    if len(ids) == 2:
        return {
            "method": "comparison",
            "reason": "two identifiers detected",
            "result": compare_identifiers(collection_name, ids[0], ids[1]),
        }

    names = extract_comparison_primary_names(question)
    if len(names) == 2:
        return {
            "method": "comparison",
            "reason": "two primary names detected",
            "result": compare_primary_names(collection_name, names[0], names[1]),
        }

    return {
        "method": "comparison",
        "reason": "comparison query detected but no direct pair resolved",
        "result": [],
    }


# ---------------------------------------------------------------------------
# Document chunk fetching and payload enrichment
# Ported from query_router.py — fetch_doc_chunks_by_source_file and
# build_fuller_doc_payload.
# ---------------------------------------------------------------------------

def fetch_doc_chunks_by_source_file(
    collection_name: str,
    source_file: str,
    limit: int = 50,
) -> List[Point]:
    """Return all chunks for a given source_file, ordered by chunk_id."""
    if not source_file:
        return []
    return get_by_source_file(collection_name, source_file, limit=limit)


def build_fuller_doc_payload(
    collection_name: str,
    best_payload: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Enrich a chunked-document payload by merging nearby / same-section chunks.

    For markdown notes and procedural/reference/mixed doc_types the full
    document is merged.  For all others only chunks within the same section
    heading (or within ±1 chunk_id) are merged.

    Returns a new payload dict with a combined 'description' field.
    Falls back to best_payload unchanged if no chunks are found.
    """
    from core.query_helpers import normalize_simple_text

    source_file = best_payload.get("source_file")
    if not source_file:
        return best_payload

    points = fetch_doc_chunks_by_source_file(collection_name, source_file, limit=50)
    if not points:
        return best_payload

    def _chunk_sort_key(p: Point) -> int:
        try:
            return int((p.payload or {}).get("chunk_id", 999999))
        except Exception:
            return 999999

    ordered = sorted(points, key=_chunk_sort_key)

    best_chunk_id = best_payload.get("chunk_id")
    try:
        best_chunk_id = int(best_chunk_id)
    except Exception:
        best_chunk_id = None

    best_heading = str(best_payload.get("section_heading") or "").strip()
    doc_type = str(best_payload.get("doc_type") or "").lower()
    source_file_lower = str(source_file).lower()

    include_full_note = (
        source_file_lower.endswith(".md")
        or doc_type in {"procedural", "reference", "mixed"}
    )

    selected: List[Dict] = []

    if include_full_note:
        selected = [p.payload or {} for p in ordered]
    else:
        for p in ordered:
            payload = p.payload or {}
            cid = payload.get("chunk_id")
            try:
                cid = int(cid)
            except Exception:
                cid = None

            heading = str(payload.get("section_heading") or "").strip()

            if best_chunk_id is None:
                if best_heading:
                    if heading == best_heading or not heading:
                        selected.append(payload)
                else:
                    selected.append(payload)
            else:
                if cid is None:
                    continue
                if best_heading:
                    if heading == best_heading:
                        selected.append(payload)
                    elif abs(cid - best_chunk_id) <= 1 and not heading:
                        selected.append(payload)
                else:
                    if abs(cid - best_chunk_id) <= 1:
                        selected.append(payload)

    # Deduplicate selected chunks
    deduped: List[Dict] = []
    seen_chunk_keys: set = set()
    for payload in selected:
        text = str(payload.get("description") or payload.get("text") or "").strip()
        chunk_key = (
            payload.get("source_file"),
            payload.get("chunk_id"),
            normalize_simple_text(text)[:500],
        )
        if chunk_key in seen_chunk_keys:
            continue
        seen_chunk_keys.add(chunk_key)
        deduped.append(payload)

    selected = deduped or [best_payload]

    # Merge selected chunks into a single payload
    max_chars = 12000
    current_chars = 0

    combined_parts: List[str] = []
    seen_headings: set = set()
    seen_text_parts: set = set()
    seen_related: set = set()
    merged_related_titles: List[str] = []
    seen_image_paths: set = set()
    merged_image_paths: List[str] = []
    seen_image_targets: set = set()
    merged_image_targets: List[str] = []

    note_title = str(best_payload.get("primary_name") or "").strip()

    for payload in selected:
        heading = str(payload.get("section_heading") or "").strip()
        text = str(payload.get("text") or payload.get("description") or "").strip()

        for image_path in payload.get("embedded_image_paths") or []:
            image_path = str(image_path).strip()
            if image_path and image_path not in seen_image_paths:
                seen_image_paths.add(image_path)
                merged_image_paths.append(image_path)

        for image_target in payload.get("embedded_image_targets") or []:
            image_target = str(image_target).strip()
            if image_target and image_target not in seen_image_targets:
                seen_image_targets.add(image_target)
                merged_image_targets.append(image_target)

        for title in payload.get("related_titles") or []:
            title = str(title).strip()
            if title and title not in seen_related:
                seen_related.add(title)
                merged_related_titles.append(title)

        if heading and heading not in seen_headings:
            seen_headings.add(heading)
            heading_norm = normalize_simple_text(heading)
            title_norm = normalize_simple_text(note_title)
            if heading_norm != title_norm and not text.startswith(heading):
                combined_parts.append(heading)

        if text:
            text_lines = [ln for ln in text.splitlines() if ln.strip()]
            if text_lines:
                first_line_norm = normalize_simple_text(text_lines[0])
                title_norm = normalize_simple_text(note_title)
                heading_norm = normalize_simple_text(heading)
                if first_line_norm == title_norm or (heading and first_line_norm == heading_norm):
                    text = "\n".join(text_lines[1:]).strip()

            if text:
                text_key = normalize_simple_text(text)
                if text_key and text_key in seen_text_parts:
                    continue
                if text_key:
                    seen_text_parts.add(text_key)

                remaining = max_chars - current_chars
                if remaining <= 0:
                    break

                if len(text) > remaining:
                    combined_parts.append(text[:remaining].rstrip() + "\n\n[Answer truncated due to length.]")
                    current_chars = max_chars
                    break

                combined_parts.append(text)
                current_chars += len(text)

    merged_payload = dict(best_payload)
    merged_payload["description"] = "\n\n".join(part for part in combined_parts if part).strip()
    merged_payload["related_titles"] = merged_related_titles

    if merged_image_paths:
        merged_payload["embedded_image_paths"] = merged_image_paths
    if merged_image_targets:
        merged_payload["embedded_image_targets"] = merged_image_targets

    return merged_payload