"""
core/retrieval/lexical.py
==========================
BM25 and lexical search retrieval.
Handles:
  - Short keyword queries (lexical_short)
  - Structured record lexical search
  - Document chunk lexical search
  - Entity row lexical search

Key improvement over old code:
  Old: client.scroll(limit=5000) then score 5000 records in Python
  New: PostgreSQL BM25 (tsvector) pre-filters to relevant records,
       Python scoring runs on a much smaller candidate set.

All database access goes through db_retrieval.py.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from core.query_helpers import (
    infer_doc_type,
    normalize_simple_text,
    expand_terms_with_synonyms,
    load_doc_query_hints,
)
from core.retrieval.db_retrieval import (
    search_bm25,
    scroll_collection,
    Point,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def contains_token_or_phrase(text_norm: str, query_norm: str) -> bool:
    """Check if query appears as a word/phrase in text (word-boundary aware)."""
    text_norm = normalize_simple_text(text_norm)
    query_norm = normalize_simple_text(query_norm)

    if not text_norm or not query_norm:
        return False

    return re.search(rf"\b{re.escape(query_norm)}\b", text_norm) is not None


# ---------------------------------------------------------------------------
# Lexical short query search
# Best for: short field names, abbreviations, 1-2 word lookups
# Old code: scroll(limit=5000) + Python scoring
# New code: BM25 pre-filter + Python scoring on smaller set
# ---------------------------------------------------------------------------
def lexical_short_query_search(
    collection_name: str,
    question: str,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    """
    BM25 search optimised for short keyword queries against structured records.
    Delegates entirely to PostgreSQL BM25 -- no Python re-scoring.
    """
    q_norm = normalize_simple_text(question)
    if not q_norm:
        return []

    # Strip namespace/question noise so BM25 searches content terms only
    hints = load_doc_query_hints()
    noise = set(hints.get("discovery_noise_words", []))
    noise.update(hints.get("question_words", []))
    noise.update(hints.get("structured_namespace_terms", []))
    noise.update(hints.get("stopwords", []))

    content_words = [w for w in q_norm.split() if w and w not in noise]

    # Run multiple BM25 queries — one per synonym variant — and merge results
    seen_ids: set = set()
    candidates = []

    queries_to_try: set = set()
    queries_to_try.add(" ".join(content_words))

    for i, word in enumerate(content_words):
        synonyms = expand_terms_with_synonyms([word])
        for syn in synonyms:
            if syn != word:
                variant = content_words[:i] + [syn] + content_words[i+1:]
                queries_to_try.add(" ".join(variant))

    for bm25_q in queries_to_try:
        results = search_bm25(
            collection_name=collection_name,
            query=bm25_q,
            doc_type="structured",
            limit=limit,
        )
        for p in results:
            pid = getattr(p, "id", None) or id(p)
            if pid not in seen_ids:
                seen_ids.add(pid)
                candidates.append(p)

    candidates.sort(key=lambda p: getattr(p, "score", 0.0), reverse=True)

    return [
        {
            "identifier": p.payload.get("identifier"),
            "primary_name": p.payload.get("primary_name"),
            "description": p.payload.get("description"),
            "score": getattr(p, "score", 0.0),
            "payload": p.payload,
        }
        for p in candidates[:limit]
    ]


# ---------------------------------------------------------------------------
# Lexical structured search
# Best for: structured records (FIX tags, BBG fields)
# Old code: scroll(limit=5000) + Python scoring on structured only
# New code: BM25 pre-filter with doc_type=structured + Python scoring
# ---------------------------------------------------------------------------
def lexical_structured_search(
    collection_name: str,
    question: str,
    limit: int = 25,
) -> List[Point]:
    """
    Lexical search over structured records.
    Returns Points sorted by relevance score.
    """
    q_norm = normalize_simple_text(question)

    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    raw_words = [w for w in q_norm.split() if w]
    meaningful_words = [
        w for w in raw_words
        if w not in stopwords and w not in {"tag", "field"}
    ]
    expanded_words = expand_terms_with_synonyms(meaningful_words)

    if not expanded_words:
        return []

    # BM25 pre-filter on structured records
    search_query = " ".join(meaningful_words) if meaningful_words else q_norm
    candidates = search_bm25(
        collection_name=collection_name,
        query=search_query,
        doc_type="structured",
        limit=200,
    )

    scored = []

    for p in candidates:
        payload = p.payload or {}

        identifier = payload.get("identifier")
        primary_name = str(payload.get("primary_name") or "")
        description = str(payload.get("description") or "")

        if identifier in [None, ""] and not primary_name:
            continue

        name_norm = normalize_simple_text(primary_name.replace("_", " "))
        desc_norm = normalize_simple_text(description)
        combined = f"{name_norm} {desc_norm}"

        score = 0.0

        if q_norm and q_norm in name_norm:
            score += 8.0
        if q_norm and q_norm in desc_norm:
            score += 2.5

        exact_name_hits = sum(1 for w in meaningful_words if w in name_norm)
        exact_desc_hits = sum(1 for w in meaningful_words if w in desc_norm)

        score += exact_name_hits * 3.0
        score += exact_desc_hits * 0.8

        if meaningful_words and all(w in name_norm for w in meaningful_words):
            score += 8.0
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 3.0

        expanded_name_hits = sum(1 for w in expanded_words if w in name_norm)
        expanded_desc_hits = sum(1 for w in expanded_words if w in desc_norm)

        score += expanded_name_hits * 1.2
        score += expanded_desc_hits * 0.3

        if primary_name and len(primary_name.strip()) <= 30:
            score += 0.5

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]


# ---------------------------------------------------------------------------
# Lexical chunk search
# Best for: document/procedural content (KB articles, notes)
# Old code: scroll(limit=500) + Python scoring on doc-like chunks
# New code: BM25 pre-filter + Python scoring
# ---------------------------------------------------------------------------
def lexical_chunk_search(
    collection_name: str,
    question: str,
    limit: int = 25,
) -> List[Point]:
    """
    Lexical search over document chunks (procedural/KB content).
    Returns Points sorted by relevance score.
    """
    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    q_norm = normalize_simple_text(question)
    query_terms = [
        w for w in q_norm.split()
        if len(w) > 2 and w not in stopwords
    ]

    if not query_terms:
        return []

    candidates = search_bm25(
        collection_name=collection_name,
        query=" ".join(query_terms),
        limit=100,
    )

    scored = []

    for p in candidates:
        payload = p.payload or {}

        source_type = str(payload.get("source_type") or "").lower()
        file_type = str(payload.get("file_type") or "").lower()
        doc_type = str(payload.get("doc_type") or "").lower()

        is_chunked_doc_like = (
            bool(payload.get("section_heading") or payload.get("block_types"))
            and doc_type != "entity_row"
            and file_type != "image"
            and source_type not in ["image", "standalone_image"]
        )

        if not is_chunked_doc_like:
            continue

        text = normalize_simple_text(payload.get("text") or payload.get("nlp_text", ""))
        heading = normalize_simple_text(payload.get("section_heading", ""))

        score = 0.0

        if q_norm and q_norm in text:
            score += 8.0
        if q_norm and q_norm in heading:
            score += 6.0

        unique_hits = len({w for w in query_terms if w in text})
        heading_hits = len({w for w in query_terms if w in heading})

        score += unique_hits * 1.5
        score += heading_hits * 2.0

        if query_terms and all(w in text for w in query_terms):
            score += 4.0

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]


# ---------------------------------------------------------------------------
# Lexical entity row search
# Best for: entity rows (table records, KB CSV entries)
# ---------------------------------------------------------------------------
def lexical_entity_row_search(
    collection_name: str,
    question: str,
    limit: int = 25,
) -> List[Point]:
    """
    Lexical search over entity_row records.
    Returns Points sorted by relevance score.
    """
    q_norm = normalize_simple_text(question)

    hints = load_doc_query_hints()
    stopwords = set(hints.get("stopwords", []))

    raw_words = [w for w in q_norm.split() if w]
    meaningful_words = [w for w in raw_words if w not in stopwords]
    expanded_words = expand_terms_with_synonyms(meaningful_words)

    if not meaningful_words and not expanded_words:
        return []

    search_query = " ".join(meaningful_words) if meaningful_words else q_norm
    candidates = search_bm25(
        collection_name=collection_name,
        query=search_query,
        doc_type="entity_row",
        limit=200,
    )

    scored = []

    for p in candidates:
        payload = p.payload or {}

        name = str(payload.get("primary_name") or "")
        desc = str(payload.get("description") or "")

        if not name and not desc:
            continue

        name_norm = normalize_simple_text(name)
        desc_norm = normalize_simple_text(desc)
        combined = f"{name_norm} {desc_norm}"

        score = 0.0
        exact_query = " ".join(meaningful_words).strip()

        if exact_query and name_norm == exact_query:
            score += 20.0
        elif exact_query and exact_query in name_norm:
            score += 8.0
        elif exact_query and exact_query in desc_norm:
            score += 2.0

        exact_name_hits = sum(1 for w in meaningful_words if w in name_norm)
        exact_desc_hits = sum(1 for w in meaningful_words if w in desc_norm)

        score += exact_name_hits * 3.0
        score += exact_desc_hits * 0.8

        if meaningful_words and all(w in name_norm for w in meaningful_words):
            score += 8.0
        elif meaningful_words and all(w in combined for w in meaningful_words):
            score += 3.0

        expanded_name_hits = sum(1 for w in expanded_words if w in name_norm)
        expanded_desc_hits = sum(1 for w in expanded_words if w in desc_norm)

        score += expanded_name_hits * 1.0
        score += expanded_desc_hits * 0.2

        if score > 0:
            scored.append((score, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [p for _, p in scored[:limit]]
