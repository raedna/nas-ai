"""
core/retrieval/db_retrieval.py
==============================
All database queries for the retrieval layer.
This is the ONLY file in the retrieval package that knows about PostgreSQL.

All other retrieval files (semantic, lexical, structured, etc.) call
functions from here. Swapping the database backend means changing only
this file.

Returns SimpleNamespace objects with .payload and .score attributes
to maintain compatibility with existing code that expects Qdrant-style
point objects.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

from core.db import fetchall, fetchone, fetchval

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Point wrapper
# Mimics Qdrant ScoredPoint so existing code using p.payload and p.score
# works without changes.
# ---------------------------------------------------------------------------
class Point:
    """
    Lightweight wrapper around a chunk row.
    Mimics Qdrant ScoredPoint interface:
        p.payload  -> dict of all payload fields
        p.score    -> float similarity/relevance score
        p.id       -> chunk id string
    """
    def __init__(self, row: Dict[str, Any], score: float = 0.0):
        # Build payload from row — mirrors Qdrant payload structure
        payload = {}

        # If row has a payload JSONB column, start from that
        if row.get("payload") and isinstance(row["payload"], dict):
            payload = dict(row["payload"])

        # Always overlay the structured columns — they are authoritative
        for key in (
            "identifier", "identifier_field", "identifier_namespace",
            "identifier_kind", "primary_name", "description",
            "source_file", "source_type", "doc_type",
            "collection_name",
        ):
            if row.get(key) is not None:
                payload[key] = row[key]

        # nlp_text -> text (Qdrant payload uses "text" key)
        if row.get("nlp_text") is not None:
            payload["text"] = row["nlp_text"]

        # bm25_score or similarity from SQL
        if row.get("bm25_score") is not None:
            payload["_bm25_score"] = float(row["bm25_score"])
        if row.get("similarity") is not None:
            payload["_similarity"] = float(row["similarity"])

        self.payload = payload
        self.score = score or row.get("bm25_score") or row.get("similarity") or 0.0
        self.id = row.get("id", "")

    def __repr__(self):
        name = self.payload.get("primary_name", "?")
        return f"Point(id={self.id!r}, name={name!r}, score={self.score:.3f})"


def _rows_to_points(rows: List[Dict], score_key: str = None) -> List[Point]:
    """Convert SQL rows to Point objects."""
    points = []
    for row in rows:
        score = 0.0
        if score_key and row.get(score_key) is not None:
            score = float(row[score_key])
        elif row.get("bm25_score") is not None:
            score = float(row["bm25_score"])
        elif row.get("similarity") is not None:
            score = float(row["similarity"])
        points.append(Point(row, score))
    return points


# ---------------------------------------------------------------------------
# Core scroll replacement
# Replaces: client.scroll(collection_name=X, limit=5000, with_payload=True)
# ---------------------------------------------------------------------------
def scroll_collection(
    collection_name: str,
    limit: int = 5000,
    doc_type: str = None,
    source_type: str = None,
    identifier_namespace: str = None,
) -> List[Point]:
    """
    Fetch all chunks for a collection.
    Replaces client.scroll(collection_name=X, limit=5000).

    Use sparingly -- prefer specific query functions below.
    Filters reduce the result set for better performance.
    """
    conditions = ["collection_name = %s"]
    params = [collection_name]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)

    if identifier_namespace:
        conditions.append("identifier_namespace = %s")
        params.append(identifier_namespace)

    params.append(limit)

    sql = f"""
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE {" AND ".join(conditions)}
        LIMIT %s
    """
    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows)


# ---------------------------------------------------------------------------
# Identifier lookup
# Replaces: scroll + filter by identifier
# ---------------------------------------------------------------------------
def get_by_identifier(
    collection_name: str,
    identifier: str,
    identifier_namespace: str = None,
    limit: int = 20,
) -> List[Point]:
    """
    Fetch chunks by identifier value.
    Replaces scroll + manual filter by identifier.
    """
    conditions = ["collection_name = %s", "identifier = %s"]
    params = [collection_name, str(identifier)]

    if identifier_namespace:
        conditions.append("identifier_namespace = %s")
        params.append(identifier_namespace)

    params.append(limit)

    sql = f"""
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE {" AND ".join(conditions)}
        LIMIT %s
    """
    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows)


def get_by_identifier_namespace(
    collection_name: str,
    identifier: str,
    identifier_namespace: str,
    limit: int = 20,
) -> List[Point]:
    """Fetch chunks by identifier + namespace. Direct deterministic lookup."""
    return get_by_identifier(
        collection_name, identifier, identifier_namespace, limit
    )


# ---------------------------------------------------------------------------
# Primary name lookup
# Replaces: scroll + manual filter by primary_name
# ---------------------------------------------------------------------------
def get_by_primary_name(
    collection_name: str,
    primary_name: str,
    doc_type: str = None,
    limit: int = 20,
) -> List[Point]:
    """Fetch chunks by exact primary_name match."""
    conditions = ["collection_name = %s", "LOWER(primary_name) = LOWER(%s)"]
    params = [collection_name, primary_name]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    params.append(limit)

    sql = f"""
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE {" AND ".join(conditions)}
        LIMIT %s
    """
    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows)


def get_by_primary_name_contains(
    collection_name: str,
    search_text: str,
    doc_type: str = None,
    limit: int = 10,
) -> List[Point]:
    """Fetch chunks where primary_name contains search_text (trigram search)."""
    conditions = [
        "collection_name = %s",
        "primary_name ILIKE %s"
    ]
    params = [collection_name, f"%{search_text}%"]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    params.append(limit)

    sql = f"""
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE {" AND ".join(conditions)}
        LIMIT %s
    """
    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows)


# ---------------------------------------------------------------------------
# Link key lookup
# Replaces: scroll + filter by link_keys contains value
# ---------------------------------------------------------------------------
def get_by_link_key(
    collection_name: str,
    link_key: str,
    limit: int = 20,
) -> List[Point]:
    """
    Fetch chunks whose link_keys array contains the given key.
    Replaces Qdrant scroll + manual link_key filter.
    Uses JSONB containment operator.
    """
    sql = """
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE collection_name = %s
          AND payload->'link_keys' @> %s::jsonb
        LIMIT %s
    """
    rows = fetchall(sql, (collection_name, json.dumps([link_key]), limit))
    return _rows_to_points(rows)


def get_by_related_link_key(
    collection_name: str,
    link_key: str,
    limit: int = 50,
) -> List[Point]:
    """
    Fetch chunks whose related_link_keys array contains the given key.
    """
    sql = """
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE collection_name = %s
          AND payload->'related_link_keys' @> %s::jsonb
        LIMIT %s
    """
    rows = fetchall(sql, (collection_name, json.dumps([link_key]), limit))
    return _rows_to_points(rows)


# ---------------------------------------------------------------------------
# Source file lookup
# Replaces: scroll + filter by source_file
# ---------------------------------------------------------------------------
def get_by_source_file(
    collection_name: str,
    source_file: str,
    limit: int = 50,
) -> List[Point]:
    """Fetch all chunks from a specific source file."""
    sql = """
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE collection_name = %s
          AND source_file = %s
        LIMIT %s
    """
    rows = fetchall(sql, (collection_name, source_file, limit))
    return _rows_to_points(rows)


# ---------------------------------------------------------------------------
# BM25 full-text search
# Replaces: scroll(limit=5000) + Python scoring
# ---------------------------------------------------------------------------
def search_bm25(
    collection_name: str,
    query: str,
    doc_type: str = None,
    source_type: str = None,
    limit: int = 25,
) -> List[Point]:
    """
    BM25 full-text search using PostgreSQL tsvector.
    Replaces client.scroll(limit=5000) + Python term scoring.
    Much faster -- uses GIN index.
    """
    conditions = [
        "collection_name = %s",
        "nlp_text_tsv @@ websearch_to_tsquery('english', %s)"
    ]

    params = [collection_name, query]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)

    params.append(limit)

    sql = f"""
        WITH q AS (SELECT websearch_to_tsquery('english', %s) AS tsq)
        SELECT c.id, c.collection_name, c.source_file, c.source_type, c.doc_type,
               c.identifier, c.identifier_field, c.identifier_namespace, c.identifier_kind,
               c.primary_name, c.description, c.nlp_text, c.payload,
               ts_rank(c.nlp_text_tsv, q.tsq) AS bm25_score
        FROM chunks c, q
        WHERE {" AND ".join(conditions)}
        ORDER BY bm25_score DESC
        LIMIT %s
    """
    # CTE query param must be first -- before WHERE params
    rows = fetchall(sql, tuple([query] + params))
    return _rows_to_points(rows, score_key="bm25_score")


# ---------------------------------------------------------------------------
# Vector similarity search
# Replaces: client.query_points(query=vector, ...)
# ---------------------------------------------------------------------------
def search_vector(
    collection_name: str,
    embedding: List[float],
    doc_type: str = None,
    identifier: str = None,
    limit: int = 10,
) -> List[Point]:
    """
    Vector similarity search using pgvector cosine distance.
    Replaces client.query_points(collection_name=X, query=vector).
    """
    conditions = [
        "collection_name = %s",
        "embedding IS NOT NULL"
    ]
    params = [collection_name]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if identifier:
        conditions.append("identifier = %s")
        params.append(str(identifier))

    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    params.append(limit)
    sql = f"""
        WITH v AS (SELECT %s::vector AS vec)
        SELECT c.id, c.collection_name, c.source_file, c.source_type, c.doc_type,
               c.identifier, c.identifier_field, c.identifier_namespace, c.identifier_kind,
               c.primary_name, c.description, c.nlp_text, c.payload,
               1 - (c.embedding <=> v.vec) AS similarity
        FROM chunks c, v
        WHERE {" AND ".join(conditions)}
        ORDER BY c.embedding <=> v.vec
        LIMIT %s
    """
    rows = fetchall(sql, tuple([vector_str] + params))
    return _rows_to_points(rows, score_key="similarity")

# ---------------------------------------------------------------------------
# Hybrid RRF search
# Combines BM25 + vector search using Reciprocal Rank Fusion
# Eliminates score scale disparity between BM25 and semantic
# ---------------------------------------------------------------------------
def search_rrf(
    collection_name: str,
    bm25_queries: List[str],
    embedding: List[float],
    doc_type: str = None,
    limit: int = 25,
    k_bm25: int = 60,
    k_vector: int = 60,
    k_trgm: int = 10,
    identifier_namespace: str = None
) -> List[Point]:
    """
    Hybrid search combining BM25 + pgvector + trigram using Reciprocal Rank Fusion.
    Three signals fused by rank position — eliminates score scale disparity.
    k_trgm is lower (10) to give name similarity more weight.
    """
    tsquery_expr = " || ".join(
        "websearch_to_tsquery('english', %s)"
        for _ in bm25_queries
    )

    # Build GREATEST(...) expression for trigram — picks best similarity across all variants
    trgm_expr = "GREATEST(" + ", ".join("similarity(primary_name, %s)" for _ in bm25_queries) + ")"

    # Build OR condition for trigram match across all variants
    trgm_match = " OR ".join("primary_name %% %s" for _ in bm25_queries)

    doc_type_filter = "AND doc_type = %s" if doc_type else ""
    namespace_filter_clause = "AND identifier_namespace = %s" if identifier_namespace else ""
    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    rrf_limit = limit * 2

    sql = f"""
        WITH
        bm25_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       ORDER BY ts_rank_cd(nlp_text_tsv, {tsquery_expr}) DESC
                   ) as rank
            FROM chunks
            WHERE collection_name = %s
            {doc_type_filter}
            {namespace_filter_clause}
            AND nlp_text_tsv @@ ({tsquery_expr})
            LIMIT %s
        ),
        vector_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       ORDER BY embedding <=> %s::vector
                   ) as rank
            FROM chunks
            WHERE collection_name = %s
            {doc_type_filter}
            {namespace_filter_clause}
            AND embedding IS NOT NULL
            LIMIT %s
        ),
        trigram_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       ORDER BY {trgm_expr} DESC
                   ) as rank
            FROM chunks
            WHERE collection_name = %s
            {doc_type_filter}
            {namespace_filter_clause}
            AND ({trgm_match})
            LIMIT %s
        ),
        fused AS (
            SELECT
                COALESCE(b.id, v.id, t.id) as id,
                COALESCE(1.0 / ({k_bm25} + b.rank), 0.0) +
                COALESCE(1.0 / ({k_vector} + v.rank), 0.0) +
                COALESCE(1.0 / ({k_trgm} + t.rank), 0.0) as rrf_score
            FROM bm25_search b
            FULL OUTER JOIN vector_search v ON b.id = v.id
            FULL OUTER JOIN trigram_search t ON COALESCE(b.id, v.id) = t.id
        )
        SELECT c.id, c.collection_name, c.source_file, c.source_type, c.doc_type,
               c.identifier, c.identifier_field, c.identifier_namespace, c.identifier_kind,
               c.primary_name, c.description, c.nlp_text, c.payload,
               f.rrf_score AS bm25_score
        FROM fused f
        JOIN chunks c ON c.id = f.id
        ORDER BY f.rrf_score DESC
        LIMIT %s
    """

    params = []
    # ts_rank args
    params.extend(bm25_queries)
    # bm25_search WHERE collection_name
    params.append(collection_name)
    if doc_type:
        params.append(doc_type)
    if identifier_namespace:
        params.append(identifier_namespace)
    params.extend(bm25_queries)
    # bm25_search LIMIT
    params.append(rrf_limit)
    # vector ORDER BY
    params.append(vector_str)
    # vector WHERE collection_name
    params.append(collection_name)
    if doc_type:
        params.append(doc_type)
    if identifier_namespace:
        params.append(identifier_namespace)
    params.append(rrf_limit)
    # trigram GREATEST args
    params.extend(bm25_queries)
    # trigram WHERE collection_name
    params.append(collection_name)
    if doc_type:
        params.append(doc_type)
    if identifier_namespace:
        params.append(identifier_namespace)
    params.extend(bm25_queries)
    # trigram LIMIT
    params.append(rrf_limit)
    # final LIMIT
    params.append(limit)

    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows, score_key="bm25_score")

# ---------------------------------------------------------------------------
# Enum lookup
# Replaces: scroll + manual enum_values JSONB scan
# ---------------------------------------------------------------------------
def search_enum_values(
    collection_name: str,
    search_text: str,
    limit: int = 10,
) -> List[Point]:
    """
    Reverse enum lookup -- find chunks that have a given enum value or name.
    Replaces scroll + manual JSON scan of enum_values arrays.
    Uses normalized enum_values table for fast indexed lookup.
    """
    sql = """
        SELECT DISTINCT
            c.id, c.collection_name, c.source_file, c.source_type, c.doc_type,
            c.identifier, c.identifier_field, c.identifier_namespace, c.identifier_kind,
            c.primary_name, c.description, c.nlp_text, c.payload,
            e.enum_value as _matched_enum_value,
            e.enum_name as _matched_enum_name,
            e.enum_description as _matched_enum_description
        FROM enum_values e
        JOIN chunks c ON c.id = e.chunk_id
        WHERE e.collection_name = %s
          AND (
              LOWER(e.enum_value) = LOWER(%s)
              OR LOWER(e.enum_name) = LOWER(%s)
              OR LOWER(e.enum_description) LIKE LOWER(%s)
          )
        LIMIT %s
    """
    rows = fetchall(
        sql,
        (collection_name, search_text, search_text, f"%{search_text}%", limit)
    )

    points = []
    for row in rows:
        p = Point(row)
        # Attach matched enum info to payload for answer formatter
        p.payload["_matched_enum"] = {
            "enum_value": row.get("_matched_enum_value"),
            "enum_name": row.get("_matched_enum_name"),
            "description": row.get("_matched_enum_description"),
        }
        points.append(p)

    return points

def collection_has_enums(collection_name: str) -> bool:
    """Return True if the collection has any enum values ingested."""
    rows = fetchall(
        "SELECT 1 FROM enum_values WHERE collection_name = %s LIMIT 1",
        (collection_name,)
    )
    return len(rows) > 0


# ---------------------------------------------------------------------------
# Structured role search
# Replaces: scroll + manual role field scoring
# ---------------------------------------------------------------------------
def search_by_role_field(
    collection_name: str,
    search_text: str,
    role: str,
    doc_type: str = "structured",
    limit: int = 10,
) -> List[Point]:
    """
    Search chunks where a specific role field contains search_text.
    role can be: primary_name, description, identifier
    """
    role_col_map = {
        "primary_name": "primary_name",
        "description": "description",
        "identifier": "identifier",
    }

    col = role_col_map.get(role)
    if not col:
        return search_bm25(collection_name, search_text, doc_type=doc_type, limit=limit)

    conditions = [
        "collection_name = %s",
        f"LOWER({col}) LIKE LOWER(%s)",
    ]
    params = [collection_name, f"%{search_text}%"]

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    params.append(limit)

    sql = f"""
        SELECT id, collection_name, source_file, source_type, doc_type,
               identifier, identifier_field, identifier_namespace, identifier_kind,
               primary_name, description, nlp_text, payload
        FROM chunks
        WHERE {" AND ".join(conditions)}
        LIMIT %s
    """
    rows = fetchall(sql, tuple(params))
    return _rows_to_points(rows)


# ---------------------------------------------------------------------------
# Collection stats
# ---------------------------------------------------------------------------
def get_collection_point_count(collection_name: str) -> int:
    """Return total chunk count for a collection."""
    return fetchval(
        "SELECT COUNT(*) FROM chunks WHERE collection_name = %s",
        (collection_name,)
    ) or 0


def get_collections_with_counts() -> List[Dict[str, Any]]:
    """Return all collections with chunk counts."""
    return fetchall("""
        SELECT c.name, c.source_label, COUNT(ch.id) as point_count
        FROM collections c
        LEFT JOIN chunks ch ON ch.collection_name = c.name
        GROUP BY c.name, c.source_label
        ORDER BY c.name
    """)
