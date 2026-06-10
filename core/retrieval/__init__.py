"""
core/retrieval — PostgreSQL-backed retrieval package.

Primary entry point:
    from core.retrieval.router import run_query_with_method, route_query

Sub-modules:
    db_retrieval  — all SQL queries (only file that touches PostgreSQL)
    structured    — namespace / identifier / primary_name lookups
    lexical       — BM25 and lexical search
    semantic      — pgvector similarity search
    crosslink     — relationships, enum lookups, payload merging
    discovery     — count / list queries
    reranker      — scoring and reranking
    answer        — answer synthesis
    router        — query routing entry point (use this)
"""

from core.retrieval.router import run_query_with_method, route_query, debug_route_query
from core.retrieval.answer import synthesize_answer, build_answer, get_display_labels, get_source_label

__all__ = [
    "run_query_with_method",
    "route_query",
    "debug_route_query",
    "synthesize_answer",
    "build_answer",
    "get_display_labels",
    "get_source_label",
]
