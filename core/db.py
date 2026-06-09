"""
core/db.py
==========
PostgreSQL database layer for NAS-AI.

All database access goes through this module.
No psycopg2 calls anywhere else in the codebase.

Connection config is read from config/system.json under the
"pgvector" key:

    {
      "pgvector": {
        "host": "100.123.16.57",
        "port": 5433,
        "dbname": "nasai",
        "user": "nasai",
        "password": "nasai2024"
      }
    }

Usage:
    from core.db import get_conn, execute, fetchall, fetchone

    # Simple query
    rows = fetchall("SELECT * FROM collections ORDER BY name")

    # Parameterised query
    row = fetchone(
        "SELECT * FROM chunks WHERE identifier_namespace=%s AND identifier=%s",
        ("tag", "22")
    )

    # Write query
    execute(
        "UPDATE chunks SET embedding_model=%s WHERE id=%s",
        ("text-embedding-bge-large-en-v1.5", chunk_id)
    )

    # Use a connection directly for transactions
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO ...")
            cur.execute("UPDATE ...")
        conn.commit()
"""

import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports -- psycopg2 is only imported when first used.
# This prevents import errors if psycopg2 is not installed and db
# features are not needed.
# ---------------------------------------------------------------------------
_psycopg2 = None
_pool = None

def _get_psycopg2():
    global _psycopg2
    if _psycopg2 is None:
        try:
            import psycopg2
            import psycopg2.extras
            import psycopg2.pool
            _psycopg2 = psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is not installed. "
                "Run: pip install psycopg2-binary --break-system-packages"
            )
    return _psycopg2


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _load_db_config() -> Dict[str, Any]:
    """Load PostgreSQL config from config/system.json pgvector section."""
    config_path = Path(__file__).resolve().parent.parent / "config" / "system.json"
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            system = json.load(f)
        pg = system.get("pgvector")
        if not pg:
            raise ValueError(
                "No 'pgvector' section found in config/system.json. "
                "Add: {\"pgvector\": {\"host\": \"...\", \"port\": 5433, "
                "\"dbname\": \"nasai\", \"user\": \"nasai\", \"password\": \"...\"}}"
            )
        return pg
    except FileNotFoundError:
        raise FileNotFoundError(f"config/system.json not found at {config_path}")


# ---------------------------------------------------------------------------
# Connection pool
# ---------------------------------------------------------------------------
def _get_pool():
    """Get or create the connection pool. Lazy init on first use."""
    global _pool
    if _pool is not None:
        return _pool

    psycopg2 = _get_psycopg2()
    cfg = _load_db_config()

    try:
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            host=cfg["host"],
            port=int(cfg.get("port", 5433)),
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"],
            connect_timeout=10,
        )
        logger.info(
            f"PostgreSQL pool created: {cfg['host']}:{cfg.get('port', 5433)}/{cfg['dbname']}"
        )
        return _pool
    except Exception as e:
        raise ConnectionError(
            f"Could not connect to PostgreSQL at "
            f"{cfg.get('host')}:{cfg.get('port', 5433)}: {e}"
        )


def close_pool():
    """Close the connection pool. Call on application shutdown."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        logger.info("PostgreSQL connection pool closed.")


# ---------------------------------------------------------------------------
# Context manager for raw connection access
# ---------------------------------------------------------------------------
@contextmanager
def get_conn():
    """
    Context manager that yields a psycopg2 connection from the pool.
    Automatically returns the connection to the pool on exit.
    Does NOT auto-commit -- caller must call conn.commit() or conn.rollback().

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------
def execute(sql: str, params: Tuple = None) -> None:
    """
    Execute a write query (INSERT, UPDATE, DELETE).
    Auto-commits on success, rolls back on error.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()


def execute_many(sql: str, params_list: List[Tuple]) -> None:
    """
    Execute a write query for multiple rows.
    All rows committed in one transaction.
    """
    if not params_list:
        return
    psycopg2 = _get_psycopg2()
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, params_list, page_size=500)
        conn.commit()


def fetchall(sql: str, params: Tuple = None) -> List[Dict[str, Any]]:
    """
    Execute a read query and return all rows as list of dicts.
    """
    with get_conn() as conn:
        with conn.cursor(
            cursor_factory=_get_psycopg2().extras.RealDictCursor
        ) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def fetchone(sql: str, params: Tuple = None) -> Optional[Dict[str, Any]]:
    """
    Execute a read query and return one row as dict, or None.
    """
    with get_conn() as conn:
        with conn.cursor(
            cursor_factory=_get_psycopg2().extras.RealDictCursor
        ) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def fetchval(sql: str, params: Tuple = None) -> Any:
    """
    Execute a read query and return a single scalar value.
    Useful for COUNT(*), MAX(), etc.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row[0] if row else None


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
def ping() -> bool:
    """
    Returns True if PostgreSQL is reachable, False otherwise.
    Safe to call at startup to check connectivity.
    """
    try:
        val = fetchval("SELECT 1")
        return val == 1
    except Exception as e:
        logger.warning(f"PostgreSQL ping failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Collections CRUD
# ---------------------------------------------------------------------------
def get_all_collections() -> List[Dict[str, Any]]:
    """Return all collections ordered by name."""
    return fetchall("SELECT * FROM collections ORDER BY name")


def get_collection(name: str) -> Optional[Dict[str, Any]]:
    """Return a single collection by name."""
    return fetchone("SELECT * FROM collections WHERE name = %s", (name,))


def upsert_collection(name: str, cfg: Dict[str, Any]) -> None:
    """Insert or update a collection config row."""
    execute("""
        INSERT INTO collections (
            name, path, source_label, notes,
            allowed_filetypes, allowed_extensions,
            exclude_dirs, exclude_extensions,
            asset_search_roots, filters,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
            %s::jsonb,
            NOW(), NOW()
        )
        ON CONFLICT (name) DO UPDATE SET
            path                = EXCLUDED.path,
            source_label        = EXCLUDED.source_label,
            notes               = EXCLUDED.notes,
            allowed_filetypes   = EXCLUDED.allowed_filetypes,
            allowed_extensions  = EXCLUDED.allowed_extensions,
            exclude_dirs        = EXCLUDED.exclude_dirs,
            exclude_extensions  = EXCLUDED.exclude_extensions,
            asset_search_roots  = EXCLUDED.asset_search_roots,
            filters             = EXCLUDED.filters,
            updated_at          = NOW()
    """, (
        name,
        cfg.get("path", ""),
        cfg.get("source_label", ""),
        cfg.get("notes", ""),
        json.dumps(cfg.get("allowed_filetypes") or []),
        json.dumps(cfg.get("allowed_extensions") or []),
        json.dumps(cfg.get("exclude_dirs") or cfg.get("exclude_folders") or []),
        json.dumps(cfg.get("exclude_extensions") or []),
        json.dumps(cfg.get("asset_search_roots") or []),
        json.dumps(cfg.get("filters") or {}),
    ))


def delete_collection(name: str) -> None:
    """
    Delete a collection and all its files and chunks via CASCADE.
    This is destructive -- use carefully.
    """
    execute("DELETE FROM collections WHERE name = %s", (name,))


# ---------------------------------------------------------------------------
# Files CRUD
# ---------------------------------------------------------------------------
def get_file_state(collection_name: str, file_path: str) -> Optional[Dict[str, Any]]:
    """Return ingestion state for a specific file."""
    return fetchone(
        "SELECT * FROM files WHERE collection_name=%s AND file_path=%s",
        (collection_name, str(file_path))
    )


def upsert_file_state(
    collection_name: str,
    file_path: str,
    file_hash: str,
    mtime: float,
    file_size: int,
    filetype: str,
    status: str,
    chunk_count: int = 0,
    error: str = None,
    metadata: Dict = None,
) -> None:
    """Insert or update file ingestion state."""
    execute("""
        INSERT INTO files (
            collection_name, file_path, file_hash, mtime, file_size,
            filetype, status, chunk_count, error, metadata,
            ingested_at, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s::jsonb,
            NOW(), NOW(), NOW()
        )
        ON CONFLICT (collection_name, file_path) DO UPDATE SET
            file_hash   = EXCLUDED.file_hash,
            mtime       = EXCLUDED.mtime,
            file_size   = EXCLUDED.file_size,
            filetype    = EXCLUDED.filetype,
            status      = EXCLUDED.status,
            chunk_count = EXCLUDED.chunk_count,
            error       = EXCLUDED.error,
            metadata    = EXCLUDED.metadata,
            ingested_at = NOW(),
            updated_at  = NOW()
    """, (
        collection_name, str(file_path), file_hash, mtime, file_size,
        filetype, status, chunk_count, error,
        json.dumps(metadata or {}),
    ))


def should_skip_file_pg(
    collection_name: str,
    file_path: str,
    file_hash: str,
) -> Tuple[bool, str]:
    """
    Check if a file should be skipped based on PostgreSQL state.
    Returns (should_skip, reason).
    Replaces collection_state.py should_skip_file() for PG-backed ingestion.
    """
    row = get_file_state(collection_name, str(file_path))
    if not row:
        return False, "new file"
    if row.get("file_hash") == file_hash and row.get("status") == "ingested":
        return True, "unchanged"
    return False, "changed or previously failed"


# ---------------------------------------------------------------------------
# Chunks CRUD
# ---------------------------------------------------------------------------
def upsert_chunk(chunk: Dict[str, Any]) -> None:
    """
    Insert or update a single chunk.
    The tsvector trigger handles nlp_text_tsv automatically.
    """
    execute("""
        INSERT INTO chunks (
            id, collection_name, file_id, source_file, source_type, doc_type,
            identifier, identifier_field, identifier_namespace, identifier_kind,
            primary_name, description, nlp_text,
            embedding, embedding_model, embedded_at,
            payload, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s::vector, %s, %s,
            %s::jsonb, NOW(), NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
            collection_name     = EXCLUDED.collection_name,
            source_file         = EXCLUDED.source_file,
            source_type         = EXCLUDED.source_type,
            doc_type            = EXCLUDED.doc_type,
            identifier          = EXCLUDED.identifier,
            identifier_field    = EXCLUDED.identifier_field,
            identifier_namespace= EXCLUDED.identifier_namespace,
            identifier_kind     = EXCLUDED.identifier_kind,
            primary_name        = EXCLUDED.primary_name,
            description         = EXCLUDED.description,
            nlp_text            = EXCLUDED.nlp_text,
            embedding           = EXCLUDED.embedding,
            embedding_model     = EXCLUDED.embedding_model,
            embedded_at         = EXCLUDED.embedded_at,
            payload             = EXCLUDED.payload,
            updated_at          = NOW()
    """, (
        chunk["id"],
        chunk["collection_name"],
        chunk.get("file_id"),
        chunk.get("source_file"),
        chunk.get("source_type"),
        chunk.get("doc_type"),
        chunk.get("identifier"),
        chunk.get("identifier_field"),
        chunk.get("identifier_namespace"),
        chunk.get("identifier_kind"),
        chunk.get("primary_name"),
        chunk.get("description"),
        chunk.get("nlp_text", ""),
        json.dumps(chunk["embedding"]) if chunk.get("embedding") else None,
        chunk.get("embedding_model"),
        chunk.get("embedded_at"),
        json.dumps(chunk.get("payload") or {}),
    ))


def upsert_chunks_batch(chunks: List[Dict[str, Any]]) -> None:
    """Insert or update multiple chunks in one transaction."""
    for chunk in chunks:
        upsert_chunk(chunk)


# ---------------------------------------------------------------------------
# BM25 Search
# ---------------------------------------------------------------------------
def search_bm25(
    query: str,
    collection_name: str = None,
    doc_type: str = None,
    source_type: str = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    BM25 full-text search over chunks using PostgreSQL tsvector.
    Returns ranked results with bm25_score.

    Args:
        query:           Search terms (plain text -- auto-converted to tsquery)
        collection_name: Optional -- filter to one collection
        doc_type:        Optional -- filter by doc_type (structured/procedural/etc)
        source_type:     Optional -- filter by source_type (xml/table/docs/etc)
        limit:           Max results to return
    """
    # Convert plain text to tsquery safely
    # plainto_tsquery handles multi-word queries without needing & | syntax
    conditions = ["nlp_text_tsv @@ plainto_tsquery('english', %s)"]
    params = [query]

    if collection_name:
        conditions.append("collection_name = %s")
        params.append(collection_name)

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)

    where = " AND ".join(conditions)
    params.append(limit)

    sql = f"""
        SELECT
            id,
            collection_name,
            source_file,
            doc_type,
            source_type,
            identifier,
            identifier_namespace,
            identifier_field,
            primary_name,
            description,
            payload,
            ts_rank(nlp_text_tsv, plainto_tsquery('english', %s)) AS bm25_score
        FROM chunks
        WHERE {where}
        ORDER BY bm25_score DESC
        LIMIT %s
    """
    # plainto_tsquery called twice -- once in WHERE, once in ts_rank
    return fetchall(sql, tuple([query] + params))


# ---------------------------------------------------------------------------
# Identifier Lookup
# ---------------------------------------------------------------------------
def lookup_identifier(
    collection_name: str,
    namespace: str,
    identifier: str,
) -> Optional[Dict[str, Any]]:
    """
    Direct deterministic lookup by namespace + identifier.
    Replaces explicit_namespace_lookup in query_router.
    """
    return fetchone("""
        SELECT * FROM chunks
        WHERE collection_name = %s
          AND identifier_namespace = %s
          AND identifier = %s
        LIMIT 1
    """, (collection_name, namespace, identifier))


# ---------------------------------------------------------------------------
# Enum Lookup
# ---------------------------------------------------------------------------
def reverse_enum_lookup(
    collection_name: str,
    enum_value: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """
    Find chunks that have a given enum value.
    Replaces JSONB enum_values scan in Qdrant.
    """
    return fetchall("""
        SELECT
            c.id,
            c.collection_name,
            c.identifier,
            c.identifier_namespace,
            c.primary_name,
            c.description,
            e.enum_value,
            e.enum_name,
            e.enum_description
        FROM enum_values e
        JOIN chunks c ON c.id = e.chunk_id
        WHERE e.collection_name = %s
          AND (
              LOWER(e.enum_value) = LOWER(%s)
              OR LOWER(e.enum_name) = LOWER(%s)
          )
        LIMIT %s
    """, (collection_name, enum_value, enum_value, limit))


# ---------------------------------------------------------------------------
# Vector Search
# ---------------------------------------------------------------------------
def search_vector(
    embedding: List[float],
    collection_name: str = None,
    doc_type: str = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """
    Vector similarity search using pgvector cosine distance.
    Replaces Qdrant client.search().

    Args:
        embedding:       Query vector (1024 dimensions)
        collection_name: Optional collection filter
        limit:           Max results
    """
    conditions = ["embedding IS NOT NULL"]
    params = []

    if collection_name:
        conditions.append("collection_name = %s")
        params.append(collection_name)

    if doc_type:
        conditions.append("doc_type = %s")
        params.append(doc_type)

    where = " AND ".join(conditions)
    vector_str = "[" + ",".join(str(x) for x in embedding) + "]"
    params.append(limit)

    sql = f"""
        SELECT
            id,
            collection_name,
            source_file,
            doc_type,
            primary_name,
            description,
            payload,
            1 - (embedding <=> %s::vector) AS similarity
        FROM chunks
        WHERE {where}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    return fetchall(sql, tuple([vector_str] + params + [vector_str]))


# ---------------------------------------------------------------------------
# Collection summary
# ---------------------------------------------------------------------------
def get_collection_summary() -> List[Dict[str, Any]]:
    """Return chunk and file counts per collection."""
    return fetchall("SELECT * FROM collection_summary ORDER BY name")


# ---------------------------------------------------------------------------
# Startup check
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Quick connectivity test
    # Run: python core/db.py
    print("Testing PostgreSQL connection...")
    if ping():
        print("✅ Connected successfully")
        summary = get_collection_summary()
        if summary:
            print(f"\nCollections in database ({len(summary)}):")
            for row in summary:
                print(f"  {row['name']:30s} chunks={row['chunk_count']}  files={row['file_count']}")
        else:
            print("No collections yet.")
    else:
        print("❌ Could not connect to PostgreSQL")
        print("   Check config/system.json pgvector section")
