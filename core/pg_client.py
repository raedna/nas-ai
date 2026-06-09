"""
core/pg_client.py
=================
PostgreSQL backend for NAS-AI ingestion.
Drop-in replacement for core/qdrant_client.py.

Exposes the same function signatures as qdrant_client.py so the
orchestrator and ingest_collection.py need minimal changes.

Key functions:
    upsert_vectors()    -- replaces qdrant_client.upsert_vectors()
    recreate_collection() -- replaces qdrant_client.recreate_collection()
    update_file_state_pg() -- replaces collection_state.update_file_state()
    should_skip_file_pg()  -- replaces collection_state.should_skip_file()
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.db import (
    execute,
    fetchone,
    fetchval,
    upsert_chunk,
    upsert_file_state,
    should_skip_file_pg,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stable chunk ID generation
# Deterministic: same payload always gets same ID.
# Format: collection:sourcefile_hash8:seq
# ---------------------------------------------------------------------------
def _make_chunk_id(collection_name: str, source_file: str, seq: int) -> str:
    file_hash = hashlib.sha256(str(source_file).encode()).hexdigest()[:8]
    return f"{collection_name}:{file_hash}:{seq:06d}"


# ---------------------------------------------------------------------------
# Collection management
# Replaces qdrant_client.recreate_collection()
# ---------------------------------------------------------------------------
def recreate_collection(collection_name: str) -> None:
    """
    Delete all chunks and files for a collection.
    Equivalent to Qdrant recreate_collection (force re-ingest).
    The collection config row in collections table is preserved.
    """
    execute(
        "DELETE FROM files WHERE collection_name = %s",
        (collection_name,)
    )
    execute(
        "DELETE FROM chunks WHERE collection_name = %s",
        (collection_name,)
    )
    logger.info(f"[pg_client] recreate_collection: cleared {collection_name}")
    print(f"[PG] Cleared collection: {collection_name}")


def ensure_collection_exists(collection_name: str) -> None:
    """
    No-op for PostgreSQL -- collections table row was created during
    collections.json migration. Called for compatibility with qdrant_client.
    """
    pass


# ---------------------------------------------------------------------------
# File state management
# Replaces core/collection_state.py
# ---------------------------------------------------------------------------
def update_file_state_pg(
    collection_name: str,
    file_path: Path,
    filetype_name: str,
    result: Any,  # FileResult dataclass
    extra_metadata: Dict = None,
) -> None:
    """
    Update PostgreSQL files table with ingestion result.
    Replaces collection_state.update_file_state().
    """
    path_obj = Path(file_path)
    stat = path_obj.stat() if path_obj.exists() else None

    # Compute file hash for skip detection
    file_hash = None
    if path_obj.exists():
        try:
            h = hashlib.sha256()
            with open(path_obj, "rb") as f:
                for chunk in iter(lambda: f.read(65536), b""):
                    h.update(chunk)
            file_hash = h.hexdigest()
        except Exception as e:
            logger.warning(f"Could not hash {path_obj}: {e}")

    status = (
        "failed" if not result.success
        else "skipped" if result.skipped
        else "ingested"
    )

    metadata = dict(result.metadata or {})
    if extra_metadata:
        metadata.update(extra_metadata)

    upsert_file_state(
        collection_name=collection_name,
        file_path=str(path_obj.resolve()),
        file_hash=file_hash,
        mtime=stat.st_mtime if stat else None,
        file_size=stat.st_size if stat else None,
        filetype=filetype_name,
        status=status,
        chunk_count=result.chunks_created or 0,
        error=result.error,
        metadata=metadata,
    )


def should_skip_file_with_hash(
    collection_name: str,
    file_path: Path,
) -> Tuple[bool, str]:
    """
    Check if file should be skipped using PostgreSQL files table + hash.
    Replaces collection_state.should_skip_file().
    More reliable than mtime+size check.
    """
    path_obj = Path(file_path)

    if not path_obj.exists():
        return False, "file_missing"

    # Compute current hash
    try:
        h = hashlib.sha256()
        with open(path_obj, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        current_hash = h.hexdigest()
    except Exception as e:
        logger.warning(f"Could not hash {path_obj}: {e}")
        return False, "hash_error"

    return should_skip_file_pg(
        collection_name=collection_name,
        file_path=str(path_obj.resolve()),
        file_hash=current_hash,
    )


# ---------------------------------------------------------------------------
# Enum values helper
# ---------------------------------------------------------------------------
def _upsert_enum_values(chunk_id: str, collection_name: str, enum_values: List[Any]) -> None:
    """
    Insert enum values for a chunk into the enum_values table.
    Deletes existing enum values for the chunk first (clean upsert).
    """
    if not enum_values:
        return

    execute(
        "DELETE FROM enum_values WHERE chunk_id = %s",
        (chunk_id,)
    )

    for ev in enum_values:
        if isinstance(ev, dict):
            enum_value = (
                ev.get("enum_value") or ev.get("Value") or
                ev.get("value") or ""
            )
            enum_name = (
                ev.get("enum_name") or ev.get("SymbolicName") or
                ev.get("name") or ""
            )
            enum_desc = (
                ev.get("description") or ev.get("Description") or
                ev.get("enum_description") or ""
            )
        else:
            enum_value = str(ev)
            enum_name = ""
            enum_desc = ""

        if not enum_value and not enum_name:
            continue

        execute("""
            INSERT INTO enum_values (chunk_id, collection_name, enum_value, enum_name, enum_description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (chunk_id, collection_name, enum_value, enum_name, enum_desc))


# ---------------------------------------------------------------------------
# Main upsert function
# Replaces qdrant_client.upsert_vectors() -- same signature.
# ---------------------------------------------------------------------------
def upsert_vectors(
    collection_name: str,
    vectors: List[List[float]],
    payloads: List[Dict[str, Any]],
    source_file: str = None,
    force_reingest: bool = False,
) -> int:
    """
    Write vectors and payloads to PostgreSQL chunks table.
    Drop-in replacement for qdrant_client.upsert_vectors().

    Same signature -- orchestrator calls this without knowing
    whether the backend is Qdrant or PostgreSQL.
    """
    if not vectors or not payloads:
        raise ValueError("Vectors or payloads empty")

    if len(vectors) != len(payloads):
        raise ValueError("Vectors and payloads length mismatch")

    print(f"[PG] upsert_vectors: collection={collection_name} count={len(payloads)}")

    upserted = 0

    for seq, (vec, payload) in enumerate(zip(vectors, payloads)):
        # Determine source file
        effective_source = (
            payload.get("source_file") or
            payload.get("ingest_source") or
            (Path(source_file).name if source_file else None) or
            "unknown"
        )

        # Build stable chunk ID
        chunk_id = _make_chunk_id(collection_name, effective_source, seq)

        # Build chunk dict
        chunk = {
            "id": chunk_id,
            "collection_name": collection_name,
            "file_id": None,  # linked later via file_path if needed
            "source_file": effective_source,
            "source_type": payload.get("source_type"),
            "doc_type": payload.get("doc_type"),
            "identifier": str(payload.get("identifier")) if payload.get("identifier") is not None else None,
            "identifier_field": payload.get("identifier_field"),
            "identifier_namespace": payload.get("identifier_namespace"),
            "identifier_kind": payload.get("identifier_kind"),
            "primary_name": payload.get("primary_name"),
            "description": payload.get("description"),
            "nlp_text": payload.get("text", ""),
            "embedding": vec,
            "embedding_model": payload.get("embedding_model"),
            "embedded_at": None,
            "payload": payload,
        }

        upsert_chunk(chunk)

        # Write enum values to normalized table
        enum_values = payload.get("enum_values") or []
        if enum_values:
            _upsert_enum_values(chunk_id, collection_name, enum_values)

        upserted += 1

    print(f"[PG] upserted {upserted} chunks into {collection_name}")
    return upserted
