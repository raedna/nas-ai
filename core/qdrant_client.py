import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from core.system_config import load_system_config
from qdrant_client.models import Filter, FieldCondition, MatchValue


cfg = load_system_config()
client = QdrantClient(url=cfg["qdrant_url"])
VECTOR_SIZE = cfg["vector_size"]


# =========================================================
# COLLECTION HELPERS
# =========================================================
def ensure_collection_exists(collection_name):
    existing = [c.name for c in client.get_collections().collections]

    if collection_name not in existing:
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE
            )
        )


# =========================================================
# POINT UPSERT
# =========================================================
def upsert_points(collection_name, points, batch_size=500):
    if not points:
        raise ValueError("No points to upload (points list is empty)")

    for i in range(0, len(points), batch_size):
        batch = points[i:i + batch_size]

        client.upsert(
            collection_name=collection_name,
            points=batch
        )

# =========================================================
# DELETE EXISTING POINTS
# =========================================================
def delete_points_by_source_file(collection_name, source_file):
    if not source_file:
        return

    client.delete(
        collection_name=collection_name,
        points_selector=Filter(
            must=[
                FieldCondition(
                    key="source_file",
                    match=MatchValue(value=str(source_file))
                )
            ]
        ),
        wait=True
    )

# =========================================================
# VECTOR UPSERT
# =========================================================
def upsert_vectors(collection_name, vectors, payloads, source_file=None, force_reingest=False):
    if not vectors or not payloads:
        raise ValueError("Vectors or payloads empty")

    if len(vectors) != len(payloads):
        raise ValueError("Vectors and payloads length mismatch")

    ensure_collection_exists(collection_name)

    effective_source_file = None
    if payloads:
        effective_source_file = payloads[0].get("source_file")

    if not effective_source_file and source_file:
        from pathlib import Path
        effective_source_file = Path(source_file).name

    print("[UPSERT] collection:", collection_name)
    print("[UPSERT] source_file arg:", source_file)
    print("[UPSERT] first payload source_file:", payloads[0].get("source_file") if payloads else None)
    print("[UPSERT] effective_source_file:", effective_source_file)

    if effective_source_file:
        delete_points_by_source_file(collection_name, effective_source_file)
        print("[UPSERT] delete complete for:", effective_source_file)

    points = []

    for vec, payload in zip(vectors, payloads):
        point = PointStruct(
            id=str(uuid.uuid4()),
            vector=vec,
            payload=payload
        )
        points.append(point)

    print("[UPSERT] uploading points:", len(points))
    upsert_points(collection_name, points)

    return len(points)