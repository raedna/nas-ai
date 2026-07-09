import json
import numpy as np
from statistics import median
from core.db import fetchall


def _cfg():
    """Config: concept_links {min_sim, margin}. Defaults measured from the
    corpus distribution (diag_concept_dist): ambient centroid-vs-centroid
    median ~0.60, genuine links >=0.81 with margins >=0.17 over that median;
    noise lives in the 0.75-0.80 band."""
    try:
        from core.system_config import load_system_config
        c = load_system_config().get("concept_links", {})
        return float(c.get("min_sim", 0.80)), float(c.get("margin", 0.15))
    except Exception:
        return 0.80, 0.15


def find_concept_links(collection, chunk_id, target_collections=None, similarity_threshold=None):
    """
    Given a chunk from a collection, find related concept clusters in other collections
    by comparing its concept cluster centroid against target collection centroids.

    A cluster qualifies only if it stands out from its OWN collection's ambient
    similarity level: sim >= min_sim AND sim >= median(all that collection's
    cluster sims to this source) + margin. Centroid-vs-centroid similarities are
    double-averaged and sit near the dense middle of the embedding space, so an
    absolute threshold alone admits ambient noise ("either too many or none").
    The median is computed from the live distribution — self-calibrating for
    new collections and embedding models.

    Returns list of {target_collection, cluster_id, similarity, anchor_chunk_ids, anchor_texts}
    """
    # Step 1: Find which concept cluster this chunk belongs to
    source_cluster = fetchall("""
        SELECT cv.collection, cv.group_value, cv.cluster_id, cv.centroid
        FROM concept_vectors cv
        WHERE cv.collection = %s
        AND cv.anchor_chunk_ids @> %s::jsonb
    """, (collection, json.dumps([str(chunk_id)])))

    if not source_cluster:
        # Chunk not an anchor — find nearest cluster by embedding similarity
        chunk_row = fetchall("""
            SELECT embedding FROM chunks WHERE id = %s
        """, (chunk_id,))
        
        if not chunk_row or not chunk_row[0]['embedding']:
            return []

        chunk_emb = chunk_row[0]['embedding']
        if isinstance(chunk_emb, str):
            chunk_emb = json.loads(chunk_emb)
        chunk_emb_str = json.dumps(chunk_emb) if isinstance(chunk_emb, list) else chunk_emb

        # Find nearest concept cluster in same collection
        source_cluster = fetchall("""
            SELECT collection, group_value, cluster_id, centroid,
                   centroid::vector <=> %s::vector AS distance
            FROM concept_vectors
            WHERE collection = %s
            ORDER BY distance ASC
            LIMIT 1
        """, (chunk_emb_str, collection))

    if not source_cluster:
        return []

    src = source_cluster[0]
    src_centroid = src['centroid']  # keep as string — pgvector expects this format

    # Step 2: Find target collections
    if target_collections is None:
        rows = fetchall(
            "SELECT DISTINCT collection FROM concept_vectors WHERE collection != %s",
            (collection,)
        )
        target_collections = [r['collection'] for r in rows]

    # Step 3: Compare source centroid against ALL target cluster centroids,
    # then keep only standouts above that collection's own ambient level.
    min_sim, margin = _cfg()
    if similarity_threshold is not None:  # explicit caller override
        min_sim = float(similarity_threshold)
    results = []

    for target in target_collections:
        src_centroid_str = src_centroid
        all_clusters = fetchall("""
            SELECT cluster_id, group_value, centroid, anchor_chunk_ids, anchor_texts,
                   1 - (centroid::vector <=> %s::vector) AS similarity
            FROM concept_vectors
            WHERE collection = %s
            ORDER BY similarity DESC
        """, (src_centroid_str, target))
        if not all_clusters:
            continue
        _amb = median(float(r["similarity"]) for r in all_clusters)
        target_clusters = [
            r for r in all_clusters
            if float(r["similarity"]) >= min_sim
            and float(r["similarity"]) >= _amb + margin
        ][:3]

        for tc in target_clusters:
            results.append({
                "target_collection": target,
                "cluster_id": tc['cluster_id'],
                "group_value": tc['group_value'],
                "similarity": float(tc['similarity']),
                "anchor_chunk_ids": tc['anchor_chunk_ids'] if isinstance(tc['anchor_chunk_ids'], list) else (json.loads(tc['anchor_chunk_ids']) if tc['anchor_chunk_ids'] else []),
                "anchor_texts": tc['anchor_texts'] if isinstance(tc['anchor_texts'], list) else (json.loads(tc['anchor_texts']) if tc['anchor_texts'] else []),
            })

    results.sort(key=lambda x: -x['similarity'])
    return results