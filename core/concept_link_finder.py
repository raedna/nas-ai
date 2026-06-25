import json
import numpy as np
from core.db import fetchall


def find_concept_links(collection, chunk_id, target_collections=None, similarity_threshold=0.75):
    """
    Given a chunk from a collection, find related concept clusters in other collections
    by comparing its concept cluster centroid against target collection centroids.
    
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

    # Step 3: Compare source centroid against all target cluster centroids
    results = []

    for target in target_collections:
        src_centroid_str = src_centroid
        target_clusters = fetchall("""
            SELECT cluster_id, group_value, centroid, anchor_chunk_ids, anchor_texts,
                   1 - (centroid::vector <=> %s::vector) AS similarity
            FROM concept_vectors
            WHERE collection = %s
            AND 1 - (centroid::vector <=> %s::vector) >= %s
            ORDER BY similarity DESC
            LIMIT 3
        """, (src_centroid_str, target, src_centroid_str, similarity_threshold))

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