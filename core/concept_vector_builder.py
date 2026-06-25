import json
import numpy as np
from core.db import fetchall, execute


def _get_group_field(collection):
    """Determine which payload field to group chunks by for this collection."""
    sample = fetchall("""
        SELECT 
            payload->>'folder_path' AS folder_path,
            payload->>'category' AS category,
            payload->>'type' AS type,
            payload->>'doc_type' AS doc_type,
            payload->>'source_file' AS source_file
        FROM chunks WHERE collection_name = %s LIMIT 50
    """, (collection,))

    if not sample:
        return 'source_file'

    has_folder = any(r.get('folder_path') for r in sample)
    has_category = any(r.get('category') for r in sample)
    has_type = any(r.get('type') for r in sample)
    doc_types = set(r.get('doc_type') for r in sample if r.get('doc_type'))
    unique_sources = len(set(r.get('source_file') for r in sample if r.get('source_file')))

    # Doc collections with folder structure — best grouping
    if has_folder:
        return 'folder_path'

    # Multi-file collections with category
    if has_category and unique_sources > 1:
        return 'category'

    # Structured single-file collections (RECON, BBG) — group by type (Goldman/JPM/Citi or field category)
    if 'structured' in doc_types and has_type:
        return 'type'

    # Single-file doc collections (KB articles) — group by category
    if has_category:
        return 'category'

    # XML/FIX collections — group by category if available, else source_file
    if has_category:
        return 'category'

    return 'source_file'


def build_concept_vectors(collection, min_cluster_size=3, similarity_threshold=0.75):
    """
    Build concept vectors for a collection by:
    1. Grouping chunks by folder_path/category/source_file
    2. Clustering embeddings with HDBSCAN
    3. Multi-label assigning chunks to clusters
    4. Storing centroids + anchor chunks
    """
    import hdbscan
    from sklearn.metrics.pairwise import cosine_similarity

    group_field = _get_group_field(collection)
    print(f"[CONCEPT] Building concept vectors for {collection} grouped by {group_field}")

    # Fetch all chunks with embeddings
    rows = fetchall(f"""
            SELECT 
                id,
                payload->>'{group_field}' AS group_value,
                COALESCE(LEFT(payload->>'description', 1000), LEFT(payload->>'text', 1000), '') AS text,
                embedding
            FROM chunks
            WHERE collection_name = %s
            AND embedding IS NOT NULL
            AND payload->>'{group_field}' IS NOT NULL
        """, (collection,))

    if not rows:
        print(f"[CONCEPT] No rows found for {collection}")
        return 0

    # Group by group_value
    groups = {}
    for row in rows:
        gv = (row['group_value'] or '').strip()
        if not gv:
            continue
        if gv not in groups:
            groups[gv] = []
        emb = row['embedding']
        if isinstance(emb, str):
            emb = json.loads(emb)
        groups[gv].append({
            'id': row['id'],
            'text': row['text'] or '',
            'embedding': np.array(emb, dtype=np.float32)
        })

    total_saved = 0

    for group_value, chunks in groups.items():
        if len(chunks) < 2:
            # Single chunk — treat as its own cluster
            centroid = chunks[0]['embedding']
            _save_cluster(collection, group_field, group_value, 0, centroid,
                         [chunks[0]['id']], [chunks[0]['text']])
            total_saved += 1
            continue

        embeddings = np.array([c['embedding'] for c in chunks])

        # Cluster with HDBSCAN
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min(min_cluster_size, max(2, len(chunks) // 3)),
            metric='euclidean',
            prediction_data=True
        )
        labels = clusterer.fit_predict(embeddings)

        unique_labels = set(labels)
        unique_labels.discard(-1)  # -1 = noise in HDBSCAN

        if not unique_labels:
            # All noise — treat entire group as one cluster
            centroid = embeddings.mean(axis=0)
            chunk_ids = [c['id'] for c in chunks]
            chunk_texts = [c['text'] for c in chunks[:5]]
            _save_cluster(collection, group_field, group_value, 0, centroid,
                         chunk_ids[:5], chunk_texts)
            total_saved += 1
            continue

        # For each cluster — multi-label assignment
        for cluster_id in unique_labels:
            # Get primary members
            primary_mask = labels == cluster_id
            primary_embeddings = embeddings[primary_mask]
            centroid = primary_embeddings.mean(axis=0)

            # Multi-label: assign ANY chunk above similarity threshold
            sims = cosine_similarity([centroid], embeddings)[0]
            assigned_indices = np.where(sims >= similarity_threshold)[0]

            if len(assigned_indices) == 0:
                assigned_indices = np.where(primary_mask)[0]

            # Anchor chunks: top 5 closest to centroid
            assigned_sims = sims[assigned_indices]
            top_indices = assigned_indices[np.argsort(-assigned_sims)[:5]]

            anchor_ids = [chunks[i]['id'] for i in top_indices]
            anchor_texts = [chunks[i]['text'] for i in top_indices]

            _save_cluster(collection, group_field, group_value, int(cluster_id),
                         centroid, anchor_ids, anchor_texts)
            total_saved += 1

    print(f"[CONCEPT] Saved {total_saved} concept vectors for {collection}")
    return total_saved


def _save_cluster(collection, group_field, group_value, cluster_id,
                  centroid, anchor_ids, anchor_texts):
    centroid_list = centroid.tolist()
    print(f"[DEBUG] saving cluster {cluster_id}, anchor_texts={anchor_texts[:2]}")
    execute("""
        INSERT INTO concept_vectors
            (collection, group_field, group_value, cluster_id, centroid, anchor_chunk_ids, anchor_texts)
        VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
        ON CONFLICT (collection, group_value, cluster_id)
        DO UPDATE SET
            centroid = EXCLUDED.centroid,
            anchor_chunk_ids = EXCLUDED.anchor_chunk_ids,
            anchor_texts = EXCLUDED.anchor_texts,
            created_at = NOW()
    """, (
        collection, group_field, group_value, cluster_id,
        json.dumps(centroid_list),
        json.dumps(anchor_ids),
        json.dumps(anchor_texts)
    ))