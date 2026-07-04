import json
import numpy as np
from collections import Counter
from core.db import fetchall, execute


def _clean(v):
    """Treat NULL / 'none' / '' as absent."""
    s = str(v or "").strip()
    return s if s and s.lower() != "none" else ""


def _get_group_field(collection):
    """Determine which payload field to group chunks by — purely data-driven."""
    sample = fetchall("""
        SELECT
            payload->>'folder_path'  AS folder_path,
            payload->>'category'     AS category,
            payload->>'type'         AS type,
            payload->>'source_file'  AS source_file,
            payload->>'section_heading' AS section_heading,
            payload->>'identifier_namespace' AS namespace
        FROM chunks WHERE collection_name = %s LIMIT 50
    """, (collection,))

    if not sample:
        return 'source_file'

    has_folder = any(_clean(r.get('folder_path')) for r in sample)
    has_category = any(_clean(r.get('category')) for r in sample)
    distinct_sections = len(set(
        _clean(r.get('section_heading')) for r in sample if _clean(r.get('section_heading'))
    ))

    distinct_namespaces = len(set(
        r.get('namespace') for r in sample if r.get('namespace')
    ))

    _data_types = {
        'double', 'string', 'int32', 'int64', 'float', 'boolean',
        'datetime', 'date', 'integer', 'decimal', 'structured'
    }
    meaningful_types = set(
        r.get('type', '').lower() for r in sample
        if r.get('type')
        and r.get('type', '').lower() not in _data_types
        and not r.get('type', '').strip().lstrip('-').isdigit()
    )

    # 1. Folder path — best for doc collections with hierarchy (obsidian)
    if has_folder:
        return 'folder_path'

    # 1b. CV-01: section_heading for chunked docs (docx/pdf) with real headings
    if distinct_sections > 1:
        return 'section_heading'

    # 2. Multiple distinct namespaces — XML/FIX collections
    if distinct_namespaces > 1:
        return 'identifier_namespace'

    # 3. Meaningful type values — broker names, object types etc.
    if len(meaningful_types) > 1:
        return 'type'

    # 4. Category field
    if has_category:
        return 'category'

    # 5. Tags populated on >=20% of chunks — tag/IDF grouping (CV-02/03)
    tag_stats = fetchall("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE payload->>'tags' IS NOT NULL
                                AND payload->>'tags' NOT IN ('', '[]')) AS tagged
        FROM chunks WHERE collection_name = %s
    """, (collection,))
    if tag_stats and tag_stats[0]['total'] and tag_stats[0]['tagged'] / tag_stats[0]['total'] >= 0.2:
        return 'tags'

    # 6. Fallback
    return 'source_file'


# ---------------------------------------------------------------------------
# Row -> chunk-object helpers
# ---------------------------------------------------------------------------
def _mk_obj(row):
    emb = row['embedding']
    if isinstance(emb, str):
        emb = json.loads(emb)
    return {
        'id': row['id'],
        'text': row.get('text') or '',
        'embedding': np.array(emb, dtype=np.float32),
    }


# ---------------------------------------------------------------------------
# Grouping — field-based (existing path) and entity_row computed (CV-02/03)
# ---------------------------------------------------------------------------
def _field_groups(collection):
    group_field = _get_group_field(collection)
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

    groups = {}
    for row in rows:
        gv = _clean(row['group_value'])
        if not gv:
            continue
        groups.setdefault(gv, []).append(_mk_obj(row))
    return groups, group_field


def _parse_kb_tags(raw):
    """tags payload is a JSON list; tolerate string/CSV too."""
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    s = str(raw).strip()
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return [str(t).strip() for t in v if str(t).strip()]
    except Exception:
        pass
    return [t.strip() for t in s.split(",") if t.strip()]


def _entity_row_groups(collection):
    """CV-02/03: per-chunk group_value for entity_row collections.
    Tier 1: most distinctive *shared* kb_tag by inverse document frequency.
    Tier 2: TF-IDF top terms -> one batched LLM call for a shared topic label."""
    from core.query_helpers import load_doc_query_hints

    rows = fetchall("""
        SELECT id, payload->>'tags' AS kb_tags,
               COALESCE(payload->>'text', payload->>'description', '') AS text,
               embedding
        FROM chunks
        WHERE collection_name = %s AND embedding IS NOT NULL
    """, (collection,))

    generic = {t.lower() for t in load_doc_query_hints().get("generic_terms", [])}
    objs, tags_by_id, df = {}, {}, Counter()
    for r in rows:
        objs[r['id']] = _mk_obj(r)
        tags = [t for t in _parse_kb_tags(r['kb_tags']) if t.lower() not in generic]
        tags_by_id[r['id']] = tags
        for t in set(tags):
            df[t] += 1

    n = max(1, len(rows))
    broad = {t for t, c in df.items() if c > max(3, 0.30 * n)}   # too-generic batch labels

    groups, tier2 = {}, []
    for cid, obj in objs.items():
        tags = [t for t in tags_by_id[cid] if t not in broad]
        if not tags:
            tier2.append(obj)
            continue
        shared = [t for t in tags if df[t] >= 2]            # CV-02: distinctive but shared
        pool = shared or tags
        best = min(pool, key=lambda t: (df[t], -len(t)))    # lowest df = highest IDF
        groups.setdefault(best, []).append(obj)

    if tier2:
        _assign_tier2_topic_groups(tier2, groups)           # CV-03
    return groups, 'tags'


def _assign_tier2_topic_groups(tier2_objs, groups):
    """CV-03: TF-IDF top terms per chunk -> one batched LLM call for shared topic labels."""
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        vec = TfidfVectorizer(max_features=2000, stop_words='english', ngram_range=(1, 2))
        mat = vec.fit_transform([o['text'] or '' for o in tier2_objs])
        terms = vec.get_feature_names_out()
    except Exception:
        for o in tier2_objs:
            groups.setdefault('Uncategorized', []).append(o)
        return

    top_terms = []
    for i in range(mat.shape[0]):
        rowm = mat.getrow(i)
        idx = rowm.indices[np.argsort(-rowm.data)[:10]] if rowm.nnz else []
        top_terms.append([terms[j] for j in idx])

    labels = _llm_topic_labels(top_terms)
    for o, tt, lab in zip(tier2_objs, top_terms, labels):
        gv = lab or (tt[0] if tt else 'Uncategorized')
        groups.setdefault(gv, []).append(o)


def _llm_topic_labels(top_terms_per, batch_size=30):
    from core.local_llm_client import call_local_llm_json
    labels = [None] * len(top_terms_per)
    sys_p = ("You assign each document a short topic label (2-4 words, Title Case). "
             "Reuse the SAME label for documents on the same topic. "
             "Respond ONLY with JSON mapping each index (as a string) to its label.")
    for s in range(0, len(top_terms_per), batch_size):
        batch = top_terms_per[s:s + batch_size]
        listing = "\n".join(f"{i}: {', '.join(t)}" for i, t in enumerate(batch))
        resp = call_local_llm_json(
            sys_p,
            f'Documents (index: key terms):\n{listing}\n\nReturn e.g. {{"0":"Trade Settlement"}}')
        if isinstance(resp, dict):
            for k, v in resp.items():
                try:
                    idx = int(k)
                except (TypeError, ValueError):
                    continue
                if 0 <= idx < len(batch) and isinstance(v, str) and v.strip():
                    labels[s + idx] = v.strip()
    return labels

def _llm_consolidate_labels(groups, batch_size=60):
    """CV-03b: merge synonymous group labels via one LLM pass.
    Asks the LLM to map variant labels to a canonical label that MUST be one of
    the existing labels — invented names are rejected. Groups whose labels map
    to the same canonical are merged. Merge mapping is printed for review."""
    from core.local_llm_client import call_local_llm_json

    labels = sorted(groups.keys())
    if len(labels) < 2:
        return groups

    canonical_map = {}
    sys_p = ("You deduplicate topic labels for a technical knowledge base. "
             "Merge ONLY labels that are trivial rewordings of the SAME specific topic: "
             "plural/singular, verb forms (Create/Creating), or identical meaning. "
             "NEVER merge labels that differ in environment or stage (QA vs PROD vs UAT vs DEV). "
             "NEVER merge different functions of the same system — e.g. login issues, order export, "
             "account setup, and environment recovery are all DIFFERENT topics even if all mention the same product. "
             "NEVER merge a specific job/alert/ID with a general category. "
             "When unsure, do NOT merge. "
             "Respond ONLY with JSON mapping each duplicate label to its canonical label. "
             "The canonical label MUST be copied exactly from the list. "
             "Unique labels must be OMITTED from the response.")

    for s in range(0, len(labels), batch_size):
        batch = labels[s:s + batch_size]
        listing = "\n".join(f"- {l}" for l in batch)
        resp = call_local_llm_json(
            sys_p,
            f'Labels:\n{listing}\n\nReturn e.g. {{"Creating KB Articles":"Create KB Article"}}')
        if not isinstance(resp, dict):
            continue
        batch_set = set(batch)
        for variant, canonical in resp.items():
            if (isinstance(variant, str) and isinstance(canonical, str)
                    and variant in batch_set and canonical in batch_set
                    and variant != canonical):
                canonical_map[variant] = canonical

    if not canonical_map:
        return groups

    # Resolve chains (A->B, B->C becomes A->C), with a cycle guard
    def _resolve(l, seen=None):
        seen = seen or set()
        while l in canonical_map and l not in seen:
            seen.add(l)
            l = canonical_map[l]
        return l

    merged = {}
    for label, chunks in groups.items():
        target = _resolve(label)
        if target != label:
            print(f"[CONCEPT] Label merge: '{label}' -> '{target}'")
        merged.setdefault(target, []).extend(chunks)
    return merged

def _build_groups(collection):
    if _get_group_field(collection) == 'tags':
        return _entity_row_groups(collection)
    return _field_groups(collection)


# ---------------------------------------------------------------------------
# CV-04: relabel filename/generic group_values from anchor texts via LLM
# ---------------------------------------------------------------------------
_BOOLEANISH = {"yes", "no", "true", "false", "none", "na", "n/a", "tbd", "null", "y", "n"}


def _looks_generic_group_value(gv, group_field):
    """A group_value that makes a poor 'related topic' label and should be relabeled
    by the LLM from anchor texts — filenames, boolean/flag values, very short codes,
    pure numbers, and configured generic terms."""
    import re
    g = str(gv or "").strip()
    if not g:
        return True
    if group_field == 'source_file':
        return True
    if re.match(r'.+\.[A-Za-z0-9]{1,6}$', g):              # filename-like
        return True
    if len(g) <= 3:                                        # NO, FX, tag, QA, UAT-ish stubs
        return True
    if g.lower() in _BOOLEANISH:                           # boolean/flag column values
        return True
    if g.replace("-", "").replace(".", "").isdigit():     # pure numeric / version-ish
        return True
    from core.query_helpers import load_doc_query_hints
    return g.lower() in {t.lower() for t in load_doc_query_hints().get("generic_terms", [])}


def _llm_label_from_anchors(anchor_texts):
    from core.local_llm_client import call_local_llm_json
    joined = "\n---\n".join((t or "")[:400] for t in (anchor_texts or [])[:5] if t)
    if not joined.strip():
        return None
    resp = call_local_llm_json(
        'Generate ONE short topic label (2-5 words, Title Case) for these related '
        'excerpts. Respond ONLY as JSON {"label":"..."}.',
        joined)
    if isinstance(resp, dict) and isinstance(resp.get("label"), str) and resp["label"].strip():
        return resp["label"].strip()
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def build_concept_vectors(collection, min_cluster_size=3, similarity_threshold=0.75):
    """
    Build concept vectors for a collection by:
    1. Grouping chunks (entity_row: tag/topic; else folder/section/type/source_file)
    2. Clustering embeddings with HDBSCAN
    3. Multi-label assigning chunks to clusters
    4. Relabeling filename/generic groups via LLM (CV-04)
    5. Storing centroids + anchor chunks
    """
    import hdbscan
    from sklearn.metrics.pairwise import cosine_similarity

    groups, group_field = _build_groups(collection)
    print(f"[CONCEPT] Building concept vectors for {collection} grouped by {group_field}")
    if not groups:
        print(f"[CONCEPT] No rows found for {collection}")
        return 0

    # Clear stale concept vectors for this collection before rebuilding, so old
    # group_values (prior grouping fields / pre-relabel filenames) don't linger.
    execute("DELETE FROM concept_vectors WHERE collection = %s", (collection,))

    state = {"saved": 0, "relabel_cid": 900000}
    label_cache = {}

    def _save(group_value, cluster_id, centroid, anchor_ids, anchor_texts):
        label, cid = group_value, int(cluster_id)
        # CV-04: swap a filename/generic group_value for an LLM topic label, using a
        # high running cluster_id so two relabeled groups can't collide on the
        # (collection, group_value, cluster_id) conflict key.
        if _looks_generic_group_value(group_value, group_field):
            if group_value not in label_cache:
                label_cache[group_value] = _llm_label_from_anchors(anchor_texts)
            if label_cache[group_value]:
                label = label_cache[group_value]
                cid = state["relabel_cid"]
                state["relabel_cid"] += 1
        _save_cluster(collection, group_field, label, cid, centroid, anchor_ids, anchor_texts)
        state["saved"] += 1

    for group_value, chunks in groups.items():
        if len(chunks) < 2:
            _save(group_value, 0, chunks[0]['embedding'],
                  [chunks[0]['id']], [chunks[0]['text']])
            continue

        embeddings = np.array([c['embedding'] for c in chunks])
        _adaptive_min = max(2, min(min_cluster_size, len(chunks) // 10))
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=_adaptive_min, metric='euclidean', prediction_data=True)
        labels = clusterer.fit_predict(embeddings)

        unique_labels = set(labels)
        unique_labels.discard(-1)

        if not unique_labels:
            centroid = embeddings.mean(axis=0)
            _save(group_value, 0, centroid,
                  [c['id'] for c in chunks[:5]], [c['text'] for c in chunks[:5]])
            continue

        for cluster_id in unique_labels:
            primary_mask = labels == cluster_id
            centroid = embeddings[primary_mask].mean(axis=0)
            sims = cosine_similarity([centroid], embeddings)[0]
            assigned_indices = np.where(sims >= similarity_threshold)[0]
            if len(assigned_indices) == 0:
                assigned_indices = np.where(primary_mask)[0]
            top_indices = assigned_indices[np.argsort(-sims[assigned_indices])[:5]]
            _save(group_value, int(cluster_id), centroid,
                  [chunks[i]['id'] for i in top_indices],
                  [chunks[i]['text'] for i in top_indices])

    print(f"[CONCEPT] Saved {state['saved']} concept vectors for {collection}")
    return state["saved"]


def _save_cluster(collection, group_field, group_value, cluster_id,
                  centroid, anchor_ids, anchor_texts):
    centroid_list = centroid.tolist() if hasattr(centroid, "tolist") else list(centroid)
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
