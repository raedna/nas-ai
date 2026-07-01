"""
core/ui_data.py
===============
UI-framework-agnostic data helpers. Plain functions (no Streamlit/NiceGUI deps) so the
NiceGUI app, the Streamlit app, and any future API/MCP layer can all share them.
"""
from core.db import fetchall


def list_registered_collections():
    """Collection names from the collections config table."""
    rows = fetchall("SELECT name FROM collections ORDER BY name", ())
    return [r["name"] for r in rows]


def collection_stats():
    """Per-collection chunk + enum counts. Includes collections that have data even
    if their config row was removed (orphans), so the list reflects what's actually
    stored — same union logic the delete UI uses."""
    chunks = fetchall(
        "SELECT collection_name, COUNT(*) AS n FROM chunks GROUP BY collection_name", ())
    enums = fetchall(
        "SELECT collection_name, COUNT(*) AS n FROM enum_values GROUP BY collection_name", ())
    cmap = {r["collection_name"]: r["n"] for r in chunks}
    emap = {r["collection_name"]: r["n"] for r in enums}
    # Union of: collections with data (chunks/enums), DB-registered collections,
    # and collections defined in collections.json but not yet ingested — so a
    # freshly-created collection still appears (e.g. in the Ingestion dropdown)
    # with 0 chunks until it's ingested.
    configured = set(load_collections_config().keys())
    names = sorted(set(cmap) | set(emap) | set(list_registered_collections()) | configured)
    return [{"name": n, "chunks": cmap.get(n, 0), "enums": emap.get(n, 0)} for n in names]


def background_tasks(limit=15):
    """Recent background tasks (cross-link discovery / concept rebuild)."""
    return fetchall(
        """SELECT collection, task_name, status, started_at, finished_at
           FROM background_tasks ORDER BY id DESC LIMIT %s""", (limit,))


# ---------------------------------------------------------------------------
# Collection config CRUD + ingestion (shared actions; no UI deps)
# ---------------------------------------------------------------------------
import json as _json
from core.db import execute
from core.paths import COLLECTIONS_PATH


def load_collections_config():
    try:
        with open(COLLECTIONS_PATH, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return {}


def save_collections_config(cfg):
    with open(COLLECTIONS_PATH, "w", encoding="utf-8") as f:
        _json.dump(cfg, f, indent=2)


def get_collection_config(name):
    return load_collections_config().get(name, {})


def upsert_collection_config(name, cfg):
    """Persist a collection to collections.json AND the collections table. Returns an
    error string on partial failure, else None."""
    all_cfg = load_collections_config()
    all_cfg[name] = cfg
    save_collections_config(all_cfg)
    try:
        from core.db import upsert_collection
        upsert_collection(name, cfg)
    except Exception as e:
        return f"saved to config, but DB upsert failed: {e}"
    return None


def delete_collection(name, drop_config=True):
    """Delete a collection's data (chunks/files/enums/collections row); optionally its
    config entry too. Children deleted before the parent (FK-safe)."""
    execute("DELETE FROM enum_values WHERE collection_name = %s", (name,))
    execute("DELETE FROM chunks WHERE collection_name = %s", (name,))
    execute("DELETE FROM files WHERE collection_name = %s", (name,))
    execute("DELETE FROM collections WHERE name = %s", (name,))
    if drop_config:
        cfg = load_collections_config()
        if name in cfg:
            cfg.pop(name, None)
            save_collections_config(cfg)


def run_ingest(name, force=False):
    """Ingest a collection by name, then launch the background cross-link/concept
    rebuild. Returns the ingestion result dict (+ optional _bg_error)."""
    from core.ingest_collection import ingest_collection
    cfg = get_collection_config(name)
    result = ingest_collection(collection_name=name, collection_cfg=cfg, force_reingest=force)
    try:
        from core.background_runner import launch_cross_link_discovery
        launch_cross_link_discovery(name)
    except Exception as e:
        result = dict(result or {})
        result["_bg_error"] = str(e)
    return result


def ingest_single_file(collection_name, file_path):
    """Ingest one specific file into an existing collection, without touching the
    collection's normal configured scan path(s). Used by Data Prep's "Save + Ingest".
    Inherits the collection's real config (allowed_filetypes, etc.) but overrides the
    scan path to just this file, so it never re-scans or force-clears the collection."""
    from core.ingest_collection import ingest_collection
    cfg = dict(get_collection_config(collection_name))
    cfg["paths"] = [str(file_path)]
    cfg.pop("path", None)
    result = ingest_collection(collection_name=collection_name, collection_cfg=cfg, force_reingest=False)
    try:
        from core.background_runner import launch_cross_link_discovery
        launch_cross_link_discovery(collection_name)
    except Exception as e:
        result = dict(result or {})
        result["_bg_error"] = str(e)
    return result


def rebuild_links(name):
    """Launch cross-link discovery + concept-vector rebuild for a collection."""
    from core.background_runner import launch_cross_link_discovery
    launch_cross_link_discovery(name)


def collection_path_info(name):
    """Path existence/type for a collection (for the ingestion Path Check)."""
    import os
    cfg = get_collection_config(name)
    p = cfg.get("path") or (cfg.get("paths") or [""])[0]
    return {"path": p, "exists": bool(p) and os.path.exists(p),
            "is_dir": bool(p) and os.path.isdir(p)}


def scan_collection_files(name):
    """List the files that would be ingested for a collection (post-exclude)."""
    from core.ingest_collection import discover_files, _get_collection_paths, _should_exclude_file
    cfg = get_collection_config(name)
    files = [f for f in discover_files(_get_collection_paths(cfg))
             if not _should_exclude_file(f, cfg)]
    return [str(f) for f in files]


# ---------------------------------------------------------------------------
# Knowledge Graph: cross-link review + concept inspector
# ---------------------------------------------------------------------------
def crosslink_review_groups(status="pending_review", collection=None, direction="both", limit=100):
    """Cross-links grouped by target, with source/target names resolved in one query
    per collection (no N+1). direction: outgoing|incoming|both relative to `collection`."""
    where, params = ["status = %s"], [status]
    if collection and collection != "(all)":
        if direction == "outgoing":
            where.append("source_collection = %s"); params.append(collection)
        elif direction == "incoming":
            where.append("target_collection = %s"); params.append(collection)
        else:
            where.append("(source_collection = %s OR target_collection = %s)")
            params += [collection, collection]
    sql = f"""SELECT target_collection, target_identifier, match_type,
                     COUNT(*) AS n, AVG(confidence) AS avg_conf,
                     string_agg(source_identifier, '||' ORDER BY source_identifier) AS sources,
                     string_agg(source_collection, '||' ORDER BY source_identifier) AS source_cols
              FROM cross_links WHERE {' AND '.join(where)}
              GROUP BY target_collection, target_identifier, match_type
              ORDER BY n DESC, avg_conf DESC LIMIT %s"""
    params.append(limit)
    groups = fetchall(sql, tuple(params))

    need = {}
    for g in groups:
        need.setdefault(g["target_collection"], set()).add(g["target_identifier"])
        sids = (g["sources"] or "").split("||")
        scols = (g["source_cols"] or "").split("||")
        for i, sid in enumerate(sids[:10]):
            c = scols[i] if i < len(scols) else g["target_collection"]
            need.setdefault(c, set()).add(sid)

    name_map = {}
    for c, ids in need.items():
        idl = [i for i in ids if i]
        if not idl:
            continue
        for r in fetchall(
            """SELECT payload->>'identifier' AS ident, payload->>'source_file' AS sf,
                      payload->>'primary_name' AS name
               FROM chunks WHERE collection_name = %s
                 AND (payload->>'identifier' = ANY(%s) OR payload->>'source_file' = ANY(%s))""",
            (c, idl, idl),
        ):
            if r.get("ident"):
                name_map.setdefault((c, r["ident"]), r["name"])
            if r.get("sf"):
                name_map.setdefault((c, r["sf"]), r["name"])

    out = []
    for g in groups:
        sids = (g["sources"] or "").split("||")
        scols = (g["source_cols"] or "").split("||")
        srcs = []
        for i, sid in enumerate(sids[:10]):
            c = scols[i] if i < len(scols) else g["target_collection"]
            srcs.append({"collection": c, "id": sid, "name": name_map.get((c, sid)) or sid})
        out.append({
            "target_collection": g["target_collection"],
            "target_identifier": g["target_identifier"],
            "target_display": name_map.get((g["target_collection"], g["target_identifier"])) or g["target_identifier"],
            "match_type": g["match_type"],
            "n": g["n"],
            "avg_conf": float(g["avg_conf"] or 0),
            "sources": srcs,
            "more": max(0, len(sids) - 10),
        })
    return out


def set_crosslink_status(target_collection, target_identifier, match_type, from_status, to_status):
    execute("""UPDATE cross_links SET status = %s, updated_at = NOW()
               WHERE target_collection = %s AND target_identifier = %s
                 AND match_type = %s AND status = %s""",
            (to_status, target_collection, target_identifier, match_type, from_status))


def ignore_crosslink_term(term):
    """Add a term to generic_terms in doc_query_hints.json (cross-link noise filter)."""
    from core.paths import DOC_QUERY_HINTS_PATH
    try:
        with open(DOC_QUERY_HINTS_PATH, "r", encoding="utf-8") as f:
            hints = _json.load(f)
    except Exception:
        hints = {}
    terms = set(hints.get("generic_terms", []))
    terms.add(str(term).strip().lower())
    hints["generic_terms"] = sorted(terms)
    with open(DOC_QUERY_HINTS_PATH, "w", encoding="utf-8") as f:
        _json.dump(hints, f, indent=2)


def concept_clusters(collection, limit=300):
    return fetchall(
        """SELECT group_field, group_value, cluster_id,
                  LEFT(anchor_texts::text, 240) AS preview
           FROM concept_vectors WHERE collection = %s
           ORDER BY group_value, cluster_id LIMIT %s""", (collection, limit))


def truncation_report(cap=2500):
    """Per-collection embed-text length stats (catches silent truncation)."""
    return fetchall(
        """SELECT collection_name AS collection, COUNT(*) AS chunks,
                  MAX(LENGTH(payload->>'text')) AS max_chars,
                  SUM(CASE WHEN LENGTH(payload->>'text') > %s THEN 1 ELSE 0 END) AS over_cap
           FROM chunks WHERE payload->>'text' IS NOT NULL
           GROUP BY collection_name ORDER BY collection_name""", (cap,))


def crosslink_counts():
    return fetchall(
        """SELECT status, match_type, COUNT(*) AS n FROM cross_links
           GROUP BY status, match_type ORDER BY status, n DESC""", ())


def concept_counts():
    return fetchall(
        """SELECT collection, group_field, COUNT(*) AS n FROM concept_vectors
           GROUP BY collection, group_field ORDER BY collection""", ())


def crosslink_graph(status=None):
    """Collection-level graph: nodes = collections (sized by chunk count), edges =
    cross-link counts between distinct collections. Returns ECharts-ready structure."""
    cond = ["source_collection <> target_collection"]
    params = []
    if status and status != "(all)":
        cond.append("status = %s")
        params.append(status)
    rows = fetchall(
        f"""SELECT source_collection AS s, target_collection AS t, COUNT(*) AS n
            FROM cross_links WHERE {' AND '.join(cond)}
            GROUP BY s, t""", tuple(params))

    chunks = {d["name"]: d["chunks"] for d in collection_stats()}
    involved = set()
    for r in rows:
        involved.add(r["s"]); involved.add(r["t"])
    node_names = sorted(set(n for n, c in chunks.items() if c) | involved)

    max_chunks = max(list(chunks.values()) + [1])
    max_edge = max([r["n"] for r in rows], default=1)
    cats = [{"name": n} for n in node_names]
    nodes = [{
        "name": n,
        "value": chunks.get(n, 0),
        "symbolSize": 18 + 42 * (chunks.get(n, 0) / max_chunks),
        "category": node_names.index(n),
    } for n in node_names]
    links = [{
        "source": r["s"], "target": r["t"], "value": r["n"],
        "lineStyle": {"width": 1 + 5 * (r["n"] / max_edge), "curveness": 0.15},
        "label": {"show": True, "formatter": str(r["n"])},
    } for r in rows]
    return {"nodes": nodes, "links": links, "categories": cats,
            "legend": [n["name"] for n in cats]}
