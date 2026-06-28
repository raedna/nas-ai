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
    names = sorted(set(cmap) | set(emap) | set(list_registered_collections()))
    return [{"name": n, "chunks": cmap.get(n, 0), "enums": emap.get(n, 0)} for n in names]


def background_tasks(limit=15):
    """Recent background tasks (cross-link discovery / concept rebuild)."""
    return fetchall(
        """SELECT collection, task_name, status, started_at, finished_at
           FROM background_tasks ORDER BY id DESC LIMIT %s""", (limit,))
