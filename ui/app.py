"""
ui/app.py — NiceGUI front-end for NAS-AI (Phase 1: skeleton).

Coexists with the Streamlit app; reuses core/ logic. Run from the repo root:

    python ui/app.py

then open http://localhost:8080 . Streamlit is unaffected.
"""
import os
import sys

# Allow `python ui/app.py` from anywhere by putting the repo root on the path.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui  # noqa: E402
from core.ui_data import collection_stats  # noqa: E402
from ui.ask import render_ask_panel  # noqa: E402
from ui.chat import render_chat_panel  # noqa: E402

# Tab order — query tabs first (built next), admin/debug after. Only "Collections"
# is live in this skeleton; the rest are placeholders we fill in per phase.
TABS = [
    "Collections", "Ask", "Chat", "Ingestion", "Validation",
    "Preview", "SQL Inspector", "System Config", "Data Prep", "Debug",
]

_COLLECTION_COLUMNS = [
    {"name": "name", "label": "Collection", "field": "name", "align": "left", "sortable": True},
    {"name": "chunks", "label": "Chunks", "field": "chunks", "sortable": True},
    {"name": "enums", "label": "Enums", "field": "enums", "sortable": True},
]


def collections_panel():
    """Live collections list with chunk/enum counts — proves the core/ + DB wiring."""
    with ui.row().classes("items-center w-full"):
        ui.label("Collections").classes("text-lg font-medium")
        ui.space()
        refresh = ui.button(icon="refresh").props("flat round dense")
    status = ui.label("").classes("text-sm text-gray-500")
    table = ui.table(columns=_COLLECTION_COLUMNS, rows=[], row_key="name").classes("w-full")

    def load():
        try:
            rows = collection_stats()
            table.rows = rows
            table.update()
            total_chunks = sum(r["chunks"] for r in rows)
            status.text = f"{len(rows)} collections · {total_chunks:,} chunks total"
        except Exception as exc:  # surface DB issues instead of a blank table
            table.rows = []
            table.update()
            status.text = f"⚠ could not load collections: {exc}"

    refresh.on_click(load)
    load()


@ui.page("/")
def index():
    ui.colors(primary="#1F4E79")
    with ui.header().classes("items-center"):
        ui.label("NAS AI").classes("text-2xl font-bold")
        ui.label("Offline AI Knowledge Retrieval").classes("text-sm opacity-70 ml-3")

    with ui.tabs().classes("w-full") as tabs:
        tab_refs = {name: ui.tab(name) for name in TABS}

    with ui.tab_panels(tabs, value=tab_refs["Collections"]).classes("w-full"):
        for name in TABS:
            with ui.tab_panel(tab_refs[name]):
                if name == "Collections":
                    collections_panel()
                elif name == "Ask":
                    render_ask_panel()
                elif name == "Chat":
                    render_chat_panel()
                else:
                    ui.label(f"{name} — coming soon").classes("text-gray-500 p-4")


ui.run(title="NAS AI", port=8080, reload=False, show=True)
