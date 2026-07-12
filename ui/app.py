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

from nicegui import ui, app  # noqa: E402
from ui.ask import render_ask_panel  # noqa: E402
from ui.chat import render_chat_panel  # noqa: E402
from ui.collections_view import render_collections_panel  # noqa: E402
from ui.ingestion import render_ingestion_panel  # noqa: E402
from ui.knowledge_graph import render_kg_panel  # noqa: E402
from ui.debug import render_debug_panel  # noqa: E402
from ui.sql_inspector import render_sql_inspector_panel  # noqa: E402
from ui.analysis import render_analysis_panel
from ui.validation import render_validation_panel  # noqa: E402
from ui.system_config import render_system_config_panel  # noqa: E402
from ui.data_prep import render_data_prep_panel  # noqa: E402
from ui import entry_page  # noqa: E402,F401  (registers the /entry/{chunk_id} full-article page)

# Tab order — query tabs first, admin next, debug/remaining after.
TABS = [
    "Collections", "Ask", "Chat", "Analysis", "Ingestion", "Knowledge Graph", "Validation",
    "SQL Inspector", "System Config", "Data Prep", "Debug",
]


@ui.page("/")
def index():
    ui.colors(primary="#1F4E79")
    # Remember the last-selected tab across reloads/reconnects so a dropped
    # websocket (switching apps / laptop sleep) doesn't dump the user back on
    # Collections. Stored per-user in a signed cookie via app.storage.user.
    saved_tab = app.storage.user.get("active_tab", "Collections")
    if saved_tab not in TABS:
        saved_tab = "Collections"

    # Tabs live INSIDE the fixed header so they stay visible while scrolling a tab.
    with ui.header().classes("items-center q-pa-sm"):
        ui.label("NAS AI").classes("text-xl font-bold")
        ui.label("Offline AI Knowledge Retrieval").classes("text-xs opacity-70 ml-2 mr-4")
        with ui.tabs().props("inline-label dense") as tabs:
            tab_refs = {name: ui.tab(name) for name in TABS}
    tabs.on_value_change(lambda e: app.storage.user.update(active_tab=e.value))

    with ui.tab_panels(tabs, value=tab_refs[saved_tab]).classes("w-full"):
        for name in TABS:
            with ui.tab_panel(tab_refs[name]):
                if name == "Collections":
                    render_collections_panel()
                elif name == "Ask":
                    render_ask_panel()
                elif name == "Chat":
                    render_chat_panel()
                elif name == "Analysis":
                    render_analysis_panel()
                elif name == "Ingestion":
                    render_ingestion_panel()
                elif name == "Knowledge Graph":
                    render_kg_panel()
                elif name == "SQL Inspector":
                    render_sql_inspector_panel()
                elif name == "Validation":
                    render_validation_panel()
                elif name == "System Config":
                    render_system_config_panel()
                elif name == "Data Prep":
                    render_data_prep_panel()
                elif name == "Debug":
                    render_debug_panel()
                else:
                    ui.label(f"{name} — coming soon").classes("text-gray-500 p-4")


# storage_secret enables app.storage.user (signed cookie).
# reconnect_timeout is generous so briefly switching away from the tab doesn't
# trigger a full page reload when the websocket reconnects.
ui.run(title="NAS AI", port=int(os.environ.get("NASAI_PORT", 8080)), reload=False, show=True,
       storage_secret="nas-ai-local-ui", reconnect_timeout=600)
