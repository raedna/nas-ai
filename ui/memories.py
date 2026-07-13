"""ui/memories.py — Memories tab: everything NAS-AI has been asked to
remember, with provenance, and a Forget button per note. Read/delete only —
memories are CREATED in chat (capture triggers or the Remember button)."""
from nicegui import ui

from core.memory_store import list_memories, forget


def render_memories_panel():
    header = ui.row().classes("w-full items-center gap-2")
    with header:
        title = ui.label().classes("text-lg font-bold")
        ui.space()
        ui.button("Refresh", on_click=lambda: _refresh()).props("flat")

    box = ui.column().classes("w-full gap-2 mt-2")

    def _refresh():
        box.clear()
        rows = list_memories()
        title.set_text(f"Memories ({len(rows)})")
        if not rows:
            with box:
                ui.label("Nothing remembered yet — use \"remember that ...\" "
                         "or the Remember-this button in Chat.").classes(
                    "text-gray-500")
            return
        for m in rows:
            with box:
                with ui.card().classes("w-full"):
                    with ui.row().classes("w-full items-start"):
                        with ui.column().classes("flex-grow gap-0"):
                            ui.label(m["primary_name"]).classes("font-medium")
                            ui.label(
                                f"{m.get('told_at') or ''} · "
                                f"{m.get('origin') or 'chat'} · id {m['identifier']}"
                            ).classes("text-xs text-gray-500")
                        def _forget(ident=m["identifier"]):
                            n = forget(ident)
                            ui.notify(f"Forgotten ({n} note)", type="warning")
                            _refresh()
                        ui.button("Forget", on_click=_forget).props(
                            "flat dense color=negative icon=delete")

    _refresh()
