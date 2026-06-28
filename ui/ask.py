"""ui/ask.py — Ask tab: single-collection query with inline images + related sections."""
from functools import partial

from nicegui import ui, run

from core.ui_data import collection_stats
from core.retrieval.router import run_query_with_method
from ui.render import render_answer, build_image_items


def _related(container, sections):
    container.clear()
    if not sections:
        return
    with container:
        ui.label("Related from other collections").classes("text-sm font-medium mt-3")
        for s in sections:
            label = (f"[{s.get('collection')}] {s.get('title')} · "
                     f"{s.get('match_type')} {float(s.get('confidence') or 0):.2f}")
            with ui.expansion(label).classes("w-full"):
                ui.markdown(str(s.get("preview") or ""))


def render_ask_panel():
    names = [r["name"] for r in collection_stats() if r["chunks"]]
    default = names[0] if names else None

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(names, value=default, label="Collection").props("outlined dense").classes("w-64")
        q = ui.input(placeholder="Ask a question…").props("outlined dense clearable").classes("flex-grow")
        ask_btn = ui.button("Ask").props("unelevated")
    with ui.row().classes("gap-4 items-center"):
        show_links = ui.checkbox("Exact cross-links", value=True)
        show_topics = ui.checkbox("Related topics", value=True)
        meta = ui.label("").classes("text-xs text-gray-500")

    answer_box = ui.column().classes("w-full mt-2")
    related_box = ui.column().classes("w-full")

    async def do_ask():
        if not coll.value or not (q.value or "").strip():
            ui.notify("Pick a collection and enter a question", type="warning")
            return
        related_box.clear()
        answer_box.clear()
        with answer_box:
            ui.spinner(size="lg")
        meta.text = "…"
        fn = partial(run_query_with_method, coll.value, q.value.strip(),
                     show_exact_links=show_links.value, show_related_topics=show_topics.value)
        try:
            resp = await run.io_bound(fn)
        except Exception as exc:
            answer_box.clear()
            with answer_box:
                ui.label(f"Error: {exc}").classes("text-red-600")
            meta.text = ""
            return

        result = resp.get("result") if isinstance(resp, dict) else resp
        meta.text = f"method: {resp.get('method', '?')}" if isinstance(resp, dict) else ""
        payload = resp.get("answer_payload") if isinstance(resp, dict) else None
        render_answer(answer_box, result, build_image_items(payload), show_ocr=True)
        _related(related_box, resp.get("related_sections") if isinstance(resp, dict) else [])

    ask_btn.on_click(do_ask)
    q.on("keydown.enter", do_ask)
