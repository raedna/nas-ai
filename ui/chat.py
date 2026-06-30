"""ui/chat.py — Chat tab: multi-turn, auto-routed, inline images + related sections."""
from functools import partial

from nicegui import ui, run

from core.ui_data import collection_stats
from core.chat_engine import chat_turn
from ui.render import render_answer, build_image_items


def render_chat_panel():
    names = [r["name"] for r in collection_stats() if r["chunks"]]
    history = []  # per-page session

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(names, multiple=True,
                         label="Collections (empty = auto)").props("outlined dense").classes("w-96")
        ui.space()
        ui.button("Clear", on_click=lambda: (_clear())).props("flat")

    log = ui.column().classes("w-full gap-2 mt-2")

    with ui.row().classes("w-full items-center gap-2"):
        msg = ui.input(placeholder="Ask anything…").props("outlined dense clearable").classes("flex-grow")
        send = ui.button("Send").props("unelevated")

    def _clear():
        history.clear()
        log.clear()

    async def do_send():
        text = (msg.value or "").strip()
        if not text:
            return
        msg.value = ""
        with log:
            with ui.card().classes("self-end bg-blue-50 max-w-2xl"):
                ui.label(text)
        history.append({"role": "user", "content": text})

        avail = [c for c in (coll.value or [])] or names
        with log:
            card = ui.card().classes("w-full")
            with card:
                ui.spinner()

        fn = partial(chat_turn, text, list(history), avail)
        try:
            resp = await run.io_bound(fn)
        except Exception as exc:
            card.clear()
            with card:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return

        content = resp.get("content", "") if isinstance(resp, dict) else str(resp)
        if not isinstance(content, str):
            content = str(content)  # never store non-string in history (slice-safety)
        payload = resp.get("answer_payload") if isinstance(resp, dict) else None
        history.append({"role": "assistant", "content": content})

        render_answer(card, content, build_image_items(payload), show_ocr=True)
        with card:
            if isinstance(resp, dict) and resp.get("collection"):
                ui.label(f"Source: {resp['collection']} · {resp.get('method', '')}").classes(
                    "text-xs text-gray-500 mt-1")
            for s in (resp.get("related_sections") if isinstance(resp, dict) else []) or []:
                label = (f"[{s.get('collection')}] {s.get('title')} · "
                         f"{s.get('match_type')} {float(s.get('confidence') or 0):.2f}")
                with ui.expansion(label).classes("w-full"):
                    ui.markdown(str(s.get("preview") or ""))

    send.on_click(do_send)
    msg.on("keydown.enter", do_send)
