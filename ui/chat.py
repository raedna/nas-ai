"""ui/chat.py — Chat tab: multi-turn, auto-routed, inline images + related sections.
Memory M1: sessions persist in PostgreSQL — resume the latest on open, pick any
past session from the dropdown, every turn written as it happens."""
from functools import partial

from nicegui import ui, run

from core.ui_data import collection_stats
from core.chat_engine import chat_turn
from core.chat_store import (create_session, list_sessions, get_messages,
                             add_message, set_title_from_first_question,
                             delete_session)
from ui.render import render_answer, build_image_items, render_related_section


def _session_options():
    """{id: label} for the dropdown, most recent first."""
    out = {}
    for s in list_sessions():
        ts = s["updated_at"].strftime("%b %d %H:%M") if s.get("updated_at") else ""
        out[s["id"]] = f"{s['title']}  ({s['n_messages']} msg · {ts})"
    return out


def render_chat_panel():
    names = [r["name"] for r in collection_stats() if r["chunks"]]
    history = []  # engine-side history for the ACTIVE session
    state = {"session_id": None}

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(names, multiple=True,
                         label="Collections (empty = auto)").props("outlined dense").classes("w-96")
        sess = ui.select({}, label="Session").props("outlined dense").classes("w-80")
        ui.button("New chat", on_click=lambda: _new_session()).props("flat")
        ui.button("Delete", on_click=lambda: _delete_current()).props("flat color=negative")
        ui.space()

    log = ui.column().classes("w-full gap-2 mt-2")

    with ui.row().classes(
            "w-full items-center gap-2 sticky bottom-0 bg-white z-10 py-2"):
        msg = ui.input(placeholder="Ask anything…").props("outlined dense clearable").classes("flex-grow")
        send = ui.button("Send").props("unelevated")

    def _render_user(text):
        with log:
            with ui.card().classes("self-end bg-blue-50 max-w-2xl"):
                ui.label(text)

    def _render_stored_assistant(m):
        """Rehydrate a persisted assistant message (text + payload images)."""
        with log:
            card = ui.card().classes("w-full")
        payload = m.get("answer_payload")
        render_answer(card, m["content"], build_image_items(payload), show_ocr=True)
        with card:
            if m.get("collection"):
                ui.label(f"Source: {m['collection']}").classes("text-xs text-gray-500 mt-1")

    def _load_session(session_id):
        state["session_id"] = session_id
        history.clear()
        log.clear()
        for m in get_messages(session_id):
            history.append({"role": m["role"], "content": m["content"]})
            if m["role"] == "user":
                _render_user(m["content"])
            else:
                _render_stored_assistant(m)

    def _refresh_dropdown():
        opts = _session_options()
        sess.set_options(opts)
        sess.value = state["session_id"]

    def _new_session():
        sid = create_session()
        _load_session(sid)
        _refresh_dropdown()

    def _delete_current():
        if state["session_id"] is None:
            return
        delete_session(state["session_id"])
        _boot()

    def _on_pick(e):
        sid = sess.value
        if sid is not None and sid != state["session_id"]:
            _load_session(sid)

    sess.on_value_change(_on_pick)

    def _boot():
        """Resume the most recent session, or start fresh if none exist."""
        existing = list_sessions(limit=1)
        if existing:
            _load_session(existing[0]["id"])
        else:
            state["session_id"] = create_session()
            history.clear()
            log.clear()
        _refresh_dropdown()

    async def do_send():
        text = (msg.value or "").strip()
        if not text:
            return
        msg.value = ""
        _render_user(text)
        history.append({"role": "user", "content": text})
        add_message(state["session_id"], "user", text)
        set_title_from_first_question(state["session_id"], text)

        # Refresh per send: a collection created MID-SESSION (the first
        # memory) must be routable on the very next question.
        _names_now = [r["name"] for r in collection_stats() if r["chunks"]]
        avail = [c for c in (coll.value or [])] or _names_now
        with log:
            card = ui.card().classes("w-full")
            with card:
                ui.spinner()

        fn = partial(chat_turn, text, list(history), avail,
                     session_id=state["session_id"])
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
        add_message(state["session_id"], "assistant", content,
                    collection=(resp.get("collection") if isinstance(resp, dict) else None),
                    answer_payload=payload)
        _refresh_dropdown()  # updated_at + title changes

        kind = resp.get("answer_kind") if isinstance(resp, dict) else None
        raw = resp.get("raw_answer") if isinstance(resp, dict) else None
        if kind == "doc" and isinstance(raw, str) and raw.strip() and raw.strip() != content.strip():
            # Concise answer, then the full entry (with its images) in an expander.
            render_answer(card, content, [], show_ocr=True)
            with card:
                with ui.expansion("Show full entry").classes("w-full"):
                    full = ui.column().classes("w-full")
                    render_answer(full, raw, build_image_items(payload), show_ocr=True)
        else:
            render_answer(card, content, build_image_items(payload), show_ocr=True)
        with card:
            if isinstance(resp, dict) and resp.get("collection"):
                ui.label(f"Source: {resp['collection']} · {resp.get('method', '')}").classes(
                    "text-xs text-gray-500 mt-1")
            if isinstance(resp, dict) and resp.get("method") != "memory_capture":
                def _remember_answer(q=text, a=content):
                    from core.memory_store import remember
                    remember(f"Q: {q}\nA: {a[:500]}",
                             session_id=state["session_id"],
                             context_question=q, origin="button")
                    ui.notify("Saved to memory", type="positive")
                ui.button("Remember this", on_click=_remember_answer).props(
                    "flat dense size=sm icon=bookmark_add").classes("mt-1")
            for s in (resp.get("related_sections") if isinstance(resp, dict) else []) or []:
                label = (f"[{s.get('collection')}] {s.get('title')} · "
                         f"{s.get('match_type')} {float(s.get('confidence') or 0):.2f}")
                with ui.expansion(label).classes("w-full"):
                    render_related_section(s)

    send.on_click(do_send)
    msg.on("keydown.enter", do_send)
    _boot()
