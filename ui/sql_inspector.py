"""ui/sql_inspector.py — SQL Inspector tab.

Two ways to query the database directly:
  * Ask the data  — natural-language question -> guarded text-to-SQL (the same
    engine the router uses for analytics). Shows the generated SQL (editable),
    then runs it read-only and renders the result.
  * Raw SQL       — paste a read-only SELECT and run it through the same guards.

Everything is read-only: SELECT/WITH only, whitelisted tables, statement
timeout, row cap.
"""
from functools import partial

from nicegui import ui, run

from core.ui_data import collection_stats
from core.retrieval.analytics import (
    generate_sql, validate_sql, _ensure_limit, run_readonly_sql, format_result_text,
)

_ALL = "(all collections)"


def _result_table(container, cols, rows):
    container.clear()
    with container:
        if not cols:
            ui.label("Query ran but returned no columns.").classes("text-gray-500")
            return
        ui.label(f"{len(rows)} row(s)").classes("text-sm text-gray-600")
        columns = [{"name": c, "label": c, "field": c, "align": "left"} for c in cols]
        table_rows = [{c: ("" if r.get(c) is None else str(r.get(c))) for c in cols} for r in rows]
        ui.table(columns=columns, rows=table_rows, row_key=cols[0]).classes("w-full").props("dense")


def render_sql_inspector_panel():
    names = [r["name"] for r in collection_stats()]

    with ui.tabs().props("dense").classes("w-full") as sub:
        t_ask = ui.tab("Ask the data")
        t_raw = ui.tab("Raw SQL")
    with ui.tab_panels(sub, value=t_ask).classes("w-full"):
        with ui.tab_panel(t_ask):
            _ask_the_data(names)
        with ui.tab_panel(t_raw):
            _raw_sql(names)


def _ask_the_data(names):
    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select([_ALL] + names, value=(names[0] if names else _ALL),
                         label="Collection").props("outlined dense").classes("w-56")
        q = ui.input(placeholder="e.g. how many fits files are in this collection?"
                     ).props("outlined dense clearable").classes("flex-grow")
        gen_btn = ui.button("Generate SQL", on_click=lambda: do_generate()).props("unelevated")

    explain = ui.label("").classes("text-sm text-gray-600 mt-1")
    sql_box = ui.textarea(label="Generated SQL (editable)").props(
        "outlined autogrow").classes("w-full font-mono")
    sql_box.visible = False
    run_btn = ui.button("Run SQL", on_click=lambda: do_run()).props("unelevated")
    run_btn.visible = False
    out = ui.column().classes("w-full mt-2")

    def _collection_arg():
        return None if coll.value == _ALL else coll.value

    async def do_generate():
        if not (q.value or "").strip():
            ui.notify("Enter a question", type="warning")
            return
        explain.text = "Generating…"
        sql_box.visible = False
        run_btn.visible = False
        out.clear()
        gen = await run.io_bound(partial(generate_sql, q.value.strip(), _collection_arg()))
        if not gen.get("is_analytics"):
            explain.text = ("Not a metadata/aggregate question — "
                            f"{gen.get('explanation') or 'try the Ask or Chat tab instead.'}")
            return
        explain.text = gen.get("explanation") or ""
        sql_box.value = gen.get("sql") or ""
        sql_box.visible = True
        run_btn.visible = True

    async def do_run():
        sql = (sql_box.value or "").strip()
        ok, reason = validate_sql(sql)
        if not ok:
            out.clear()
            with out:
                ui.label(f"Blocked: {reason}").classes("text-red-600")
            return
        out.clear()
        with out:
            ui.spinner(size="lg")
        try:
            cols, rows = await run.io_bound(partial(run_readonly_sql, _ensure_limit(sql)))
        except Exception as exc:
            out.clear()
            with out:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        out.clear()
        with out:
            if len(rows) == 1 and len(cols) == 1:
                ui.markdown(f"### {format_result_text(cols, rows)}")
            _result_table(ui.column().classes("w-full"), cols, rows)

    q.on("keydown.enter", lambda: do_generate())


def _raw_sql(names):
    from core.sql_snippets import list_snippets, save_snippet, delete_snippet
    ui.label("Read-only SELECT / WITH only. Whitelisted tables, statement "
             "timeout, row cap.").classes("text-sm text-gray-600")

    # Saved-statement library: pick to load, Save stores the current box.
    with ui.row().classes("w-full items-center gap-2"):
        saved = ui.select({}, label="Saved statements", with_input=True
                          ).props("outlined dense clearable").classes("w-96")
        ui.button("Save current", on_click=lambda: _save_current()).props("flat")
        ui.button("Delete selected", on_click=lambda: _delete_selected()).props(
            "flat color=negative")

    sql_box = ui.textarea(
        label="SQL", placeholder="SELECT collection_name, COUNT(*) FROM files GROUP BY 1"
    ).props("outlined autogrow").classes("w-full font-mono")

    _snips = {}

    def _refresh_saved():
        _snips.clear()
        opts = {}
        for r in list_snippets():
            _snips[r["id"]] = r["sql"]
            opts[r["id"]] = r["label"]
        saved.set_options(opts)

    def _on_pick(e):
        if saved.value in _snips:
            sql_box.value = _snips[saved.value]
    saved.on_value_change(_on_pick)

    def _save_current():
        sql = (sql_box.value or "").strip()
        if not sql:
            ui.notify("Nothing to save", type="warning")
            return
        label = sql.replace("\n", " ")[:80]
        save_snippet(label, sql)
        _refresh_saved()
        ui.notify("Saved", type="positive")

    def _delete_selected():
        if saved.value in _snips:
            delete_snippet(saved.value)
            saved.value = None
            _refresh_saved()
            ui.notify("Deleted", type="warning")

    _refresh_saved()
    ui.button("Run SQL", on_click=lambda: do_run()).props("unelevated")
    out = ui.column().classes("w-full mt-2")

    async def do_run():
        sql = (sql_box.value or "").strip()
        ok, reason = validate_sql(sql)
        if not ok:
            out.clear()
            with out:
                ui.label(f"Blocked: {reason}").classes("text-red-600")
            return
        out.clear()
        with out:
            ui.spinner(size="lg")
        try:
            cols, rows = await run.io_bound(partial(run_readonly_sql, _ensure_limit(sql)))
        except Exception as exc:
            out.clear()
            with out:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        _result_table(out, cols, rows)
