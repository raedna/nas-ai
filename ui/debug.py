"""ui/debug.py — Debug tab: per-query candidate/rerank inspection + diagnostics."""
from functools import partial

from nicegui import ui, run

from core.ui_data import (
    collection_stats, truncation_report, crosslink_counts, concept_counts, background_tasks,
)
from core.retrieval.router import debug_route_query, run_query_with_method
from core.retrieval.reranker import score_point_shared
from ui.render import render_answer


def _points_to_rows(points, question=None, n=25):
    rows = []
    for i, p in enumerate(points or [], 1):
        if i > n:
            break
        pl = p.payload or {}
        preview = str(pl.get("text") or pl.get("description") or "").strip().replace("\n", " ")[:200]
        row = {
            "rank": i,
            "score": round(float(getattr(p, "score", 0) or 0), 4),
            "identifier": pl.get("identifier"),
            "primary_name": pl.get("primary_name"),
            "doc_type": pl.get("doc_type"),
            "source_file": pl.get("source_file"),
            "preview": preview,
        }
        if question is not None:
            try:
                row["rerank"] = round(score_point_shared(p, question), 3)
            except Exception:
                row["rerank"] = None
        rows.append(row)
    return rows


def render_debug_panel():
    names = [r["name"] for r in collection_stats()]

    with ui.tabs().props("dense").classes("w-full") as sub:
        t_query = ui.tab("Query Debug")
        t_diag = ui.tab("Diagnostics")
    with ui.tab_panels(sub, value=t_query).classes("w-full"):
        with ui.tab_panel(t_query):
            _query_debug(names)
        with ui.tab_panel(t_diag):
            _diagnostics(names)


def _query_debug(names):
    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(names, value=(names[0] if names else None),
                         label="Collection").props("outlined dense").classes("w-56")
        q = ui.input(placeholder="Query to debug…").props("outlined dense clearable").classes("flex-grow")
        ui.button("Debug", on_click=lambda: do_debug()).props("unelevated")
    out = ui.column().classes("w-full mt-2")

    async def do_debug():
        if not coll.value or not (q.value or "").strip():
            ui.notify("Pick a collection and enter a query", type="warning")
            return
        out.clear()
        with out:
            ui.spinner(size="lg")
        try:
            dbg = await run.io_bound(partial(debug_route_query, coll.value, q.value.strip(), 25))
            ans = await run.io_bound(partial(run_query_with_method, coll.value, q.value.strip()))
        except Exception as exc:
            out.clear()
            with out:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        out.clear()
        merged = _points_to_rows(dbg.get("merged_points"), n=25)
        ranked = _points_to_rows(dbg.get("ranked_points"), question=q.value.strip(), n=25)
        cols_base = [
            {"name": "rank", "label": "#", "field": "rank"},
            {"name": "score", "label": "score", "field": "score"},
            {"name": "identifier", "label": "identifier", "field": "identifier", "align": "left"},
            {"name": "primary_name", "label": "primary_name", "field": "primary_name", "align": "left"},
            {"name": "doc_type", "label": "doc_type", "field": "doc_type", "align": "left"},
            {"name": "source_file", "label": "source_file", "field": "source_file", "align": "left"},
            {"name": "preview", "label": "preview", "field": "preview", "align": "left"},
        ]
        ranked_cols = cols_base[:2] + [{"name": "rerank", "label": "rerank", "field": "rerank"}] + cols_base[2:]
        with out:
            ui.label(f"method: {ans.get('method', '?')} · reason: {ans.get('reason', '')}").classes(
                "text-sm text-gray-600")
            with ui.expansion(f"Merged Candidates Before Rerank ({len(merged)})").classes("w-full"):
                ui.table(columns=cols_base, rows=merged, row_key="rank").classes("w-full").props("dense")
            with ui.expansion(f"Final Reranked Candidates ({len(ranked)})", value=True).classes("w-full"):
                ui.table(columns=ranked_cols, rows=ranked, row_key="rank").classes("w-full").props("dense")
            with ui.expansion("Returned Answer", value=True).classes("w-full"):
                ans_box = ui.column().classes("w-full")
                render_answer(ans_box, ans.get("result"), show_ocr=False)


def _diagnostics(names):
    ui.button("Refresh diagnostics", on_click=lambda: load()).props("outline")
    box = ui.column().classes("w-full gap-3 mt-2")

    def _table(title, rows, columns):
        ui.label(title).classes("text-md font-medium mt-2")
        ui.table(columns=columns, rows=rows, row_key=columns[0]["field"]).classes("w-full").props("dense")

    def load():
        box.clear()
        with box:
            tr = [dict(r) for r in truncation_report()]
            _table("Truncation (chars per collection, cap 2500)", tr, [
                {"name": "collection", "label": "collection", "field": "collection", "align": "left"},
                {"name": "chunks", "label": "chunks", "field": "chunks"},
                {"name": "max_chars", "label": "max_chars", "field": "max_chars"},
                {"name": "over_cap", "label": "over_cap", "field": "over_cap"},
            ])
            cc = [dict(r) for r in crosslink_counts()]
            _table("Cross-links by status / match_type", cc, [
                {"name": "status", "label": "status", "field": "status", "align": "left"},
                {"name": "match_type", "label": "match_type", "field": "match_type", "align": "left"},
                {"name": "n", "label": "n", "field": "n"},
            ])
            cv = [dict(r) for r in concept_counts()]
            _table("Concept vectors by collection / group_field", cv, [
                {"name": "collection", "label": "collection", "field": "collection", "align": "left"},
                {"name": "group_field", "label": "group_field", "field": "group_field", "align": "left"},
                {"name": "n", "label": "n", "field": "n"},
            ])
            bg = []
            for r in background_tasks(10):
                bg.append({"collection": r.get("collection"), "status": r.get("status"),
                           "started_at": str(r.get("started_at") or ""),
                           "finished_at": str(r.get("finished_at") or "")})
            _table("Recent background tasks", bg, [
                {"name": "collection", "label": "collection", "field": "collection", "align": "left"},
                {"name": "status", "label": "status", "field": "status", "align": "left"},
                {"name": "started_at", "label": "started", "field": "started_at", "align": "left"},
                {"name": "finished_at", "label": "finished", "field": "finished_at", "align": "left"},
            ])

    load()
