"""ui/preview.py — Preview / Inspector tab.

Pick a collection, pull a sample of points, and see:
  * doc_type / source_type counts
  * distinct source files in the sample
  * a table of sample points (identifier, primary_name, doc_type, ..., preview text)
  * raw payloads (collapsible JSON)

Ported from the Streamlit "Preview / Inspector" tab (core/ui_app.py).
"""
import json
from functools import partial

from nicegui import ui, run

from core.ui_data import collection_stats
from core.retrieval.db_retrieval import scroll_collection


def render_preview_panel():
    names = [r["name"] for r in collection_stats()]

    if not names:
        ui.label("No collections found in PostgreSQL.").classes("text-gray-500")
        return

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(sorted(names), value=names[0], label="Collection").props(
            "outlined dense").classes("w-64")
        limit = ui.number(label="Sample size", value=25, min=1, max=200, step=1).props(
            "outlined dense").classes("w-40")
        load_btn = ui.button("Load Preview", on_click=lambda: do_load()).props("unelevated")

    out = ui.column().classes("w-full mt-2")

    async def do_load():
        load_btn.props("loading")
        out.clear()
        try:
            points = await run.io_bound(
                partial(scroll_collection, coll.value, limit=int(limit.value or 25)))
        except Exception as exc:
            out.clear()
            with out:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        finally:
            load_btn.props(remove="loading")

        out.clear()
        with out:
            if not points:
                ui.label("No points found in this collection.").classes("text-gray-500")
                return

            ui.label(f"Loaded {len(points)} point(s).").classes("text-sm text-gray-600")

            # -------------------------
            # Summary counts
            # -------------------------
            doc_type_counts, source_type_counts, source_files = {}, {}, set()
            for p in points:
                payload = p.payload or {}
                doc_type = str(payload.get("doc_type") or "unknown")
                source_type = str(payload.get("source_type") or "unknown")
                source_file = payload.get("source_file")
                doc_type_counts[doc_type] = doc_type_counts.get(doc_type, 0) + 1
                source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1
                if source_file:
                    source_files.add(str(source_file))

            with ui.row().classes("w-full gap-4"):
                with ui.column().classes("flex-grow"):
                    ui.label("Doc Type Counts").classes("font-bold")
                    ui.table(
                        columns=[
                            {"name": "doc_type", "label": "doc_type", "field": "doc_type", "align": "left"},
                            {"name": "count", "label": "count", "field": "count"},
                        ],
                        rows=[{"doc_type": k, "count": v} for k, v in sorted(doc_type_counts.items())],
                        row_key="doc_type",
                    ).classes("w-full").props("dense")
                with ui.column().classes("flex-grow"):
                    ui.label("Source Type Counts").classes("font-bold")
                    ui.table(
                        columns=[
                            {"name": "source_type", "label": "source_type", "field": "source_type", "align": "left"},
                            {"name": "count", "label": "count", "field": "count"},
                        ],
                        rows=[{"source_type": k, "count": v} for k, v in sorted(source_type_counts.items())],
                        row_key="source_type",
                    ).classes("w-full").props("dense")

            with ui.expansion(f"Source Files ({len(source_files)})").classes("w-full mt-2"):
                ui.table(
                    columns=[{"name": "source_file", "label": "source_file", "field": "source_file", "align": "left"}],
                    rows=[{"source_file": x} for x in sorted(source_files)],
                    row_key="source_file",
                ).classes("w-full").props("dense")

            # -------------------------
            # Sample payload preview
            # -------------------------
            preview_rows = []
            for i, p in enumerate(points, start=1):
                payload = p.payload or {}
                preview_text = str(payload.get("text") or payload.get("description") or "").strip()
                preview_text = preview_text.replace("\n", " ")[:300]
                preview_rows.append({
                    "rank": i,
                    "identifier": payload.get("identifier"),
                    "primary_name": payload.get("primary_name"),
                    "doc_type": payload.get("doc_type"),
                    "source_type": payload.get("source_type"),
                    "source_file": payload.get("source_file"),
                    "page_num": payload.get("page_num"),
                    "related_identifiers": str(payload.get("related_identifiers") or ""),
                    "preview": preview_text,
                })

            ui.label("Sample Points").classes("font-bold mt-2")
            ui.table(
                columns=[
                    {"name": "rank", "label": "#", "field": "rank"},
                    {"name": "identifier", "label": "identifier", "field": "identifier", "align": "left"},
                    {"name": "primary_name", "label": "primary_name", "field": "primary_name", "align": "left"},
                    {"name": "doc_type", "label": "doc_type", "field": "doc_type", "align": "left"},
                    {"name": "source_type", "label": "source_type", "field": "source_type", "align": "left"},
                    {"name": "source_file", "label": "source_file", "field": "source_file", "align": "left"},
                    {"name": "page_num", "label": "page_num", "field": "page_num"},
                    {"name": "related_identifiers", "label": "related_identifiers", "field": "related_identifiers", "align": "left"},
                    {"name": "preview", "label": "preview", "field": "preview", "align": "left"},
                ],
                rows=preview_rows,
                row_key="rank",
                pagination=25,
            ).classes("w-full").props("dense")

            with ui.expansion("Raw Payloads").classes("w-full mt-2"):
                raw = [{"id": p.id, "payload": p.payload} for p in points]
                ui.code(json.dumps(raw, indent=2, ensure_ascii=False, default=str),
                         language="json").classes("w-full max-h-96 overflow-auto")

    limit.on("keydown.enter", lambda: do_load())