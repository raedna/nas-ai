"""ui/ingestion.py — Ingestion tab: path check + scan, run ingest with per-file
results, live background-task status, and kill switch."""
import json
from functools import partial

from nicegui import ui, run

from core.ui_data import (
    collection_stats, run_ingest, background_tasks,
    collection_path_info, scan_collection_files,
)
from core.background_runner import cancel_running_tasks

_BG_COLUMNS = [
    {"name": "collection", "label": "Collection", "field": "collection", "align": "left"},
    {"name": "task_name", "label": "Task", "field": "task_name", "align": "left"},
    {"name": "status", "label": "Status", "field": "status", "align": "left"},
    {"name": "started_at", "label": "Started", "field": "started_at", "align": "left"},
    {"name": "finished_at", "label": "Finished", "field": "finished_at", "align": "left"},
]
_FILE_COLUMNS = [
    {"name": "path", "label": "Path", "field": "path", "align": "left"},
    {"name": "filetype", "label": "Filetype", "field": "filetype", "align": "left"},
    {"name": "success", "label": "Success", "field": "success", "align": "left"},
    {"name": "skipped", "label": "Skipped", "field": "skipped", "align": "left"},
    {"name": "chunks_created", "label": "Chunks", "field": "chunks_created"},
    {"name": "error", "label": "Error", "field": "error", "align": "left"},
    {"name": "metadata", "label": "Metadata", "field": "metadata", "align": "left"},
]
_METRICS = [
    ("total_files", "Files"), ("processed_files", "Processed"),
    ("skipped_files", "Skipped"), ("failed_files", "Failed"), ("total_chunks", "Chunks"),
]


def render_ingestion_panel():
    names = [r["name"] for r in collection_stats()]

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(names, label="Collection", with_input=True).props("outlined dense").classes("w-64")
        force = ui.checkbox("Force re-ingest")
    path_status = ui.label("").classes("text-sm")
    with ui.row().classes("gap-2"):
        ui.button("Path Check", on_click=lambda: do_path_check()).props("outline")
        ui.button("Scan Directory", on_click=lambda: do_scan()).props("outline")
        ui.button("Run Ingestion", on_click=lambda: do_run()).props("unelevated")
    scan_box = ui.column().classes("w-full")
    result_box = ui.column().classes("w-full mt-2")

    ui.separator().classes("my-4")
    with ui.row().classes("items-center w-full"):
        ui.label("Background Tasks").classes("text-lg font-medium")
        ui.space()
        ui.button(icon="refresh", on_click=lambda: refresh_status()).props("flat round dense")
        ui.button("🛑 Stop running", on_click=lambda: do_stop()).props("color=negative outline")
    bg_table = ui.table(columns=_BG_COLUMNS, rows=[], row_key="started_at").classes("w-full")

    def do_path_check():
        if not coll.value:
            ui.notify("Pick a collection", type="warning")
            return
        info = collection_path_info(coll.value)
        if not info["path"]:
            path_status.text = "⚠ no path configured"
            path_status.classes(replace="text-sm text-red-600")
        elif info["exists"]:
            kind = "folder" if info["is_dir"] else "file"
            path_status.text = f"✓ {info['path']} — exists ({kind})"
            path_status.classes(replace="text-sm text-green-700")
        else:
            path_status.text = f"✗ {info['path']} — not found"
            path_status.classes(replace="text-sm text-red-600")

    async def do_scan():
        if not coll.value:
            ui.notify("Pick a collection", type="warning")
            return
        scan_box.clear()
        with scan_box:
            ui.spinner()
        try:
            files = await run.io_bound(partial(scan_collection_files, coll.value))
        except Exception as exc:
            scan_box.clear()
            with scan_box:
                ui.label(f"Scan error: {exc}").classes("text-red-600")
            return
        import os
        counts = {}
        for f in files:
            ext = (os.path.splitext(f)[1] or "(none)").lower()
            counts[ext] = counts.get(ext, 0) + 1
        rows = [{"extension": k, "count": v} for k, v in sorted(counts.items())]
        scan_box.clear()
        with scan_box:
            ui.label(f"Files Detected — {len(files)} total across {len(rows)} types").classes(
                "text-sm font-medium")
            ui.table(columns=[
                {"name": "extension", "label": "extension", "field": "extension", "align": "left"},
                {"name": "count", "label": "count", "field": "count"},
            ], rows=rows, row_key="extension").classes("w-full").props("dense")

    async def do_run():
        if not coll.value:
            ui.notify("Pick a collection", type="warning")
            return
        result_box.clear()
        with result_box:
            ui.spinner(size="lg")
            ui.label(f"Ingesting {coll.value}…").classes("text-sm text-gray-500")
        try:
            res = await run.io_bound(partial(run_ingest, coll.value, force.value))
        except Exception as exc:
            result_box.clear()
            with result_box:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        res = res or {}
        result_box.clear()
        with result_box:
            ui.label("Ingestion complete.").classes("text-green-700 font-medium")
            with ui.row().classes("gap-8"):
                for key, lbl in _METRICS:
                    with ui.column().classes("items-center"):
                        ui.label(str(res.get(key, 0))).classes("text-xl font-bold")
                        ui.label(lbl).classes("text-xs text-gray-500")
            ui.label("⚙ Cross-link discovery + concept rebuild running in background.").classes(
                "text-sm text-blue-700")
            if res.get("_bg_error"):
                ui.label(f"(background launch issue: {res['_bg_error']})").classes("text-xs text-red-500")
            # per-file results
            file_rows = []
            for fr in res.get("results", []) or []:
                file_rows.append({
                    "path": str(getattr(fr, "path", "")),
                    "filetype": getattr(fr, "filetype_name", ""),
                    "success": "✓" if getattr(fr, "success", False) else "",
                    "skipped": "✓" if getattr(fr, "skipped", False) else "",
                    "chunks_created": getattr(fr, "chunks_created", 0),
                    "error": getattr(fr, "error", "") or "",
                    "metadata": json.dumps(getattr(fr, "metadata", {}) or {}),
                })
            if file_rows:
                ui.label("Per-file Results").classes("text-md font-medium mt-3")
                ui.table(columns=_FILE_COLUMNS, rows=file_rows, row_key="path").classes("w-full").props("dense")
        refresh_status()

    def do_stop():
        n = cancel_running_tasks()
        ui.notify(f"Cancelled {n} running task(s)", type="warning")
        refresh_status()

    def refresh_status():
        try:
            rows = background_tasks(15)
            for r in rows:
                r["started_at"] = str(r.get("started_at") or "")
                r["finished_at"] = str(r.get("finished_at") or "")
            bg_table.rows = rows
            bg_table.update()
        except RunTimeError:
            pass # tab was closed/reloaded mid-tick; nothing to update

    refresh_status()
    ui.timer(5.0, refresh_status)  # live auto-refresh while a build runs
