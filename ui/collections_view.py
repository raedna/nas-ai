"""ui/collections_view.py — Collections admin: stats, full create/update form,
delete, build links. (Named *_view to avoid shadowing stdlib `collections`.)"""
import json
from functools import partial

from nicegui import ui, run

from core.ui_data import (
    collection_stats, get_collection_config, upsert_collection_config,
    delete_collection, rebuild_links,
)
from core.paths import FILETYPES_PATH

_COLUMNS = [
    {"name": "name", "label": "Collection", "field": "name", "align": "left", "sortable": True},
    {"name": "chunks", "label": "Chunks", "field": "chunks", "sortable": True},
    {"name": "enums", "label": "Enums", "field": "enums", "sortable": True},
]


def _filetype_options():
    try:
        with open(FILETYPES_PATH, "r", encoding="utf-8") as f:
            return sorted(json.load(f).keys())
    except Exception:
        return ["tables", "docs", "pdf", "xml", "image", "astro"]


def _csv(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _lines(s):
    return [x.strip() for x in (s or "").splitlines() if x.strip()]


def render_collections_panel():
    ftypes = _filetype_options()
    holder = {"names": [r["name"] for r in collection_stats()]}

    # ---- stats table ----
    with ui.row().classes("items-center w-full"):
        ui.label("Collections").classes("text-lg font-medium")
        ui.space()
        ui.button(icon="refresh", on_click=lambda: refresh_all()).props("flat round dense")
    status = ui.label("").classes("text-sm text-gray-500")
    table = ui.table(columns=_COLUMNS, rows=[], row_key="name").classes("w-full")

    # ---- create / edit ----
    ui.separator().classes("my-3")
    ui.label("Create / Edit Collection").classes("text-lg font-medium")
    load_sel = ui.select(holder["names"], label="Load existing collection (optional)",
                         with_input=True).props("outlined dense clearable").classes("w-full")
    name_in = ui.input("Collection name").props('outlined dense hint="Internal name — letters/digits/underscores. Also a routing signal: questions naming it anchor here."').classes("w-full")
    path_in = ui.input("Path (file or folder)").props('outlined dense hint="Folder (or single file) ingestion scans. New/changed files are picked up on each run; unchanged files skip by hash."').classes("w-full")
    ft_in = ui.select(ftypes, multiple=True, label="Allowed filetypes").props('outlined dense hint="Which pipelines may process files here (docs=md/txt/docx, tables=csv/xlsx, halo=ticket JSONs, ...). A file must match BOTH a filetype and the extensions filter."').classes("w-full")
    allow_ext = ui.input("Allowed extensions (comma-separated, include dots)").props('outlined dense hint="Extra extension filter, e.g. .json, .md — leave empty to accept everything the filetypes allow."').classes("w-full")
    excl_dirs = ui.input("Exclude folders (comma-separated)").props('outlined dense hint="Subfolder names to skip, e.g. archive, .obsidian, assets."').classes("w-full")
    excl_ext = ui.input("Exclude extensions (comma-separated, include dots)").props('outlined dense hint="Extensions to skip even if a filetype matches, e.g. .tmp, .bak."').classes("w-full")
    asset_roots = ui.textarea("Asset search roots (one path per line)").props('outlined dense hint="Folders searched to resolve media referenced BY NAME in documents (e.g. Obsidian x_Media for ![[image.png]]). Not needed when content stores full paths (halo)."').classes("w-full")

    ui.label("Field / Row Filters").classes("text-md font-medium mt-2")
    enable_filters = ui.checkbox("Enable field filters")
    f_field = ui.input("Field / column name").props("outlined dense").classes("w-full")
    f_mode = ui.select(["exclude_equals", "include_equals"], value="exclude_equals",
                       label="Filter mode").props("outlined dense").classes("w-full")
    f_values = ui.input("Values (comma-separated)").props("outlined dense").classes("w-full")

    label_in = ui.input("Source label (optional)").props("outlined dense").classes("w-full")
    desc_in = ui.textarea("Routing description (required)").props('outlined dense hint="What this collection KNOWS — used by chat routing to decide when to search here. Write it like an answer to: what questions should come to this data?"').classes("w-full")
    notes_in = ui.textarea("Notes").props("outlined dense").classes("w-full")
    save_msg = ui.label("").classes("text-sm")
    ui.button("Save Collection", on_click=lambda: save()).props("unelevated")

    # ---- build cross-links / concept vectors ----
    ui.separator().classes("my-3")
    with ui.row().classes("items-center gap-2"):
        ui.label("Build Cross-Links + Concept Vectors").classes("text-md font-medium")
        build_sel = ui.select(holder["names"], label="Collection").props("outlined dense").classes("w-64")
        ui.button("Build", on_click=lambda: do_build()).props("outline")

    # ---- delete ----
    ui.separator().classes("my-3")
    ui.label("Delete Collection").classes("text-lg font-medium text-red-700")
    with ui.row().classes("items-center gap-2"):
        del_sel = ui.select(holder["names"], label="Collection").props("outlined dense").classes("w-64")
        del_cfg = ui.checkbox("Also delete config", value=True)
        del_confirm = ui.checkbox("Confirm", value=False)
        ui.button("Delete", on_click=lambda: do_delete()).props("color=negative outline")

    # ---- behaviors ----
    def refresh_all():
        rows = collection_stats()
        table.rows = rows
        table.update()
        status.text = f"{len(rows)} collections · {sum(r['chunks'] for r in rows):,} chunks"
        holder["names"] = [r["name"] for r in rows]
        for sel in (load_sel, build_sel, del_sel):
            sel.options = holder["names"]
            sel.update()

    def load_into_form(nm):
        cfg = get_collection_config(nm or "")
        if not cfg:
            return
        name_in.value = nm
        path_in.value = cfg.get("path", "")
        ft_in.value = cfg.get("allowed_filetypes", []) or []
        allow_ext.value = ", ".join(cfg.get("allowed_extensions", []) or [])
        excl_dirs.value = ", ".join(cfg.get("exclude_dirs", cfg.get("exclude_folders", [])) or [])
        excl_ext.value = ", ".join(cfg.get("exclude_extensions", []) or [])
        asset_roots.value = "\n".join(cfg.get("asset_search_roots", []) or [])
        label_in.value = cfg.get("source_label", "")
        desc_in.value = cfg.get("routing_description", "")
        notes_in.value = cfg.get("notes", "")
        ff = (cfg.get("filters", {}) or {}).get("field_filters", [])
        if ff:
            enable_filters.value = True
            f_field.value = ff[0].get("field", "")
            f_mode.value = ff[0].get("mode", "exclude_equals")
            f_values.value = ", ".join(ff[0].get("values", []) or [])
        else:
            enable_filters.value = False
            f_field.value = f_values.value = ""
        save_msg.text = f"Loaded '{nm}'."

    load_sel.on_value_change(lambda e: load_into_form(e.value))

    def save():
        nm = (name_in.value or "").strip()
        if not nm or not (path_in.value or "").strip():
            ui.notify("Name and path are required", type="warning")
            return
        cfg = {
            "path": path_in.value.strip(),
            "allowed_filetypes": ft_in.value or [],
            "allowed_extensions": _csv(allow_ext.value),
            "exclude_dirs": _csv(excl_dirs.value),
            "exclude_extensions": _csv(excl_ext.value),
            "asset_search_roots": _lines(asset_roots.value),
            "source_label": (label_in.value or "").strip(),
            "routing_description": (desc_in.value or "").strip(),
            "notes": (notes_in.value or "").strip(),
        }
        if enable_filters.value and (f_field.value or "").strip() and _csv(f_values.value):
            cfg["filters"] = {"field_filters": [{
                "field": f_field.value.strip(),
                "mode": f_mode.value,
                "values": _csv(f_values.value),
            }]}
        err = upsert_collection_config(nm, cfg)
        save_msg.text = err or f"Saved '{nm}'."
        refresh_all()

    async def do_build():
        if not build_sel.value:
            return
        ui.notify(f"Launching build for {build_sel.value}…")
        await run.io_bound(partial(rebuild_links, build_sel.value))
        ui.notify("Build running in background — see Ingestion tab.", type="positive")

    def do_delete():
        if not del_sel.value:
            return
        if not del_confirm.value:
            ui.notify("Tick Confirm before deleting", type="warning")
            return
        delete_collection(del_sel.value, drop_config=del_cfg.value)
        ui.notify(f"Deleted {del_sel.value}", type="positive")
        del_confirm.value = False
        refresh_all()

    refresh_all()
