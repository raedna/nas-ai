"""ui/validation.py — Validation tab.

Two sections:
  1. Schema Info & Overrides — pick a schema (DB or disk), view it, edit the
     field-role assignments (identifier, primary_name, aliases, ...), save,
     see basic validation warnings, optionally delete.
  2. Payload Inspector — pick a collection, sample/search/lookup payloads,
     see doc_type / source_type counts, source files, a results table, and
     raw JSON.

Ported and merged from the Streamlit "Validation" and "Preview / Inspector"
tabs (core/ui_app.py).
"""
import json
from functools import partial

from nicegui import ui, run

from core.paths import SCHEMAS_DIR, SCHEMA_OVERRIDES_PATH
from core.ui_data import collection_stats
from core.schema_inference import (
    ensure_schemas_table, list_schemas_from_db, load_schema_from_db,
    save_schema_to_db, delete_schema_from_db,
)
from core.retrieval.db_retrieval import (
    scroll_collection, get_by_identifier, search_bm25,
)

_SUBTYPES = ["", "definition", "enum_values", "relationship", "structured"]


def _load_json(path, default_obj):
    if not path.exists():
        return default_obj
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_obj


def _save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def render_validation_panel():
    _schema_section()
    ui.separator().classes("my-4")
    _payload_inspector_section()


# ===========================================================================
# Section 1: Schema Info & Overrides
# ===========================================================================
def _schema_section():
    ui.label("Schema Info & Overrides").classes("text-lg font-bold")
    container = ui.column().classes("w-full")

    def build():
        container.clear()
        with container:
            _build_schema_section(build)

    build()


def _build_schema_section(rebuild_cb):
    ensure_schemas_table()
    db_schemas = list_schemas_from_db()
    disk_schemas = sorted(SCHEMAS_DIR.glob("*_schema.json")) if SCHEMAS_DIR.exists() else []

    schema_options = [f"{r['collection_name']}/{r['source_file_stem']} [DB]" for r in db_schemas]
    for f in disk_schemas:
        label = f"{f.name} [disk]"
        if not any(f.stem.replace("_schema", "") in s for s in schema_options):
            schema_options.append(label)

    if not schema_options:
        ui.label("No schema files found in PostgreSQL or on disk.").classes("text-gray-500")
        return

    # --- Bulk delete -------------------------------------------------------
    _db_labels = [f"{r['collection_name']}/{r['source_file_stem']}" for r in db_schemas]
    if _db_labels:
        with ui.expansion("Bulk delete schemas (PostgreSQL)", icon="delete").classes(
                "w-full max-w-xl"):
            bulk = ui.select(_db_labels, multiple=True,
                             label="Schemas to delete").props(
                "outlined dense use-chips").classes("w-full")

            def do_bulk_delete():
                targets = list(bulk.value or [])
                if not targets:
                    ui.notify("Nothing selected", type="warning")
                    return
                with ui.dialog() as dlg, ui.card():
                    ui.label(f"Delete {len(targets)} schema(s) from PostgreSQL?").classes(
                        "font-medium")
                    for t in targets:
                        ui.label(f"• {t}").classes("text-sm text-gray-700")
                    with ui.row().classes("gap-2 mt-2"):
                        def _confirm():
                            for t in targets:
                                c, s = t.split("/", 1)
                                delete_schema_from_db(c, s)
                            dlg.close()
                            ui.notify(f"Deleted {len(targets)} schema(s)", type="warning")
                            rebuild_cb()
                        ui.button("Delete", on_click=_confirm).props("unelevated color=red")
                        ui.button("Cancel", on_click=dlg.close).props("flat")
                dlg.open()

            ui.button("Delete selected…", on_click=do_bulk_delete).props("outline color=red")

    sel = ui.select(schema_options, value=schema_options[0], label="Select schema").props(
        "outlined dense").classes("w-full max-w-xl")

    body = ui.column().classes("w-full mt-2")

    def load_selected():
        body.clear()
        selected = sel.value
        collection_name_sel = source_stem_sel = None
        if "[DB]" in selected:
            parts = selected.replace(" [DB]", "").split("/")
            collection_name_sel, source_stem_sel = parts[0], parts[1]
            schema = load_schema_from_db(collection_name_sel, source_stem_sel) or {}
        else:
            fname = selected.replace(" [disk]", "")
            schema = _load_json(SCHEMAS_DIR / fname, {})

        with body:
            _render_schema_editor(schema, collection_name_sel, source_stem_sel, selected,
                                  load_selected, delete_cb=rebuild_cb)

    sel.on_value_change(lambda: load_selected())
    load_selected()


def _render_schema_editor(schema, collection_name_sel, source_stem_sel, selected_schema,
                          reload_cb, delete_cb=None):
    ui.markdown("### Schema")
    ui.label("Current schema. Edit below and save to update.").classes("text-sm text-gray-600")
    ui.code(json.dumps(schema, indent=2, ensure_ascii=False), language="json").classes(
        "w-full max-h-64 overflow-auto")

    # Collect candidate fields from the schema itself...
    all_fields = []
    for values in schema.values():
        if isinstance(values, list):
            for v in values:
                if v not in all_fields:
                    all_fields.append(v)

    # ...plus actual column names seen in the DB, so unassigned fields show up too.
    if collection_name_sel and source_stem_sel:
        try:
            from core.db import fetchall as _fetchall
            rows = _fetchall(
                """
                SELECT DISTINCT jsonb_object_keys(payload->'description_fields') AS col
                FROM chunks
                WHERE collection_name = %s AND payload->>'source_file' ILIKE %s
                LIMIT 200
                """,
                (collection_name_sel, f"%{source_stem_sel}%"),
            )
            for r in rows:
                if r["col"] and r["col"] not in all_fields:
                    all_fields.append(r["col"])
        except Exception:
            pass

    all_fields = sorted(all_fields)

    ui.markdown("### Edit Schema")

    def _ms(label, key):
        default = [x for x in (schema.get(key) or []) if x in all_fields]
        return ui.select(all_fields, multiple=True, value=default, label=label).props(
            "outlined dense use-chips").classes("w-full")

    identifier_ms = _ms("Primary identifier field(s)", "identifier")
    reference_identifier_ms = _ms("Reference identifier field(s)", "reference_identifier")
    primary_name_ms = _ms("Primary name field(s)", "primary_name")
    aliases_ms = _ms("Alias field(s)", "aliases")
    description_ms = _ms("Description field(s)", "description")
    type_ms = _ms("Type field(s)", "type")
    enum_value_ms = _ms("Enum value field(s)", "enum_value")
    enum_name_ms = _ms("Enum name field(s)", "enum_name")
    tags_ms = _ms("Tags field(s)", "tags")
    other_ms = _ms("Other field(s) — kept in labeled fields, searchable", "other")

    subtype_default = schema.get("structured_subtype", "")
    subtype_sel = ui.select(
        _SUBTYPES, value=subtype_default if subtype_default in _SUBTYPES else "",
        label="Structured subtype",
    ).props("outlined dense").classes("w-64")

    warn_box = ui.column().classes("w-full mt-2")

    def _recompute_warnings():
        warn_box.clear()
        warnings = []
        if len(identifier_ms.value or []) > 1:
            warnings.append(f"Multiple primary identifier fields selected: {', '.join(identifier_ms.value)}")
        if (enum_value_ms.value or []) and not (enum_name_ms.value or []):
            warnings.append("Enum value fields exist, but enum name fields are missing.")
        if not (identifier_ms.value or []):
            warnings.append("No primary identifier field selected.")
        if not (primary_name_ms.value or []) and not (enum_value_ms.value or []):
            warnings.append("No primary name field selected.")
        if not (description_ms.value or []) and not (enum_value_ms.value or []):
            warnings.append("No description field selected.")
        with warn_box:
            ui.markdown("### Validation Warnings")
            if warnings:
                for w in warnings:
                    ui.label(f"⚠ {w}").classes("text-amber-700")
            else:
                ui.label("No basic schema warnings.").classes("text-green-700")

    def do_save():
        new_schema = {
            "identifier": identifier_ms.value or [],
            "reference_identifier": reference_identifier_ms.value or [],
            "primary_name": primary_name_ms.value or [],
            "aliases": aliases_ms.value or [],
            "description": description_ms.value or [],
            "type": type_ms.value or [],
            "enum_value": enum_value_ms.value or [],
            "enum_name": enum_name_ms.value or [],
            "structured_subtype": subtype_sel.value or "",
        }
        # NEVER drop columns on save: every known column not assigned to any
        # role is auto-added to 'other' — an unassigned column silently
        # vanishes from serialization otherwise (lost 'Recon Tool File
        # Format' twice before this guard).
        new_schema["tags"] = tags_ms.value or []
        new_schema["other"] = list(other_ms.value or [])
        _assigned = {c for cols in new_schema.values()
                     if isinstance(cols, list) for c in cols}
        for c in all_fields:
            if c not in _assigned:
                new_schema["other"].append(c)
        if collection_name_sel and source_stem_sel:
            save_schema_to_db(new_schema, collection_name_sel, source_stem_sel)
            ui.notify(f"Saved schema to PostgreSQL: {collection_name_sel}/{source_stem_sel}", type="positive")
        else:
            overrides = _load_json(SCHEMA_OVERRIDES_PATH, {})
            overrides[selected_schema] = new_schema
            _save_json(SCHEMA_OVERRIDES_PATH, overrides)
            ui.notify(f"Saved schema override for {selected_schema}", type="positive")
        reload_cb()

    def do_delete():
        delete_schema_from_db(collection_name_sel, source_stem_sel)
        ui.notify(f"Deleted schema: {collection_name_sel}/{source_stem_sel}", type="warning")
        (delete_cb or reload_cb)()  # full section rebuild so the dropdown updates

    with ui.row().classes("gap-2 mt-2"):
        ui.button("Save schema", on_click=do_save).props("unelevated")
        if collection_name_sel and source_stem_sel:
            ui.button("🗑️ Delete this schema from PostgreSQL", on_click=do_delete).props(
                "outline color=red")

    _recompute_warnings()
    for w in (identifier_ms, primary_name_ms, description_ms, enum_value_ms, enum_name_ms):
        w.on_value_change(lambda: _recompute_warnings())


# ===========================================================================
# Section 2: Payload Inspector (formerly the standalone Preview tab)
# ===========================================================================
def _payload_inspector_section():
    ui.label("Payload Inspector").classes("text-lg font-bold")

    names = [r["name"] for r in collection_stats()]
    if not names:
        ui.label("No collections found in PostgreSQL.").classes("text-gray-500")
        return

    with ui.row().classes("w-full items-center gap-2"):
        coll = ui.select(sorted(names), value=names[0], label="Collection").props(
            "outlined dense").classes("w-56")
        mode = ui.select(
            ["Sample payloads", "Identifier exact match", "Contains text / name / source"],
            value="Sample payloads", label="Inspector mode",
        ).props("outlined dense").classes("w-56")
        query = ui.input(label="Value").props("outlined dense clearable").classes("w-64")
        limit = ui.number(label="Max payloads", value=25, min=1, max=500, step=1).props(
            "outlined dense").classes("w-36")
        inspect_btn = ui.button("Inspect payloads", on_click=lambda: do_inspect()).props("unelevated")

    query.bind_visibility_from(mode, "value", backward=lambda v: v != "Sample payloads")

    out = ui.column().classes("w-full mt-2")

    async def do_inspect():
        m = mode.value
        q = (query.value or "").strip()
        if m != "Sample payloads" and not q:
            ui.notify("Enter a value, or choose Sample payloads.", type="warning")
            return

        inspect_btn.props("loading")
        try:
            if m == "Identifier exact match":
                points = await run.io_bound(
                    partial(get_by_identifier, coll.value, q, limit=int(limit.value or 25)))
            elif m == "Contains text / name / source":
                points = await run.io_bound(
                    partial(search_bm25, coll.value, query=q, limit=int(limit.value or 25)))
            else:
                points = await run.io_bound(
                    partial(scroll_collection, coll.value, limit=int(limit.value or 25)))
        except Exception as exc:
            out.clear()
            with out:
                ui.label(f"Error: {exc}").classes("text-red-600")
            return
        finally:
            inspect_btn.props(remove="loading")

        out.clear()
        with out:
            if not points:
                ui.label("No payloads found.").classes("text-gray-500")
                return

            ui.label(f"Found {len(points)} payload(s).").classes("text-sm text-gray-600")

            # Doc type / source type counts + source files
            doc_type_counts, source_type_counts, source_files = {}, {}, set()
            for p in points:
                payload = p.payload or {}
                doc_type_counts[str(payload.get("doc_type") or "unknown")] = \
                    doc_type_counts.get(str(payload.get("doc_type") or "unknown"), 0) + 1
                source_type_counts[str(payload.get("source_type") or "unknown")] = \
                    source_type_counts.get(str(payload.get("source_type") or "unknown"), 0) + 1
                if payload.get("source_file"):
                    source_files.add(str(payload["source_file"]))

            with ui.row().classes("w-full gap-4"):
                with ui.column().classes("flex-grow"):
                    ui.label("Doc Type Counts").classes("font-bold")
                    ui.label("Sanity check: how the sampled/matched payloads break down by doc_type "
                             "— useful for spotting a mis-classified ingest.").classes(
                        "text-xs text-gray-500")
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

            rows = []
            for p in points:
                payload = p.payload or {}
                enum_values = payload.get("enum_values") or []
                link_keys = payload.get("link_keys") or []
                related_link_keys = payload.get("related_link_keys") or []
                rows.append({
                    "identifier": payload.get("identifier"),
                    "identifier_field": payload.get("identifier_field"),
                    "identifier_namespace": payload.get("identifier_namespace"),
                    "primary_name": payload.get("primary_name"),
                    "doc_type": payload.get("doc_type"),
                    "source_type": payload.get("source_type"),
                    "source_file": payload.get("source_file"),
                    "file_path": payload.get("file_path"),
                    "enum_count": len(enum_values) if isinstance(enum_values, list) else 0,
                    "link_keys": ", ".join(link_keys) if isinstance(link_keys, list) else str(link_keys),
                    "related_link_keys": ", ".join(related_link_keys) if isinstance(related_link_keys, list) else str(related_link_keys),
                    "preview": str(payload.get("description") or payload.get("text") or "")[:300],
                })

            ui.label("Results").classes("font-bold mt-2")
            ui.table(
                columns=[
                    {"name": "identifier", "label": "identifier", "field": "identifier", "align": "left"},
                    {"name": "identifier_field", "label": "identifier_field", "field": "identifier_field", "align": "left"},
                    {"name": "identifier_namespace", "label": "namespace", "field": "identifier_namespace", "align": "left"},
                    {"name": "primary_name", "label": "primary_name", "field": "primary_name", "align": "left"},
                    {"name": "doc_type", "label": "doc_type", "field": "doc_type", "align": "left"},
                    {"name": "source_type", "label": "source_type", "field": "source_type", "align": "left"},
                    {"name": "source_file", "label": "source_file", "field": "source_file", "align": "left"},
                    {"name": "enum_count", "label": "enum_count", "field": "enum_count"},
                    {"name": "link_keys", "label": "link_keys", "field": "link_keys", "align": "left"},
                    {"name": "related_link_keys", "label": "related_link_keys", "field": "related_link_keys", "align": "left"},
                    {"name": "preview", "label": "preview", "field": "preview", "align": "left"},
                ],
                rows=rows,
                row_key="identifier",
                pagination=25,
            ).classes("w-full").props("dense")

            with ui.expansion("Raw Payloads").classes("w-full mt-2"):
                raw = [{"id": p.id, "payload": p.payload} for p in points]
                ui.code(json.dumps(raw, indent=2, ensure_ascii=False, default=str),
                         language="json").classes("w-full max-h-96 overflow-auto")

    query.on("keydown.enter", lambda: do_inspect())