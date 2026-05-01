from __future__ import annotations

from pathlib import Path
import sys


# remove /core from import path so it does not shadow installed packages
sys.path = [p for p in sys.path if Path(p).resolve() != CURRENT_DIR]

# ensure project root is first
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from datetime import datetime

import requests
import streamlit as st

from core.ingest_collection import ingest_collection
from core.query_router import route_query, semantic_search, debug_route_query, fetch_entity_row_by_title, run_query_with_method, get_display_labels
from core.discovery_engine import detect_ask_intent, run_discovery_with_method
from core.crosslink_engine import run_comparison_query

# =========================================================
# PATHS / CONFIG
# =========================================================
from core.paths import (
    PROJECT_ROOT,
    CONFIG_DIR,
    COLLECTIONS_PATH,
    SYSTEM_CONFIG_PATH,
    FILETYPES_PATH,
    SCHEMA_OVERRIDES_PATH,
    SCHEMAS_DIR,
)

BASE_DIR = PROJECT_ROOT

DEBUG = True


# =========================================================
# SESSION STATE
# =========================================================
if "ingestion_log" not in st.session_state:
    st.session_state.ingestion_log = []

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "ask_result" not in st.session_state:
    st.session_state.ask_result = None

if "ask_debug_data" not in st.session_state:
    st.session_state.ask_debug_data = None

if "ask_related_titles" not in st.session_state:
    st.session_state.ask_related_titles = []

if "ask_selected_related_article" not in st.session_state:
    st.session_state.ask_selected_related_article = ""

if "ask_selected_related_payload" not in st.session_state:
    st.session_state.ask_selected_related_payload = None

if "ask_method" not in st.session_state:
    st.session_state.ask_method = ""

if "ask_method_reason" not in st.session_state:
    st.session_state.ask_method_reason = ""

if "ask_discovery_result" not in st.session_state:
    st.session_state.ask_discovery_result = None

if "ask_discovery_preview_count" not in st.session_state:
    st.session_state.ask_discovery_preview_count = 10

# =========================================================
# JSON HELPERS
# =========================================================
def load_json(path: Path, default_obj):
    if not path.exists():
        return default_obj
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_obj


def save_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def ensure_files():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    if not COLLECTIONS_PATH.exists():
        save_json(COLLECTIONS_PATH, {})

    if not SYSTEM_CONFIG_PATH.exists():
        save_json(SYSTEM_CONFIG_PATH, {
            "qdrant_url": "http://localhost:6333",
            "embeddings_url": "http://localhost:1234/v1/embeddings",
            "embeddings_model": "nomic-embed-text",
            "vector_size": 768
        })


def extract_related_articles_from_answer(answer_text: str):
    text = str(answer_text or "")
    marker = "Related articles:"
    if marker not in text:
        return []

    after = text.split(marker, 1)[1].strip()
    lines = [line.strip() for line in after.splitlines() if line.strip()]

    related = []
    for line in lines:
        if line.startswith("- "):
            related.append(line[2:].strip())
        else:
            break

    return related


def strip_related_articles_from_answer(answer_text: str):
    text = str(answer_text or "")
    marker = "Related articles:"
    if marker not in text:
        return text
    return text.split(marker, 1)[0].rstrip()

# =========================================================
# LOGGING
# =========================================================
def log_ingestion(message: str):
    st.session_state.ingestion_log.append({
        "time": datetime.now().strftime("%H:%M:%S"),
        "msg": message
    })

    if len(st.session_state.ingestion_log) > 200:
        st.session_state.ingestion_log = st.session_state.ingestion_log[-200:]


# =========================================================
# QDRANT HELPERS
# =========================================================
def get_qdrant_collections(qdrant_url: str):
    try:
        r = requests.get(f"{qdrant_url}/collections", timeout=10)
        r.raise_for_status()
        return [c["name"] for c in r.json()["result"]["collections"]]
    except Exception:
        return []


def get_collection_stats(qdrant_url: str, collection_name: str):
    try:
        r = requests.get(f"{qdrant_url}/collections/{collection_name}", timeout=10)
        r.raise_for_status()
        data = r.json()["result"]

        return {
            "vectors": data.get("points_count", 0),
            "segments": data.get("segments_count", 0),
            "disk": data.get("disk_data_size", 0)
        }
    except Exception:
        return {
            "vectors": 0,
            "segments": 0,
            "disk": 0
        }


def delete_qdrant_collection(qdrant_url: str, collection_name: str):
    r = requests.delete(f"{qdrant_url}/collections/{collection_name}", timeout=10)
    r.raise_for_status()


# =========================================================
# APP START
# =========================================================
st.set_page_config(page_title="NAS AI", layout="wide")
ensure_files()

collections_cfg = load_json(COLLECTIONS_PATH, {})
system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
filetypes_cfg = load_json(FILETYPES_PATH, {})

qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
qdrant_collections = get_qdrant_collections(qdrant_url)

st.title("NAS AI")

tabs = st.tabs([
    "Collections",
    "Ingestion",
    "Validation",
    "Ask",
    "Preview",
    "Qdrant Debug",
    "System Config",
    "Chat",
    "Filetypes"
])

with tabs[0]:
    st.subheader("Collections")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_qdrant_collections(qdrant_url)

    left, right = st.columns([1, 1])

    with left:
        st.markdown("### Existing Collections")

        if not collections_cfg:
            st.info("No collections yet. Create one on the right.")
        else:
            for cname, cfg in collections_cfg.items():
                stats = get_collection_stats(qdrant_url, cname)
                with st.expander(f"{cname} ({stats['vectors']:,} vectors)", expanded=False):
                    st.caption(f"{stats['segments']} segments")
                    st.json(cfg)

        st.markdown("### Delete Collection Config")

        del_name = st.selectbox(
            "Select collection config to delete",
            [""] + sorted(collections_cfg.keys()),
            key="delete_collection_config"
        )

        if st.button("Delete config", disabled=(del_name == "")):
            collections_cfg.pop(del_name, None)
            save_json(COLLECTIONS_PATH, collections_cfg)
            st.success(f"Deleted config: {del_name}")
            st.rerun()

    with right:
        st.markdown("### Create / Edit Collection")

        existing_names = [""] + sorted(collections_cfg.keys())

        selected_existing = st.selectbox(
            "Load existing collection (optional)",
            existing_names,
            key="collection_existing_select"
        )

        existing_cfg = {}
        if selected_existing:
            existing_cfg = collections_cfg.get(selected_existing, {})

            st.session_state["collection_name_input"] = selected_existing
            st.session_state["collection_path_input"] = existing_cfg.get("path", "")
            st.session_state["collection_source_label"] = existing_cfg.get("source_label", "")
            st.session_state["collection_notes"] = existing_cfg.get("notes", "")

            existing_allowed = existing_cfg.get("allowed_filetypes", [])
            st.session_state["collection_allowed_filetypes"] = existing_allowed

            existing_filters = existing_cfg.get("filters", {})
            existing_field_filters = existing_filters.get("field_filters", [])
            first_filter = existing_field_filters[0] if existing_field_filters else {}

            st.session_state["collection_field_filters_enabled"] = len(existing_field_filters) > 0
            st.session_state["collection_filter_field"] = first_filter.get("field", "")
            st.session_state["collection_filter_mode"] = first_filter.get("mode", "exclude_equals")
            st.session_state["collection_filter_values"] = ",".join(first_filter.get("values", []))

        cname = st.text_input(
            "Collection name",
            key="collection_name_input"
        )

        path_value = st.text_input(
            "Path (file or folder)",
            key="collection_path_input"
        )

        all_filetypes = sorted(filetypes_cfg.keys()) if isinstance(filetypes_cfg, dict) else []

        existing_allowed = existing_cfg.get("allowed_filetypes", [])
        valid_existing_allowed = [x for x in existing_allowed if x in all_filetypes]

        allowed_filetypes = st.multiselect(
            "Allowed filetypes",
            all_filetypes,
            #default=valid_existing_allowed,
            key="collection_allowed_filetypes"
        )

        allowed_extensions_raw = st.text_input(
            "Allowed extensions (comma-separated, include dots)",
            value=",".join(existing_cfg.get("allowed_extensions", [])),
            key="collection_allowed_extensions"
        )

        exclude_dirs_raw = st.text_input(
            "Exclude folders (comma-separated)",
            value=",".join(existing_cfg.get("exclude_dirs", [])),
            key="collection_exclude_dirs"
        )

        exclude_extensions_raw = st.text_input(
            "Exclude extensions (comma-separated, include dots)",
            value=",".join(existing_cfg.get("exclude_extensions", [])),
            key="collection_exclude_extensions"
        )

        st.markdown("### Field / Row Filters")

        existing_filters = existing_cfg.get("filters", {})
        existing_field_filters = existing_filters.get("field_filters", [])

        filter_enabled_default = len(existing_field_filters) > 0
        first_filter = existing_field_filters[0] if existing_field_filters else {}

        field_filters_enabled = st.checkbox(
            "Enable field filters",
            #value=filter_enabled_default,
            key="collection_field_filters_enabled"
        )

        filter_field = st.text_input(
            "Field / column name",
            #value=first_filter.get("field", ""),
            key="collection_filter_field"
        )

        filter_mode_options = [
            "exclude_equals",
            "include_equals"
        ]

        existing_mode = first_filter.get("mode", "exclude_equals")
        if existing_mode not in filter_mode_options:
            existing_mode = "exclude_equals"

        filter_mode = st.selectbox(
            "Filter mode",
            filter_mode_options,
            index=filter_mode_options.index(existing_mode),
            key="collection_filter_mode"
        )

        filter_values = st.text_input(
            "Values (comma-separated)",
            #value=",".join(first_filter.get("values", [])),
            key="collection_filter_values"
        )

        source_label = st.text_input(
            "Source label (optional)",
            #value=existing_cfg.get("source_label", ""),
            key="collection_source_label"
        )

        notes = st.text_area(
            "Notes",
            #value=existing_cfg.get("notes", ""),
            key="collection_notes"
        )

        if st.button("Save collection"):
            cname_clean = cname.strip()

            if not cname_clean:
                st.error("Collection name is required.")
                st.stop()

            if not path_value.strip():
                st.error("Path is required.")
                st.stop()

            if not allowed_filetypes:
                st.error("Select at least one allowed filetype.")
                st.stop()

            field_filters = []

            if field_filters_enabled and filter_field.strip() and filter_values.strip():
                field_filters.append({
                    "field": filter_field.strip(),
                    "mode": filter_mode,
                    "values": [v.strip() for v in filter_values.split(",") if v.strip()]
                })

            allowed_extensions = [x.strip().lower() for x in allowed_extensions_raw.split(",") if x.strip()]
            exclude_dirs = [x.strip() for x in exclude_dirs_raw.split(",") if x.strip()]
            exclude_extensions = [x.strip().lower() for x in exclude_extensions_raw.split(",") if x.strip()]

            collections_cfg[cname_clean] = {
                "path": path_value.strip(),
                "allowed_filetypes": allowed_filetypes,
                "allowed_extensions": allowed_extensions,
                "exclude_dirs": exclude_dirs,
                "exclude_extensions": exclude_extensions,
                "source_label": source_label.strip(),
                "notes": notes.strip(),
                "filters": {
                    "field_filters": field_filters
                }
            }

            save_json(COLLECTIONS_PATH, collections_cfg)
            st.success(f"Collection '{cname_clean}' saved.")
            st.rerun()

    st.markdown("---")
    st.subheader("Delete Collection Data (Qdrant)")

    if qdrant_collections:
        col_to_delete = st.selectbox(
            "Select Qdrant collection",
            qdrant_collections,
            key="delete_qdrant_collection"
        )

        confirm_delete = st.checkbox(
            "Confirm permanent deletion",
            key=f"confirm_qdrant_delete_{col_to_delete}"
        )

        if st.button("Delete collection data"):
            if not confirm_delete:
                st.warning("Please confirm deletion.")
            else:
                try:
                    delete_qdrant_collection(qdrant_url, col_to_delete)
                    st.success(f"Deleted Qdrant collection: {col_to_delete}")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
    else:
        st.info("No Qdrant collections found.")

with tabs[1]:
    st.subheader("Ingestion")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_qdrant_collections(qdrant_url)

    if not collections_cfg:
        st.warning("No configured collections found.")
    else:
        display_map = {}
        display_list = []

        for name in sorted(collections_cfg.keys()):
            if name in qdrant_collections:
                label = f"{name} (exists)"
            else:
                label = f"{name} (new)"
            display_map[label] = name
            display_list.append(label)

        selected_label = st.selectbox(
            "Select collection",
            display_list,
            key="ingest_collection_select"
        )

        selected_collection = display_map[selected_label]
        collection_cfg = collections_cfg.get(selected_collection, {})

        st.markdown("### Collection Config")
        st.json(collection_cfg)

        raw_path = collection_cfg.get("path", "")
        path_obj = Path(raw_path).expanduser()

        st.markdown("### Path Check")
        st.write(f"**Path:** `{raw_path}`")

        if path_obj.exists():
            if path_obj.is_file():
                st.success("Path exists and is a file.")
            elif path_obj.is_dir():
                st.success("Path exists and is a folder.")
            else:
                st.info("Path exists but is not a regular file/folder.")
        else:
            st.error("Path does not exist.")

        # optional quick scan for folders
        if path_obj.exists() and path_obj.is_dir():
            if st.button("Scan Directory", key=f"scan_dir_{selected_collection}"):
                from collections import Counter

                counter = Counter()
                for f in path_obj.rglob("*"):
                    if f.is_file():
                        counter[f.suffix.lower() or "(no extension)"] += 1

                if counter:
                    st.markdown("### Files Detected")
                    rows = [{"extension": ext, "count": count} for ext, count in sorted(counter.items())]
                    st.dataframe(rows, width="stretch")
                else:
                    st.info("No files detected in this folder.")

        force_reingest = st.checkbox(
            "Force re-ingest",
            value=False,
            key=f"force_reingest_{selected_collection}"
        )

        run_disabled = not path_obj.exists()

        if st.button(
            "Run Ingestion",
            key=f"run_ingestion_{selected_collection}",
            disabled=run_disabled
        ):
            try:
                progress_bar = st.progress(0, text="Starting ingestion...")

                def ui_progress_callback(progress_value: float):
                    pct = max(0.0, min(1.0, float(progress_value)))
                    progress_bar.progress(pct, text=f"Ingestion progress: {int(pct * 100)}%")

                with st.spinner(f"Ingesting {selected_collection}..."):
                    result = ingest_collection(
                        collection_name=selected_collection,
                        collection_cfg=collection_cfg,
                        force_reingest=force_reingest,
                        progress_callback=ui_progress_callback
                    )

                progress_bar.progress(1.0, text="Ingestion complete.")

                log_ingestion(f"{selected_collection} → {result}")

                st.success("Ingestion complete.")

                st.markdown("### Ingestion Summary")

                col1, col2, col3, col4, col5 = st.columns(5)
                col1.metric("Total Files", result.get("total_files", 0))
                col2.metric("Processed", result.get("processed_files", 0))
                col3.metric("Skipped", result.get("skipped_files", 0))
                col4.metric("Failed", result.get("failed_files", 0))
                col5.metric("Total Chunks", result.get("total_chunks", 0))

                file_results = result.get("results", [])
                if file_results:
                    st.markdown("### Per-file Results")

                    rows = []
                    for r in file_results:
                        rows.append({
                            "path": str(getattr(r, "path", "")),
                            "filetype": getattr(r, "filetype_name", ""),
                            "success": getattr(r, "success", False),
                            "skipped": getattr(r, "skipped", False),
                            "chunks_created": getattr(r, "chunks_created", 0),
                            "error": getattr(r, "error", None),
                            "metadata": getattr(r, "metadata", {})
                        })

                    st.dataframe(rows, width="stretch")

                if result and result.get("results"):
                    failed_rows = [
                        {
                            "path": str(r.path),
                            "error": r.error
                        }
                        for r in result["results"]
                        if not r.success
                    ]

                    if failed_rows:
                        st.markdown("### First failed files")
                        st.dataframe(failed_rows[:20], width="stretch") 

                with st.expander("Raw Result", expanded=False):
                    st.json(result)

            except Exception as e:
                st.exception(e)

    st.markdown("---")
    st.subheader("Ingestion Log")

    if st.session_state.ingestion_log:
        for entry in reversed(st.session_state.ingestion_log):
            st.text(f"{entry['time']}  |  {entry['msg']}")
    else:
        st.info("No ingestion events yet.")

    if st.button("Clear ingestion log", key="clear_ingestion_log"):
        st.session_state.ingestion_log = []
        st.rerun()

with tabs[2]:
    st.subheader("Validation")

    schema_files = sorted(SCHEMAS_DIR.glob("*_schema.json"))
    schema_overrides = load_json(SCHEMA_OVERRIDES_PATH, {})

    if not schema_files:
        st.info("No schema files found.")
    else:
        selected_schema = st.selectbox(
            "Select schema output file",
            [p.name for p in schema_files],
            key="validation_schema_select"
        )

        schema_path = SCHEMAS_DIR / selected_schema
        schema = load_json(schema_path, {})

        st.markdown("### Generated Schema Output")
        st.caption("This is the schema produced by ingestion. It is not edited directly.")
        st.json(schema)

        current_override = schema_overrides.get(selected_schema, {})

        st.markdown("### Schema Override")

        all_fields = []
        for values in schema.values():
            if isinstance(values, list):
                for v in values:
                    if v not in all_fields:
                        all_fields.append(v)

        all_fields = sorted(all_fields)

        identifier_default = current_override.get(
            "identifier",
            schema.get("identifier", [])
        )

        reference_identifier_default = current_override.get(
            "reference_identifier",
            []
        )

        primary_name_default = current_override.get(
            "primary_name",
            schema.get("primary_name", [])
        )

        aliases_default = current_override.get(
            "aliases",
            schema.get("aliases", [])
        )

        description_default = current_override.get(
            "description",
            schema.get("description", [])
        )

        type_default = current_override.get(
            "type",
            schema.get("type", [])
        )

        enum_value_default = current_override.get(
            "enum_value",
            schema.get("enum_value", [])
        )

        enum_name_default = current_override.get(
            "enum_name",
            schema.get("enum_name", [])
        )

        structured_subtype_default = current_override.get(
            "structured_subtype",
            ""
        )

        identifier_override = st.multiselect(
            "Primary identifier field(s)",
            all_fields,
            default=[x for x in identifier_default if x in all_fields],
            key=f"override_identifier_{selected_schema}"
        )

        reference_identifier_override = st.multiselect(
            "Reference identifier field(s)",
            all_fields,
            default=[x for x in reference_identifier_default if x in all_fields],
            key=f"override_reference_identifier_{selected_schema}"
        )

        primary_name_override = st.multiselect(
            "Primary name field(s)",
            all_fields,
            default=[x for x in primary_name_default if x in all_fields],
            key=f"override_primary_name_{selected_schema}"
        )

        aliases_override = st.multiselect(
            "Alias field(s)",
            all_fields,
            default=[x for x in aliases_default if x in all_fields],
            key=f"override_aliases_{selected_schema}"
        )

        description_override = st.multiselect(
            "Description field(s)",
            all_fields,
            default=[x for x in description_default if x in all_fields],
            key=f"override_description_{selected_schema}"
        )

        type_override = st.multiselect(
            "Type field(s)",
            all_fields,
            default=[x for x in type_default if x in all_fields],
            key=f"override_type_{selected_schema}"
        )

        enum_value_override = st.multiselect(
            "Enum value field(s)",
            all_fields,
            default=[x for x in enum_value_default if x in all_fields],
            key=f"override_enum_value_{selected_schema}"
        )

        enum_name_override = st.multiselect(
            "Enum name field(s)",
            all_fields,
            default=[x for x in enum_name_default if x in all_fields],
            key=f"override_enum_name_{selected_schema}"
        )

        subtype_options = [
            "",
            "definition",
            "enum_values",
            "relationship",
            "structured"
        ]

        structured_subtype_override = st.selectbox(
            "Structured subtype",
            subtype_options,
            index=subtype_options.index(structured_subtype_default)
            if structured_subtype_default in subtype_options else 0,
            key=f"override_structured_subtype_{selected_schema}"
        )

        if st.button("Save schema override", key=f"save_override_{selected_schema}"):
            schema_overrides[selected_schema] = {
                "identifier": identifier_override,
                "reference_identifier": reference_identifier_override,
                "primary_name": primary_name_override,
                "aliases": aliases_override,
                "description": description_override,
                "type": type_override,
                "enum_value": enum_value_override,
                "enum_name": enum_name_override,
                "structured_subtype": structured_subtype_override,
            }

            save_json(SCHEMA_OVERRIDES_PATH, schema_overrides)
            st.success(f"Saved override for {selected_schema}")
            st.rerun()

        if current_override:
            with st.expander("Current Saved Override", expanded=False):
                st.json(current_override)

        warnings = []

        identifier_fields = current_override.get("identifier", schema.get("identifier", []))
        enum_value_fields = current_override.get("enum_value", schema.get("enum_value", []))
        primary_name_fields = current_override.get("primary_name", schema.get("primary_name", []))
        description_fields = current_override.get("description", schema.get("description", []))

        if len(identifier_fields) > 1:
            warnings.append(
                f"Multiple primary identifier fields selected: {', '.join(identifier_fields)}"
            )

        if enum_value_fields and not current_override.get("enum_name", schema.get("enum_name", [])):
            warnings.append("Enum value fields exist, but enum name fields are missing.")

        if not identifier_fields:
            warnings.append("No primary identifier field selected.")

        if not primary_name_fields and not enum_value_fields:
            warnings.append("No primary name field selected.")

        if not description_fields and not enum_value_fields:
            warnings.append("No description field selected.")

        st.markdown("### Validation Warnings")

        if warnings:
            for w in warnings:
                st.warning(w)
        else:
            st.success("No basic schema warnings.")

        st.markdown("---")
        st.markdown("### Payload Inspector")

        qdrant_collections_for_validation = get_qdrant_collections(qdrant_url)

        if not qdrant_collections_for_validation:
            st.info("No Qdrant collections found.")
        else:
            validation_collection = st.selectbox(
                "Select Qdrant collection",
                sorted(qdrant_collections_for_validation),
                key="validation_payload_collection"
            )

            identifier_to_inspect = st.text_input(
                "Identifier to inspect",
                value="22",
                key="validation_payload_identifier"
            )

            if st.button("Inspect payloads", key="validation_inspect_payloads"):
                try:
                    from qdrant_client import QdrantClient
                    from qdrant_client.models import Filter, FieldCondition, MatchValue

                    qclient = QdrantClient(url=qdrant_url)

                    points, _ = qclient.scroll(
                        collection_name=validation_collection,
                        scroll_filter=Filter(
                            must=[
                                FieldCondition(
                                    key="identifier",
                                    match=MatchValue(value=str(identifier_to_inspect).strip())
                                )
                            ]
                        ),
                        limit=100,
                        with_payload=True,
                        with_vectors=False
                    )

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
                            "structured_subtype": payload.get("structured_subtype"),
                            "primary_name": payload.get("primary_name"),
                            "doc_type": payload.get("doc_type"),
                            "enum_count": len(enum_values) if isinstance(enum_values, list) else 0,
                            "link_keys": ", ".join(link_keys) if isinstance(link_keys, list) else str(link_keys),
                            "related_link_keys": ", ".join(related_link_keys) if isinstance(related_link_keys, list) else str(related_link_keys),
                            "source_file": payload.get("source_file"),
                            "ingest_source": payload.get("ingest_source"),
                        })

                    if rows:
                        st.success(f"Found {len(rows)} payload(s).")
                        st.dataframe(rows, width="stretch")

                        with st.expander("Raw payloads", expanded=False):
                            for i, p in enumerate(points, start=1):
                                st.markdown(f"#### Payload {i}")
                                st.json(p.payload or {})
                    else:
                        st.warning("No payloads found for that identifier.")

                except Exception as e:
                    st.exception(e)

with tabs[3]:
    st.subheader("Ask")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_qdrant_collections(qdrant_url)

    if not qdrant_collections:
        st.warning("No Qdrant collections found.")
    else:
        selected_collection = st.selectbox(
            "Select collection",
            sorted(qdrant_collections),
            key="ask_collection_select"
        )

        # Collection metadata captions
        #schema_labels = get_display_labels(selected_collection)
        #st.caption(
        #    f"Identifier field: {schema_labels['identifier']} | "
        #    f"Primary name field: {schema_labels['primary_name']} | "
        #    f"Description field: description"
        #)

        question = st.text_area(
            "Question",
            key="ask_question_input",
            height=140,
            placeholder="Ask a question or paste email content here..."
        )

        show_debug = st.checkbox(
            "Show debug details",
            value=False,
            key="ask_show_debug"
        )

        debug_top_k = st.number_input(
            "Debug top K",
            min_value=1,
            max_value=20,
            value=10,
            step=1,
            key="ask_debug_top_k"
        )

        if st.button("Ask", key="ask_run_button"):
            if not question.strip():
                st.warning("Enter a question.")
            else:
                try:

                    st.session_state.ask_result = None
                    st.session_state.ask_debug_data = None
                    st.session_state.ask_method = ""
                    st.session_state.ask_method_reason = ""
                    st.session_state.ask_related_titles = []
                    st.session_state.ask_selected_related_article = ""
                    st.session_state.ask_selected_related_payload = None
                    st.session_state.ask_discovery_result = None
                    st.session_state["ask_discovery_selected_items"] = []

                    intent = detect_ask_intent(question)

                    with st.spinner("Running query..."):
                        if intent["mode"] == "comparison":
                            query_run = run_comparison_query(
                                selected_collection,
                                question
                            )
                            st.session_state.ask_discovery_result = None
                            result = query_run["result"]

                        elif intent["mode"] in {"discovery_count", "discovery_list"}:
                            query_run = run_discovery_with_method(
                                selected_collection,
                                question,
                                limit=200
                            )
                            st.session_state.ask_discovery_result = query_run["result"]
                            result = query_run["result"]
                        else:
                            query_run = run_query_with_method(
                                selected_collection,
                                question,
                                limit=int(debug_top_k)
                            )
                            st.session_state.ask_discovery_result = None
                            result = query_run["result"]

                    st.session_state.ask_result = result
                    st.session_state.ask_method = query_run["method"]
                    st.session_state.ask_method_reason = query_run["reason"]
                    st.session_state.ask_related_titles = []
                    st.session_state.ask_selected_related_article = ""
                    st.session_state.ask_selected_related_payload = None

                    if isinstance(result, str):
                        st.session_state.ask_related_titles = extract_related_articles_from_answer(result)

                    if show_debug and intent["mode"] == "answer":
                        debug_data = debug_route_query(
                            selected_collection,
                            question,
                            limit=int(debug_top_k)
                        )
                        debug_data["final_result"] = result
                        st.session_state.ask_debug_data = debug_data
                    else:
                        st.session_state.ask_debug_data = None

                except Exception as e:
                    st.session_state.ask_result = None
                    st.session_state.ask_debug_data = None
                    st.session_state.ask_method = ""
                    st.session_state.ask_method_reason = ""
                    st.session_state.ask_discovery_result = None
                    st.exception(e)

        result = st.session_state.get("ask_result")
        debug_data = st.session_state.get("ask_debug_data")
        method_used = st.session_state.get("ask_method", "")
        method_reason = st.session_state.get("ask_method_reason", "")
        discovery_result = st.session_state.get("ask_discovery_result")
        preview_count = st.session_state.get("ask_discovery_preview_count", 10)

        if result is not None:
            method_used = st.session_state.get("ask_method", "")
            method_reason = st.session_state.get("ask_method_reason", "")

            if method_used:
                pretty_method = {
                    "exact_title_match": "exact title match",
                    "lexical_short": "lexical short-query mode",
                    "semantic": "semantic mode"
                }.get(method_used, method_used)

                st.write("METHOD DEBUG:", method_used, method_reason)

                st.info(f"Method: {pretty_method} — Reason: {method_reason}")

            st.markdown("### Final Answer")

            if method_used in {"discovery_count", "discovery_list", "comparison"} and isinstance(discovery_result, dict):
                total_matches = int(discovery_result.get("total_matches", 0))
                results = discovery_result.get("results", [])

                st.info(f"{total_matches} match(es) found.")

                preview_count = st.number_input(
                    "How many previews to show",
                    min_value=1,
                    max_value=100,
                    value=min(preview_count, max(len(results), 1)),
                    step=1,
                    key="ask_discovery_preview_count_input"
                )
                st.session_state.ask_discovery_preview_count = int(preview_count)

                preview_rows = []
                for item in results[:int(preview_count)]:
                    preview_rows.append({
                        "rank": item.get("rank"),
                        "score": item.get("score"),
                        "doc_type": item.get("doc_type"),
                        "identifier": item.get("identifier"),
                        "primary_name": item.get("primary_name"),
                        "source_type": item.get("source_type"),
                        "source_file": item.get("source_file"),
                        "preview": item.get("preview")
                    })

                if preview_rows:
                    st.dataframe(preview_rows, width="stretch")

                show_item_input = st.text_input(
                    "Show ranked content (example: 1 or 1-3)",
                    key="ask_discovery_show_range"
                )

                if st.button("Show Selected Ranked Content", key="ask_discovery_show_button"):
                    selected_items = []
                    raw = show_item_input.strip()

                    if raw:
                        if "-" in raw:
                            parts = raw.split("-", 1)
                            try:
                                start = int(parts[0].strip())
                                end = int(parts[1].strip())
                                for item in results:
                                    rank = int(item.get("rank", 0))
                                    if start <= rank <= end:
                                        selected_items.append(item)
                            except Exception:
                                st.warning("Invalid range. Use formats like 1 or 1-3.")
                        else:
                            try:
                                wanted = int(raw)
                                for item in results:
                                    if int(item.get("rank", 0)) == wanted:
                                        selected_items.append(item)
                                        break
                            except Exception:
                                st.warning("Invalid rank. Use formats like 1 or 1-3.")

                    if selected_items:
                        st.session_state["ask_discovery_selected_items"] = selected_items

                selected_items = st.session_state.get("ask_discovery_selected_items", [])
                if selected_items:
                    st.markdown("### Ranked Content")
                    for item in selected_items:
                        payload = item.get("payload", {}) or {}

                        st.markdown(f"**Rank {item.get('rank')}: {item.get('primary_name') or '(no title)'}**")
                        meta_bits = []

                        if item.get("identifier") not in [None, ""]:
                            meta_bits.append(f"identifier={item.get('identifier')}")
                        if item.get("doc_type"):
                            meta_bits.append(f"doc_type={item.get('doc_type')}")
                        if item.get("source_type"):
                            meta_bits.append(f"source_type={item.get('source_type')}")
                        if item.get("source_file"):
                            meta_bits.append(f"source_file={item.get('source_file')}")

                        if meta_bits:
                            st.caption(" | ".join(meta_bits))

                        full_text = (
                            payload.get("description")
                            or payload.get("text")
                            or payload.get("ocr_text")
                            or ""
                        )

                        if full_text:
                            st.markdown(str(full_text))

                        st.markdown("---")

                if debug_data:
                    with st.expander("Returned Discovery Payload", expanded=False):
                        st.json(discovery_result)

            if isinstance(result, list):
                st.info(f"{len(result)} result(s) returned.")

                rows = []
                for item in result:
                    rows.append({
                        "identifier": item.get("identifier"),
                        "primary_name": item.get("primary_name"),
                        "description": item.get("description"),
                        "score": item.get("score")
                    })

                st.dataframe(rows, width="stretch")

                with st.expander("Raw Result", expanded=False):
                    st.json(result)

            elif isinstance(result, dict):
                st.json(result)

            else:
                answer_text = str(result)
                main_answer = strip_related_articles_from_answer(answer_text)
                st.markdown(main_answer)

                related_titles = st.session_state.ask_related_titles
                if related_titles:
                    st.markdown("### Related Articles")

                    selected_related = st.selectbox(
                        "Select a related article",
                        [""] + related_titles,
                        key="ask_related_article_select"
                    )

                    if st.button("Use Selected Related Article", key="ask_use_related_article"):
                        if selected_related:
                            st.session_state.ask_selected_related_article = selected_related

                            matches = fetch_entity_row_by_title(selected_collection, selected_related, limit=1)
                            st.session_state.ask_selected_related_payload = matches[0] if matches else None

                    chosen_related = st.session_state.ask_selected_related_article
                    chosen_payload = st.session_state.ask_selected_related_payload

                    if chosen_related:
                        st.info(f"Selected related article: {chosen_related}")

                    if chosen_payload:
                        st.markdown("### Related Article Content")

                        related_desc = str(chosen_payload.get("description") or "").strip()
                        related_name = str(chosen_payload.get("primary_name") or "").strip()

                        if related_name:
                            st.markdown(f"**{related_name}**")

                        if related_desc:
                            st.markdown(related_desc)

            if debug_data:
                def points_to_rows(points):
                    rows = []
                    for i, p in enumerate(points or [], start=1):
                        payload = p.payload or {}

                        preview_text = (
                            payload.get("text")
                            or payload.get("description")
                            or ""
                        )
                        preview_text = str(preview_text).strip().replace("\n", " ")
                        preview_text = preview_text[:300]

                        rows.append({
                            "rank": i,
                            "score": getattr(p, "score", None),
                            "identifier": payload.get("identifier"),
                            "primary_name": payload.get("primary_name"),
                            "doc_type": payload.get("doc_type"),
                            "source_type": payload.get("source_type"),
                            "source_file": payload.get("source_file"),
                            "page_num": payload.get("page_num"),
                            "preview": preview_text
                        })
                    return rows

                st.markdown("---")
                st.markdown("### Debug Details")

                with st.expander("Semantic Candidates", expanded=True):
                    st.dataframe(points_to_rows(debug_data.get("semantic_points")), width="stretch")

                with st.expander("Lexical Chunk Candidates", expanded=False):
                    st.dataframe(points_to_rows(debug_data.get("lexical_chunk_points")), width="stretch")

                with st.expander("Lexical Structured Candidates", expanded=False):
                    st.dataframe(points_to_rows(debug_data.get("lexical_structured_points")), width="stretch")

                with st.expander("Lexical Entity Row Candidates (disabled)", expanded=False):
                    st.dataframe(points_to_rows(debug_data.get("lexical_entity_points")), width="stretch")

                with st.expander("Merged Candidates Before Rerank", expanded=False):
                    st.dataframe(points_to_rows(debug_data.get("merged_points")), width="stretch")

                with st.expander("Final Reranked Candidates", expanded=True):
                    ranked_rows = []
                    for i, p in enumerate(debug_data.get("ranked_points") or [], start=1):
                        payload = p.payload or {}
                        preview_text = (
                            payload.get("text")
                            or payload.get("description")
                            or ""
                        )
                        preview_text = str(preview_text).strip().replace("\n", " ")
                        preview_text = preview_text[:300]

                        ranked_rows.append({
                            "rank": i,
                            "semantic_score": getattr(p, "score", None),
                            "identifier": payload.get("identifier"),
                            "primary_name": payload.get("primary_name"),
                            "doc_type": payload.get("doc_type"),
                            "source_type": payload.get("source_type"),
                            "source_file": payload.get("source_file"),
                            "page_num": payload.get("page_num"),
                            "preview": preview_text
                        })

                    st.dataframe(ranked_rows, width="stretch")

                with st.expander("Returned Answer Payload", expanded=False):
                    st.write(debug_data.get("final_result"))

with tabs[4]:
    st.subheader("Preview / Inspector")

    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_qdrant_collections(qdrant_url)

    if not qdrant_collections:
        st.warning("No Qdrant collections found.")
    else:
        selected_collection = st.selectbox(
            "Select collection",
            sorted(qdrant_collections),
            key="preview_collection_select"
        )

        sample_limit = st.number_input(
            "Sample size",
            min_value=1,
            max_value=200,
            value=25,
            step=1,
            key="preview_sample_limit"
        )

        if st.button("Load Preview", key="preview_load_button"):
            try:
                r = requests.post(
                    f"{qdrant_url}/collections/{selected_collection}/points/scroll",
                    json={
                        "limit": int(sample_limit),
                        "with_payload": True,
                        "with_vectors": False
                    },
                    timeout=30
                )
                r.raise_for_status()

                points = r.json().get("result", {}).get("points", [])

                if not points:
                    st.info("No points found in this collection.")
                else:
                    st.success(f"Loaded {len(points)} point(s).")

                    # -------------------------
                    # Summary counts
                    # -------------------------
                    doc_type_counts = {}
                    source_type_counts = {}
                    source_files = set()

                    for p in points:
                        payload = p.get("payload", {}) or {}

                        doc_type = str(payload.get("doc_type") or "unknown")
                        source_type = str(payload.get("source_type") or "unknown")
                        source_file = payload.get("source_file")

                        doc_type_counts[doc_type] = doc_type_counts.get(doc_type, 0) + 1
                        source_type_counts[source_type] = source_type_counts.get(source_type, 0) + 1

                        if source_file:
                            source_files.add(str(source_file))

                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("### Doc Type Counts")
                        st.dataframe(
                            [{"doc_type": k, "count": v} for k, v in sorted(doc_type_counts.items())],
                            width="stretch"
                        )

                    with col2:
                        st.markdown("### Source Type Counts")
                        st.dataframe(
                            [{"source_type": k, "count": v} for k, v in sorted(source_type_counts.items())],
                            width="stretch"
                        )

                    st.markdown("### Source Files")
                    st.dataframe(
                        [{"source_file": x} for x in sorted(source_files)],
                        width="stretch"
                    )

                    # -------------------------
                    # Sample payload preview
                    # -------------------------
                    preview_rows = []

                    for i, p in enumerate(points, start=1):
                        payload = p.get("payload", {}) or {}

                        preview_text = (
                            payload.get("text")
                            or payload.get("description")
                            or ""
                        )
                        preview_text = str(preview_text).strip().replace("\n", " ")
                        preview_text = preview_text[:300]

                        preview_rows.append({
                            "rank": i,
                            "identifier": payload.get("identifier"),
                            "primary_name": payload.get("primary_name"),
                            "doc_type": payload.get("doc_type"),
                            "source_type": payload.get("source_type"),
                            "source_file": payload.get("source_file"),
                            "page_num": payload.get("page_num"),
                            "related_identifiers": payload.get("related_identifiers"),
                            "preview": preview_text
                        })

                    st.markdown("### Sample Points")
                    st.dataframe(preview_rows, width="stretch")

                    with st.expander("Raw Payloads", expanded=False):
                        st.json(points)

            except Exception as e:
                st.error(e)

with tabs[5]:
    st.subheader("Qdrant Debug")
    st.info("Qdrant Debug tab scaffold ready.")

with tabs[6]:
    st.subheader("System Config")
    st.info("System Config tab scaffold ready.")

with tabs[7]:
    st.subheader("Chat")
    st.info("Chat tab scaffold ready.")

with tabs[8]:
    st.subheader("Filetypes")
    st.info("Filetypes tab scaffold ready.")