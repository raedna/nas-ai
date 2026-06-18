from __future__ import annotations
import json
from pathlib import Path
import sys
import re

CURRENT_DIR = Path(__file__).resolve().parent
LOCAL_PROJECT_ROOT = CURRENT_DIR.parent

# ensure project root is first so "core" imports work
if str(LOCAL_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(LOCAL_PROJECT_ROOT))

# remove /core from import path so it does not shadow installed packages
sys.path = [
    p for p in sys.path
    if Path(p).resolve() != CURRENT_DIR
]

import json
from datetime import datetime

import requests
import streamlit as st
from core.ingest_collection import ingest_collection
from core.retrieval.router import (
    route_query,
    semantic_search,
    debug_route_query,
    fetch_entity_row_by_title,
    run_query_with_method,
    get_display_labels,
    explain_query_routing,
    score_point_shared,
)
from core.retrieval.discovery import detect_ask_intent, run_discovery_with_method
from core.retrieval.crosslink import run_comparison_query
from core.query_helpers import infer_doc_type
from core.retrieval_debug import score_point_shared_debug
from core.schema_loader import load_collection_schemas

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

def render_answer_images_from_payload(payload):
    if not payload:
        return

    image_items = []

    # Embedded doc / Obsidian images
    for key in [
        "embedded_image_paths",
        "embedded_images",
        "image_paths",
        "related_image_paths",
    ]:
        val = payload.get(key)

        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    image_items.append({
                        "path": item,
                        "caption": Path(item).name,
                        "ocr": "",
                    })

                elif isinstance(item, dict):
                    image_path = (
                        item.get("local_path")
                        or item.get("path")
                        or item.get("file_path")
                        or item.get("image_path")
                    )

                    if image_path:
                        image_items.append({
                            "path": image_path,
                            "caption": (
                                item.get("file_name")
                                or item.get("caption")
                                or Path(str(image_path)).name
                            ),
                            "ocr": item.get("ocr_text") or item.get("text") or "",
                        })

    # Standalone image payload
    source_type = str(payload.get("source_type") or "").lower()
    file_type = str(payload.get("file_type") or "").lower()

    if source_type in ["image", "standalone_image"] or file_type == "image":
        image_path = (
            payload.get("local_path")
            or payload.get("file_path")
            or payload.get("image_path")
            or payload.get("source_file")
        )

        if image_path:
            image_items.append({
                "path": image_path,
                "caption": payload.get("file_name") or Path(str(image_path)).name,
                "ocr": payload.get("ocr_text") or payload.get("text") or "",
            })

    # Deduplicate
    seen = set()
    clean_items = []

    for item in image_items:
        image_path = str(item.get("path") or "").strip()
        if not image_path or image_path in seen:
            continue

        seen.add(image_path)
        clean_items.append(item)

    if not clean_items:
        return

    st.markdown("### Related Images")

    for item in clean_items[:10]:
        image_path = str(item["path"])
        caption = item.get("caption") or Path(image_path).name

        try:
            if Path(image_path).exists():
                st.image(image_path, caption=caption)
            else:
                st.caption(f"Image path not found: {image_path}")
        except Exception as e:
            st.caption(f"Could not render image: {image_path} — {e}")

        ocr_text = str(item.get("ocr") or "").strip()
        if ocr_text:
            with st.expander(f"OCR / extracted text: {caption}", expanded=False):
                st.text(ocr_text)

def render_answer_with_inline_images(
    answer_text,
    image_items=None,
    show_images=True,
    show_inline_ocr=False,
    show_ocr_expanders=True,
):
    text = str(answer_text or "")
    image_items = image_items or []

    image_map = {}
    for item in image_items:
        path = str(item.get("path") or "").strip()
        caption = str(item.get("caption") or Path(path).name).strip()
        ocr = str(item.get("ocr") or "").strip()

        if not path:
            continue

        image_map[Path(caption).name.lower()] = {
            "path": path,
            "caption": caption,
            "ocr": ocr,
        }

    pattern = r"\[Embedded image OCR from:\s*([^\]]+)\]"

    pos = 0
    found_marker = False
    rendered_any_image = False

    for match in re.finditer(pattern, text):
        found_marker = True

        before = text[pos:match.start()].strip()
        image_name = match.group(1).strip()
        image_key = Path(image_name).name.lower()
        image_item = image_map.get(image_key)

        if before:
            st.markdown(before)

        if image_item and show_images:
            image_path = image_item["path"]
            caption = image_item["caption"]

            try:
                if Path(image_path).exists():
                    st.image(image_path, caption=caption)
                    rendered_any_image = True
                else:
                    st.caption(f"Image path not found: {image_path}")
            except Exception as e:
                st.caption(f"Could not render image: {image_path} — {e}")

            ocr_text = image_item.get("ocr") or ""

            if show_inline_ocr and ocr_text:
                st.markdown(ocr_text)
            elif show_ocr_expanders and ocr_text:
                with st.expander(f"OCR / extracted text: {caption}", expanded=False):
                    st.text(ocr_text)
        else:
            # If image cannot be resolved, keep marker visible only when images are enabled.
            if show_images:
                st.caption(f"Image not resolved: {image_name}")

        pos = match.end()

        # Skip OCR text immediately following the marker unless inline OCR is enabled.
        # OCR may continue until the next blank paragraph, the next image marker, or end of answer.
        if not show_inline_ocr:
            j = pos

            # skip whitespace after marker
            while j < len(text) and text[j] in [" ", "\t", "\r", "\n"]:
                j += 1

            next_marker = re.search(pattern, text[j:])
            next_marker_pos = j + next_marker.start() if next_marker else None

            next_blank = text.find("\n\n", j)

            if next_blank != -1 and (next_marker_pos is None or next_blank < next_marker_pos):
                pos = next_blank + 2
            elif next_marker_pos is not None:
                pos = next_marker_pos
            else:
                # OCR was the last thing in the answer
                pos = len(text)

    if not found_marker:
        st.markdown(text)
        return False

    tail = text[pos:].strip()
    if tail:
        st.markdown(tail)

    return rendered_any_image

def resolve_image_payloads_from_related_titles(collection_name, qdrant_url, related_titles, limit=5000):
    if not collection_name or not qdrant_url or not related_titles:
        return []

    wanted = {Path(str(t)).name.strip().lower() for t in related_titles if str(t).strip()}
    if not wanted:
        return []

    try:
        r = requests.post(
            f"{qdrant_url}/collections/{collection_name}/points/scroll",
            json={
                "limit": int(limit),
                "with_payload": True,
                "with_vectors": False
            },
            timeout=30
        )
        r.raise_for_status()

        points = r.json().get("result", {}).get("points", [])
    except Exception:
        return []

    matches = []

    for p in points:
        payload = p.get("payload", {}) or {}

        candidates = [
            payload.get("file_name"),
            payload.get("source_file"),
            payload.get("primary_name"),
            payload.get("file_path"),
            payload.get("local_path"),
            payload.get("image_path"),
        ]

        candidate_names = {
            Path(str(x)).name.strip().lower()
            for x in candidates
            if str(x or "").strip()
        }

        if wanted & candidate_names:
            source_type = str(payload.get("source_type") or "").lower()
            file_type = str(payload.get("file_type") or "").lower()

            if source_type in ["image", "standalone_image"] or file_type == "image":
                matches.append(payload)

    return matches

def extract_ocr_for_image_from_payload(payload, image_name):
    payload = payload or {}
    image_name = str(image_name or "").strip()

    if not image_name:
        return ""

    candidate_texts = [
        payload.get("description"),
        payload.get("text"),
        payload.get("ocr_text"),
        payload.get("image_ocr"),
        payload.get("embedded_image_ocr"),
    ]

    full_text = "\n\n".join(
        str(t).strip()
        for t in candidate_texts
        if str(t or "").strip()
    )

    if not full_text:
        return ""

    markers = [
        f"[Embedded image OCR from: {image_name}]",
        f"Embedded image OCR from: {image_name}",
        f"OCR from: {image_name}",
    ]

    for marker in markers:
        marker_pos = full_text.find(marker)
        if marker_pos == -1:
            continue

        after = full_text[marker_pos + len(marker):].strip()
        after = after.lstrip(":\n\r\t -").strip()

        stop_markers = [
            "[Embedded image OCR from:",
            "Embedded image OCR from:",
            "OCR from:",
            "Related notes:",
            "Related images:",
            "Related Images:",
        ]

        stop_positions = [
            after.find(stop_marker)
            for stop_marker in stop_markers
            if after.find(stop_marker) != -1
        ]

        if stop_positions:
            after = after[:min(stop_positions)].strip()

        return after.strip()

    return ""

def fetch_image_paths_for_source_file(collection_name, qdrant_url, source_file, related_titles=None, limit=5000):
    if not collection_name or not qdrant_url or not source_file:
        return []

    wanted_names = {
        Path(str(t)).name.strip().lower()
        for t in (related_titles or [])
        if str(t).strip().lower().endswith((".png", ".jpg", ".jpeg", ".webp"))
    }

    # Important: do not render arbitrary images from the note
    if not wanted_names:
        return []

    try:
        r = requests.post(
            f"{qdrant_url}/collections/{collection_name}/points/scroll",
            json={
                "limit": int(limit),
                "with_payload": True,
                "with_vectors": False
            },
            timeout=30
        )
        r.raise_for_status()
        points = r.json().get("result", {}).get("points", [])
    except Exception:
        return []

    image_items = []
    seen = set()

    for p in points:
        payload = p.get("payload", {}) or {}

        if payload.get("source_file") != source_file:
            continue

        paths = payload.get("embedded_image_paths") or []
        targets = payload.get("embedded_image_targets") or []

        for idx, image_path in enumerate(paths):
            image_path = str(image_path or "").strip()
            if not image_path:
                continue

            caption = None
            if idx < len(targets):
                caption = str(targets[idx] or "").strip()

            caption_name = Path(str(caption or image_path)).name.strip().lower()

            # Strict filter: only show images explicitly listed in related_titles
            if caption_name not in wanted_names:
                continue

            if image_path in seen:
                continue

            seen.add(image_path)

            image_items.append({
                "path": image_path,
                "caption": caption or Path(image_path).name,
                "ocr": extract_ocr_for_image_from_payload(payload, caption or Path(resolved_path).name),
            })

    return image_items


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


# ---------------------------------------------------------------------------
# PostgreSQL collection helpers — replace Qdrant equivalents
# ---------------------------------------------------------------------------

def get_pg_collections() -> list:
    """Return list of collection names from PostgreSQL."""
    try:
        from core.retrieval.db_retrieval import fetchall
        rows = fetchall(
            "SELECT name FROM collections ORDER BY name",
            ()
        )
        return [r["name"] for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=60)
def get_all_collection_stats() -> dict:
    """Return chunk and enum counts for all collections in one query."""
    try:
        from core.retrieval.db_retrieval import fetchall
        chunks = fetchall(
            "SELECT collection_name, COUNT(*) as n FROM chunks GROUP BY collection_name",
            ()
        )
        enums = fetchall(
            "SELECT collection_name, COUNT(*) as n FROM enum_values GROUP BY collection_name",
            ()
        )
        chunk_map = {r["collection_name"]: r["n"] for r in chunks}
        enum_map = {r["collection_name"]: r["n"] for r in enums}
        all_names = set(chunk_map) | set(enum_map)
        return {
            name: {
                "chunks": chunk_map.get(name, 0),
                "enums": enum_map.get(name, 0),
            }
            for name in all_names
        }
    except Exception:
        return {}


def delete_pg_collection(collection_name: str):
    """Delete all data for a collection from PostgreSQL."""
    try:
        from core.db import execute
        execute("DELETE FROM enum_values WHERE collection_name = %s", (collection_name,))
        execute("DELETE FROM chunks WHERE collection_name = %s", (collection_name,))
        execute("DELETE FROM files WHERE collection_name = %s", (collection_name,))
        execute("DELETE FROM collections WHERE name = %s", (collection_name,))
    except Exception as e:
        raise RuntimeError(f"Failed to delete collection {collection_name}: {e}")


NLP_CONFIG_PATH = Path("config/nlp_config.json")

def load_nlp_ui_config():
    if not NLP_CONFIG_PATH.exists():
        return {}

    with open(NLP_CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_nlp_ui_config(cfg):
    NLP_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(NLP_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

# =========================================================
# APP START
# =========================================================
st.set_page_config(page_title="NAS AI", layout="wide")
ensure_files()

collections_cfg = load_json(COLLECTIONS_PATH, {})
system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
filetypes_cfg = load_json(FILETYPES_PATH, {})

qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
qdrant_collections = get_pg_collections()

st.title("NAS AI")

tabs = st.tabs([
    "Collections",
    "Ingestion",
    "Validation",
    "Ask",
    "Preview",
    "SQL Inspector",
    "System Config",
    "Chat",
    "Data Prep",
    "Filetypes"
])

with tabs[0]:
    st.subheader("Collections")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_pg_collections()
    left, right = st.columns([1, 1])

    with left:
        st.markdown("### Existing Collections")

        if not collections_cfg:
            st.info("No collections yet. Create one on the right.")
        else:
            all_collection_stats = get_all_collection_stats()
            for cname, cfg in collections_cfg.items():
                stats = all_collection_stats.get(cname, {"chunks": 0, "enums": 0})
                with st.expander(f"{cname} ({stats['chunks']:,} chunks, {stats['enums']:,} enums)", expanded=False):
                    st.caption(f"{stats['chunks']:,} chunks | {stats['enums']:,} enum values")
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
            try:
                delete_pg_collection(del_name)
            except Exception as e:
                st.warning(f"Config deleted but PostgreSQL cleanup failed: {e}")
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

            last_loaded = st.session_state.get("collection_loaded_existing")

            # Only load saved values when selection changes.
            # Do not overwrite user edits on every Streamlit rerun.
            if last_loaded != selected_existing:
                st.session_state["collection_loaded_existing"] = selected_existing

                st.session_state["collection_name_input"] = selected_existing
                st.session_state["collection_path_input"] = existing_cfg.get("path", "")
                st.session_state["collection_source_label"] = existing_cfg.get("source_label", "")
                st.session_state["collection_notes"] = existing_cfg.get("notes", "")

                st.session_state["collection_allowed_filetypes"] = existing_cfg.get("allowed_filetypes", [])

                st.session_state["collection_allowed_extensions"] = ",".join(
                    existing_cfg.get("allowed_extensions", [])
                )
                st.session_state["collection_exclude_dirs"] = ",".join(
                    existing_cfg.get("exclude_dirs", [])
                )
                st.session_state["collection_exclude_extensions"] = ",".join(
                    existing_cfg.get("exclude_extensions", [])
                )

                existing_filters = existing_cfg.get("filters", {})
                existing_field_filters = existing_filters.get("field_filters", [])
                first_filter = existing_field_filters[0] if existing_field_filters else {}

                st.session_state["collection_field_filters_enabled"] = len(existing_field_filters) > 0
                st.session_state["collection_filter_field"] = first_filter.get("field", "")
                st.session_state["collection_filter_mode"] = first_filter.get("mode", "exclude_equals")
                st.session_state["collection_filter_values"] = ",".join(first_filter.get("values", []))

                st.session_state["collection_asset_search_roots"] = "\n".join(
                    existing_cfg.get("asset_search_roots", [])
                )
        else:
            st.session_state["collection_loaded_existing"] = ""

        cname = st.text_input(
            "Collection name",
            key="collection_name_input"
        )

        path_value = st.text_input(
            "Path (file or folder)",
            key="collection_path_input"
        )

        # Excel sheet detection
        sheet_name = None
        if path_value.strip() and path_value.strip().endswith(('.xlsx', '.xls')):
            try:
                import pandas as pd
                xl = pd.ExcelFile(path_value.strip())
                sheet_names = xl.sheet_names
                if len(sheet_names) > 1:
                    sheet_name = st.selectbox(
                        "Select sheet to ingest",
                        sheet_names,
                        key="collection_sheet_name"
                    )
                else:
                    sheet_name = sheet_names[0]
                    st.caption(f"Sheet: {sheet_name}")

                # Show column headers from selected sheet
                if sheet_name:
                    preview_df = pd.read_excel(path_value.strip(), sheet_name=sheet_name, nrows=5, header=None, dtype=str)
                    preview_df = preview_df.fillna("")

                    # Detect header row — find first row with mostly non-empty, non-title values
                    from TABLES.table_parser import detect_header_row
                    header_row_idx = detect_header_row(preview_df)
                    headers = [str(v) for v in preview_df.iloc[header_row_idx].tolist() if str(v).strip() not in ['', 'nan']]
                    st.caption(f"Detected columns ({len(headers)}): {', '.join(headers[:10])}" + (" ..." if len(headers) > 10 else ""))

                    # Auto-infer schema using rows after header
                    from TABLES.schema_inference_table import infer_table_schema
                    data_rows = preview_df.iloc[header_row_idx + 1:].copy()
                    data_rows.columns = [str(v) for v in preview_df.iloc[header_row_idx].tolist()]
                    rows_dict = data_rows.to_dict(orient='records')
                    source_file = Path(path_value.strip()).name
                    inferred = infer_table_schema(rows_dict, collection_name="preview", source_file=source_file)

                    with st.expander("Auto-inferred schema (click to review)", expanded=False):
                        for role in ['identifier', 'primary_name', 'description', 'type', 'aliases', 'other']:
                            cols = inferred.get(role, [])
                            if cols:
                                st.markdown(f"**{role}:** {', '.join(cols)}")

            except Exception as e:
                st.caption(f"Could not read Excel file: {e}")

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
            key="collection_allowed_extensions"
        )

        exclude_dirs_raw = st.text_input(
            "Exclude folders (comma-separated)",
            key="collection_exclude_dirs"
        )

        exclude_extensions_raw = st.text_input(
            "Exclude extensions (comma-separated, include dots)",
            key="collection_exclude_extensions"
        )

        asset_search_roots_raw = st.text_area(
            "Asset search roots (one path per line)",
            key="collection_asset_search_roots",
            help="Optional. Used by DOCS/Obsidian notes to resolve embedded image links like ![[image.png]]."
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

            asset_search_roots = [
                x.strip()
                for x in asset_search_roots_raw.replace(",", "\n").splitlines()
                if x.strip()
            ]

            collections_cfg[cname_clean] = {
                "path": path_value.strip(),
                "allowed_filetypes": allowed_filetypes,
                "allowed_extensions": allowed_extensions,
                "exclude_dirs": exclude_dirs,
                "exclude_extensions": exclude_extensions,
                "asset_search_roots": asset_search_roots,
                "source_label": source_label.strip(),
                "notes": notes.strip(),
                "filters": {
                    "field_filters": field_filters
                },
                "template_config": {
                    "sheet_name": sheet_name,
                } if sheet_name is not None else {}
            }

            save_json(COLLECTIONS_PATH, collections_cfg)

            # Upsert into PostgreSQL collections table
            try:
                from core.db import execute
                import json as _json
                filters_payload = {"field_filters": field_filters}
                if sheet_name is not None:
                    filters_payload["sheet_name"] = sheet_name

                execute("""
                    INSERT INTO collections (name, path, allowed_filetypes, source_label, filters)
                    VALUES (%s, %s, %s::jsonb, %s, %s::jsonb)
                    ON CONFLICT (name) DO UPDATE SET
                        path = EXCLUDED.path,
                        allowed_filetypes = EXCLUDED.allowed_filetypes,
                        source_label = EXCLUDED.source_label,
                        filters = EXCLUDED.filters
                """, (
                    cname_clean,
                    path_value.strip(),
                    _json.dumps(allowed_filetypes),
                    source_label.strip(),
                    _json.dumps(filters_payload),
                ))
            except Exception as e:
                st.warning(f"Saved to config but PostgreSQL update failed: {e}")

            st.success(f"Collection '{cname_clean}' saved.")
            st.rerun()

    st.markdown("---")
    st.subheader("Delete Collection Data")

    pg_collections = get_pg_collections()
    if pg_collections:
        col_to_delete = st.selectbox(
            "Select collection to delete",
            pg_collections,
            key="delete_pg_collection_select"
        )

        confirm_delete = st.checkbox(
            "Confirm permanent deletion — removes all chunks, files and enum values",
            key=f"confirm_pg_delete_{col_to_delete}"
        )

        if st.button("Delete collection data", key="delete_pg_collection_btn"):
            if not confirm_delete:
                st.warning("Please confirm deletion before proceeding.")
            else:
                try:
                    delete_pg_collection(col_to_delete)
                    st.success(f"✅ Deleted collection: {col_to_delete}")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
    else:
        st.info("No collections found in PostgreSQL.")

with tabs[1]:
    st.subheader("Ingestion")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_pg_collections()

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
        # Merge PostgreSQL filters (includes sheet_name) into collection_cfg
        try:
            from core.db import fetchall as _fetchall
            pg_rows = _fetchall(
                "SELECT filters FROM collections WHERE name = %s",
                (selected_collection,)
            )
            if pg_rows and pg_rows[0].get("filters"):
                pg_filters = pg_rows[0]["filters"]
                # Merge — PostgreSQL filters take precedence
                existing_filters = collection_cfg.get("filters", {})
                existing_filters.update(pg_filters)
                collection_cfg["filters"] = existing_filters
        except Exception:
            pass

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

    from core.schema_inference import (
        ensure_schemas_table, list_schemas_from_db,
        load_schema_from_db, save_schema_to_db
    )
    ensure_schemas_table()
    db_schemas = list_schemas_from_db()
    disk_schemas = sorted(SCHEMAS_DIR.glob("*_schema.json"))

    schema_options = []
    for row in db_schemas:
        schema_options.append(f"{row['collection_name']}/{row['source_file_stem']} [DB]")
    for f in disk_schemas:
        label = f"{f.name} [disk]"
        if not any(f.stem.replace('_schema','') in s for s in schema_options):
            schema_options.append(label)

    if not schema_options:
        st.info("No schema files found in PostgreSQL or on disk.")
    else:
        selected_schema = st.selectbox(
            "Select schema",
            schema_options,
            key="validation_schema_select"
        )

        schema = {}
        collection_name_sel = None
        source_stem_sel = None

        if "[DB]" in selected_schema:
            parts = selected_schema.replace(" [DB]", "").split("/")
            collection_name_sel = parts[0]
            source_stem_sel = parts[1]
            schema = load_schema_from_db(collection_name_sel, source_stem_sel) or {}
        else:
            fname = selected_schema.replace(" [disk]", "")
            schema_path = SCHEMAS_DIR / fname
            schema = load_json(schema_path, {})

        st.markdown("### Schema")
        st.caption("Current schema. Edit below and save to update.")
        st.json(schema)

        current_override = schema

        st.markdown("### Edit Schema")

        all_fields = []
        for values in schema.values():
            if isinstance(values, list):
                for v in values:
                    if v not in all_fields:
                        all_fields.append(v)

        all_fields = sorted(all_fields)

        identifier_default = schema.get("identifier", [])
        reference_identifier_default = schema.get("reference_identifier", [])
        primary_name_default = schema.get("primary_name", [])
        aliases_default = schema.get("aliases", [])
        description_default = schema.get("description", [])
        type_default = schema.get("type", [])
        enum_value_default = schema.get("enum_value", [])
        enum_name_default = schema.get("enum_name", [])
        structured_subtype_default = schema.get("structured_subtype", "")

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

        if st.button("Save schema", key=f"save_override_{selected_schema}"):
            new_schema = {
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
            if collection_name_sel and source_stem_sel:
                save_schema_to_db(new_schema, collection_name_sel, source_stem_sel)
                st.success(f"Saved schema to PostgreSQL: {collection_name_sel}/{source_stem_sel}")
            else:
                save_json(SCHEMA_OVERRIDES_PATH, {selected_schema: new_schema})
                st.success(f"Saved schema override for {selected_schema}")
            st.rerun()

        if current_override:
            with st.expander("Current Saved Override", expanded=False):
                st.json(current_override)

        if collection_name_sel and source_stem_sel:
            st.markdown("---")
            if st.button("🗑️ Delete this schema from PostgreSQL", key=f"delete_schema_{selected_schema}", type="secondary"):
                from core.schema_inference import delete_schema_from_db
                delete_schema_from_db(collection_name_sel, source_stem_sel)
                st.success(f"Deleted schema: {collection_name_sel}/{source_stem_sel}")
                st.rerun()

        warnings = []

        identifier_fields = schema.get("identifier", [])
        enum_value_fields = schema.get("enum_value", [])
        primary_name_fields = schema.get("primary_name", [])
        description_fields = schema.get("description", [])

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

        qdrant_collections_for_validation = get_pg_collections()

        if not qdrant_collections_for_validation:
            st.warning("No collections found in PostgreSQL.")
        else:
            validation_collection = st.selectbox(
                "Select Qdrant collection",
                sorted(qdrant_collections_for_validation),
                key="validation_payload_collection"
            )

            inspect_mode = st.selectbox(
                "Inspector mode",
                [
                    "Sample payloads",
                    "Identifier exact match",
                    "Contains text / name / source",
                ],
                key="validation_payload_inspect_mode"
            )

            inspect_query = ""

            if inspect_mode == "Identifier exact match":
                inspect_query = st.text_input(
                    "Identifier to inspect",
                    value="",
                    key="validation_payload_identifier"
                )

            elif inspect_mode == "Contains text / name / source":
                inspect_query = st.text_input(
                    "Search text",
                    value="",
                    key="validation_payload_search_text"
                )

            inspect_limit = st.number_input(
                "Max payloads to inspect",
                min_value=1,
                max_value=500,
                value=25,
                step=1,
                key="validation_payload_inspect_limit"
            )

            if st.button("Inspect payloads", key="validation_inspect_payloads"):
                try:
                    from core.retrieval.db_retrieval import (
                        get_by_identifier, scroll_collection, search_bm25
                    )

                    points = []

                    if inspect_mode == "Identifier exact match":
                        identifier_value = str(inspect_query or "").strip()

                        if not identifier_value:
                            st.warning("Enter an identifier, or choose Sample payloads.")
                        else:
                            points = get_by_identifier(
                                validation_collection,
                                identifier_value,
                                limit=int(inspect_limit)
                            )

                    elif inspect_mode == "Sample payloads":
                        points = scroll_collection(
                            validation_collection,
                            limit=int(inspect_limit)
                        )

                    else:
                        query = str(inspect_query or "").strip()

                        if not query:
                            st.warning("Enter search text, or choose Sample payloads.")
                        else:
                            points = search_bm25(
                                validation_collection,
                                query=query,
                                limit=int(inspect_limit)
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

                    if rows:
                        st.success(f"Found {len(rows)} payload(s).")
                        st.dataframe(rows, width="stretch")

                        with st.expander("Raw payloads", expanded=False):
                            for i, p in enumerate(points, start=1):
                                st.markdown(f"#### Payload {i}")
                                st.json(p.payload or {})
                    else:
                        st.warning("No payloads found.")

                except Exception as e:
                    st.exception(e)

with tabs[3]:
    st.subheader("Ask")

    collections_cfg = load_json(COLLECTIONS_PATH, {})
    system_cfg = load_json(SYSTEM_CONFIG_PATH, {})
    qdrant_url = system_cfg.get("qdrant_url", "http://localhost:6333")
    qdrant_collections = get_pg_collections()

    if not qdrant_collections:
        st.warning("No collections found in PostgreSQL.")
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

        show_answer_images = st.checkbox(
            "Show related images",
            value=True,
            key="ask_show_answer_images"
        )

        show_inline_ocr = st.checkbox(
            "Show OCR inline",
            value=False,
            key="ask_show_inline_ocr"
        )

        show_ocr_expanders = st.checkbox(
            "Show OCR in expanders",
            value=True,
            key="ask_show_ocr_expanders"
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

                    from core.retrieval.discovery import llm_detect_intent
                    intent = llm_detect_intent(question)

                    with st.spinner("Running query..."):
                        if intent["mode"] == "comparison":
                            query_run = run_comparison_query(
                                selected_collection,
                                question
                            )
                            st.session_state.ask_discovery_result = None
                            result = query_run["result"]

                        else:
                            query_run = run_query_with_method(
                                selected_collection,
                                question,
                                limit=int(debug_top_k)
                            )

                            if query_run.get("method") == "discovery_list":
                                st.session_state.ask_discovery_result = query_run["result"]
                            else:
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

                # Derive a human-friendly header for the aliases column from the
                # collection schema (e.g. "Prime Broker file name"), falling back
                # to the generic role name. No hardcoding of collection specifics.
                _alias_label = "aliases"
                try:
                    _schemas = load_collection_schemas(selected_collection)
                    for _sch in _schemas.values():
                        _alias_fields = _sch.get("aliases") or []
                        if _alias_fields:
                            _alias_label = str(_alias_fields[0])
                            break
                except Exception:
                    pass

                # Derive column labels from payload fields — no schema lookup needed
                _id_label = "identifier"
                _name_label = "primary_name"
                _alias_label = "aliases"
                try:
                    _first_payload = (results[0].get("payload") or {}) if results else {}
                    if _first_payload.get("identifier_field"):
                        _id_label = str(_first_payload["identifier_field"])
                    if _first_payload.get("primary_name_field"):
                        _name_label = str(_first_payload["primary_name_field"])
                    _schemas = load_collection_schemas(selected_collection)
                    for _sch in _schemas.values():
                        if _sch.get("aliases"):
                            _alias_label = str(_sch["aliases"][0])
                        break
                except Exception:
                    pass

                preview_rows = []
                for item in results[:int(preview_count)]:
                    _payload = item.get("payload") or {}
                    _aliases = _payload.get("aliases") or []
                    _aliases_str = ", ".join(str(a) for a in _aliases if a) if isinstance(_aliases, list) else str(_aliases)
                    preview_rows.append({
                        "rank": item.get("rank"),
                        "score": item.get("score"),
                        "doc_type": item.get("doc_type"),
                        _id_label: item.get("identifier"),
                        _name_label: item.get("primary_name"),
                        _alias_label: _aliases_str,
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

                answer_payload = None

                if isinstance(debug_data, dict):
                    ranked_points = debug_data.get("ranked_points") or []
                    if ranked_points:
                        answer_payload = ranked_points[0].payload or {}

                source_image_items = []

                if answer_payload:
                    source_file = answer_payload.get("source_file")
                    source_image_items = fetch_image_paths_for_source_file(
                        selected_collection,
                        qdrant_url,
                        source_file,
                        related_titles=answer_payload.get("related_titles") or [],
                    )

                inline_images_rendered = False

                inline_images_rendered = render_answer_with_inline_images(
                    main_answer,
                    image_items=source_image_items,
                    show_images=show_answer_images,
                    show_inline_ocr=show_inline_ocr,
                    show_ocr_expanders=show_ocr_expanders,
                )

                if isinstance(debug_data, dict):
                    ranked_points = debug_data.get("ranked_points") or []
                    if ranked_points:
                        answer_payload = ranked_points[0].payload or {}

                render_answer_images_from_payload(answer_payload)

                if answer_payload:
                    source_file = answer_payload.get("source_file")
                    source_image_items = fetch_image_paths_for_source_file(
                        selected_collection,
                        qdrant_url,
                        source_file,
                        related_titles=answer_payload.get("related_titles") or [],
                    )

                    if show_answer_images and source_image_items and not inline_images_rendered:
                        st.markdown("### Related Images")

                        for item in source_image_items[:10]:
                            image_path = str(item.get("path") or "")
                            caption = item.get("caption") or Path(image_path).name

                            try:
                                if Path(image_path).exists():
                                    st.image(image_path, caption=caption)
                                else:
                                    st.caption(f"Image path not found: {image_path}")
                            except Exception as e:
                                st.caption(f"Could not render image: {image_path} — {e}")

                # If the answer payload only has related image names, resolve them to image payloads.
                if answer_payload:
                    related_titles_for_images = answer_payload.get("related_titles") or []
                    image_payloads = resolve_image_payloads_from_related_titles(
                        selected_collection,
                        qdrant_url,
                        related_titles_for_images,
                    )

                    for image_payload in image_payloads:
                        render_answer_images_from_payload(image_payload)

                if answer_payload:
                    with st.expander("Answer payload image debug", expanded=False):
                        st.write({
                            "embedded_image_paths": answer_payload.get("embedded_image_paths"),
                            "embedded_images": answer_payload.get("embedded_images"),
                            "image_paths": answer_payload.get("image_paths"),
                            "related_image_paths": answer_payload.get("related_image_paths"),
                            "related_titles": answer_payload.get("related_titles"),
                            "source_file": answer_payload.get("source_file"),
                        })
                else:
                    st.caption("No answer payload available for image rendering.")

                related_titles = st.session_state.ask_related_titles

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

            if show_debug: #and debug_data:
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
                            "semantic_score": getattr(p, "score", None),
                            "final_rerank_score": score_point_shared(p, question),
                            "score": getattr(p, "score", None),
                            "identifier": payload.get("identifier"),
                            "primary_name": payload.get("primary_name"),
                            "doc_type": payload.get("doc_type"),
                            "inferred_doc_type": infer_doc_type(payload),
                            "source_type": payload.get("source_type"),
                            "source_file": payload.get("source_file"),
                            "page_num": payload.get("page_num"),
                            "preview": preview_text
                        })
                    return rows

                st.markdown("---")
                st.markdown("### Debug Details")

                selected_method = None
                if "query_run" in locals():
                    selected_method = query_run.get("method")

                with st.expander("Routing Decision", expanded=True):
                    try:
                        st.json(explain_query_routing(selected_collection, question))
                    except Exception as e:
                        st.warning(f"Could not build routing debug: {e}")

                    if "query_run" in locals():
                        st.write({
                            "selected_method": query_run.get("method"),
                            "selected_reason": query_run.get("reason"),
                        })

                if "query_run" in locals() and query_run.get("namespace_debug"):
                    with st.expander("Structured Namespace Lookup Debug", expanded=True):
                        st.json(query_run.get("namespace_debug") or {})

                if "query_run" in locals() and query_run.get("structured_plan_dry_run"):
                    with st.expander("Structured Planner Dry Run", expanded=True):
                        st.json(query_run.get("structured_plan_dry_run") or {})

                if selected_method == "structured_query_plan":
                    with st.expander("Structured Query Plan", expanded=True):
                        st.json(query_run.get("plan") or {})

                    with st.expander("Structured Plan Executor Candidates", expanded=True):
                        executor_items = query_run.get("executor_debug_items") or []

                        if executor_items:
                            st.dataframe(executor_items, width="stretch")
                        else:
                            st.info("No structured executor candidates were returned.")

                    st.info(
                        "Final answer was produced by structured_plan_executor. "
                        "Semantic/rerank debug panels were not used for this answer."
                    )

                elif selected_method == "lexical_short" and debug_data:
                    st.info(
                        "Final answer was produced by lexical_short. "
                        "The semantic/rerank debug panels are not used for this answer path."
                    )

                    with st.expander("Lexical Short Candidates", expanded=True):
                        lexical_items = debug_data.get("lexical_short_items") or []

                        if lexical_items:
                            rows = []

                            for i, item in enumerate(lexical_items, start=1):
                                payload = item.get("payload") or {}

                                preview_text = (
                                    payload.get("text")
                                    or payload.get("description")
                                    or ""
                                )
                                preview_text = str(preview_text).strip().replace("\n", " ")
                                preview_text = preview_text[:300]

                                rows.append({
                                    "rank": i,
                                    "lexical_short_score": item.get("score"),
                                    "identifier": item.get("identifier") or payload.get("identifier"),
                                    "primary_name": item.get("primary_name") or payload.get("primary_name"),
                                    "doc_type": payload.get("doc_type"),
                                    "inferred_doc_type": infer_doc_type(payload),
                                    "source_type": payload.get("source_type"),
                                    "source_file": payload.get("source_file"),
                                    "preview": preview_text,
                                })

                            st.dataframe(rows, width="stretch")
                        else:
                            st.info("No lexical_short candidates were returned.")

                elif debug_data:

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
                                "final_rerank_score": score_point_shared(p, question),
                                "identifier": payload.get("identifier"),
                                "primary_name": payload.get("primary_name"),
                                "doc_type": payload.get("doc_type"),
                                "inferred_doc_type": infer_doc_type(payload),
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
    qdrant_collections = get_pg_collections()

    if not qdrant_collections:
        st.warning("No collections found in PostgreSQL.")
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
                from core.retrieval.db_retrieval import scroll_collection
                raw_points = scroll_collection(selected_collection, limit=int(sample_limit))
                points = [{"payload": p.payload, "id": p.id} for p in raw_points]

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
    st.subheader("SQL Inspector")

    sql_collections = get_pg_collections()
    sql_query = st.text_area(
        "SQL Query",
        value="SELECT primary_name, identifier, description, doc_type\nFROM chunks\nWHERE collection_name = 'xml_test'\nLIMIT 10",
        height=150,
        key="sql_inspector_query"
    )

    if st.button("Run Query", key="sql_run_button"):
        try:
            from core.retrieval.db_retrieval import fetchall
            rows = fetchall(sql_query, ())
            if rows:
                import pandas as pd
                st.dataframe(pd.DataFrame(rows), use_container_width=True)
                st.caption(f"{len(rows)} row(s) returned.")
            else:
                st.info("Query returned no rows.")
        except Exception as e:
            st.error(f"SQL error: {e}")

with tabs[6]:
    st.subheader("System Config")

    st.markdown("### Structured Planner")

    nlp_cfg = load_nlp_ui_config()
    planner_cfg = nlp_cfg.get("structured_planner", {})

    planner_enabled = st.checkbox(
        "Enable structured planner",
        value=bool(planner_cfg.get("enabled", True)),
        help="Allow the LLM planner to generate structured retrieval plans."
    )

    planner_dry_run = st.checkbox(
        "Dry run only",
        value=bool(planner_cfg.get("dry_run", True)),
        help="Generate and show the plan, but do not let it produce the final answer."
    )

    planner_execute = st.checkbox(
        "Allow planner execution",
        value=bool(planner_cfg.get("execute", False)),
        help="Allow the structured planner + executor to produce the final answer."
    )

    planner_min_confidence = st.number_input(
        "Minimum planner confidence",
        min_value=0.0,
        max_value=1.0,
        value=float(planner_cfg.get("min_confidence", 0.7)),
        step=0.05,
    )

    planner_debug_raw = st.checkbox(
        "Debug raw LLM plan",
        value=bool(planner_cfg.get("debug_raw_plan", False)),
        help="Print/show raw LLM planner output for troubleshooting."
    )

    if planner_execute and planner_dry_run:
        st.warning("Planner execution is enabled, but dry-run is also enabled. Dry-run prevents final-answer execution.")

    if planner_execute and not planner_dry_run:
        st.warning("Planner execution is active. Structured planner results may become final answers.")

    if st.button("Save structured planner settings"):
        nlp_cfg.setdefault("structured_planner", {})

        nlp_cfg["structured_planner"].update({
            "enabled": planner_enabled,
            "dry_run": planner_dry_run,
            "execute": planner_execute,
            "min_confidence": planner_min_confidence,
            "debug_raw_plan": planner_debug_raw,
        })

        save_nlp_ui_config(nlp_cfg)
        st.success("Structured planner settings saved. Restart Streamlit if changes do not apply immediately.")

    st.markdown("---")
    st.markdown("### Retrieval Settings")

    system_cfg_edit = load_json(SYSTEM_CONFIG_PATH, {})

    confidence_threshold = st.number_input(
        "Confidence threshold",
        min_value=0.0,
        max_value=1.0,
        value=float(system_cfg_edit.get("retrieval_confidence_threshold", 0.105)),
        step=0.005,
        format="%.3f",
        help="RRF score below this returns top 5 candidates instead of a single answer."
    )

    st.markdown("---")
    st.markdown("### Embeddings")

    embeddings_url = st.text_input(
        "Embeddings URL",
        value=system_cfg_edit.get("embeddings_url", "http://localhost:1234/v1/embeddings")
    )
    embeddings_model = st.text_input(
        "Embeddings model",
        value=system_cfg_edit.get("embeddings_model", "text-embedding-bge-large-en-v1.5")
    )
    vector_size = st.number_input(
        "Vector size",
        min_value=1,
        max_value=4096,
        value=int(system_cfg_edit.get("vector_size", 1024)),
        step=1,
    )

    st.markdown("---")
    st.markdown("### PostgreSQL")

    pg_cfg = system_cfg_edit.get("pgvector", {})
    pg_host = st.text_input("Host", value=pg_cfg.get("host", ""))
    pg_port = st.number_input("Port", min_value=1, max_value=65535, value=int(pg_cfg.get("port", 5433)))
    pg_dbname = st.text_input("Database", value=pg_cfg.get("dbname", ""))
    pg_user = st.text_input("User", value=pg_cfg.get("user", ""))

    st.markdown("---")
    st.markdown("### Qdrant (admin reference)")
    qdrant_url_cfg = st.text_input("Qdrant URL", value=system_cfg_edit.get("qdrant_url", ""))

    st.markdown("---")
    st.markdown("### Language Model (LLM)")

    nlp_cfg_edit = load_nlp_ui_config()
    llm_cfg_edit = nlp_cfg_edit.get("local_llm", {})

    llm_model = st.text_input(
        "LLM model name",
        value=llm_cfg_edit.get("model", "meta-llama-3.1-8b-instruct"),
        help="Must match exactly the model identifier in LM Studio"
    )
    llm_base_url = st.text_input(
        "LLM base URL",
        value=llm_cfg_edit.get("base_url", "http://localhost:1234"),
    )
    llm_timeout = st.number_input(
        "LLM timeout (seconds)",
        min_value=10, max_value=300,
        value=int(llm_cfg_edit.get("timeout", 60)),
        step=10,
        key="llm_timeout_input"
    )

    st.markdown("---")
    st.markdown("### Cross-Encoder Reranker")

    ce_cfg_ui = system_cfg_edit.get("cross_encoder", {})
    ce_enabled = st.checkbox(
        "Enable cross-encoder reranking",
        value=bool(ce_cfg_ui.get("enabled", False)),
        help="Uses MiniLM to rerank top-K results for entity_row and docs"
    )
    ce_model = st.text_input(
        "Cross-encoder model",
        value=ce_cfg_ui.get("model", "cross-encoder/ms-marco-MiniLM-L-6-v2"),
    )
    ce_top_k = st.number_input(
        "Top-K candidates to rerank",
        min_value=3, max_value=20,
        value=int(ce_cfg_ui.get("top_k", 10)),
        step=1,
        key="ce_top_k_input"
    )

    if st.button("Save system settings", key="save_system_settings"):
        system_cfg_edit["retrieval_confidence_threshold"] = confidence_threshold
        system_cfg_edit["embeddings_url"] = embeddings_url
        system_cfg_edit["embeddings_model"] = embeddings_model
        system_cfg_edit["vector_size"] = int(vector_size)
        system_cfg_edit["qdrant_url"] = qdrant_url_cfg
        system_cfg_edit["pgvector"] = {
            "host": pg_host,
            "port": int(pg_port),
            "dbname": pg_dbname,
            "user": pg_user,
            "password": pg_cfg.get("password", ""),
        }
        system_cfg_edit["cross_encoder"] = {
            "enabled": ce_enabled,
            "model": ce_model,
            "apply_to_doc_types": ["entity_row", "procedural", "reference", "mixed"],
            "top_k": int(ce_top_k),
        }
        nlp_cfg_edit["local_llm"] = {
            "base_url": llm_base_url,
            "model": llm_model,
            "timeout": int(llm_timeout),
        }
        save_nlp_ui_config(nlp_cfg_edit)
        save_json(SYSTEM_CONFIG_PATH, system_cfg_edit)
        st.success("System settings saved.")


with tabs[7]:
    st.subheader("Chat")
    st.info("Chat tab scaffold ready.")

with tabs[8]:
    from core.ui_data_prep import render_data_prep_tab
    render_data_prep_tab()

with tabs[9]:
    st.subheader("Filetypes")
    st.info("Filetypes tab scaffold ready.")