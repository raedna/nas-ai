"""
core/retrieval/answer.py
=========================
Answer synthesis — turns a payload dict + roles into a human-readable string.

Extracted verbatim from query_router.py (synthesize_answer, build_answer,
get_source_label, get_display_labels, dedupe_repeated_paragraphs).
No logic changes, no hardcoding.

Entry points:
  synthesize_answer(payload, roles, collection_name) -> str
  build_answer(points, roles) -> str  [legacy shim, used by older callers]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from core.query_helpers import infer_doc_type, normalize_simple_text
from core.collection_config import get_collection
from core.schema_loader import load_collection_schemas
from core.paths import CONFIG_DIR


# ---------------------------------------------------------------------------
# Label helpers
# ---------------------------------------------------------------------------

def load_source_labels() -> Dict:
    with open(CONFIG_DIR / "source_labels.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_source_label(collection_name: str, payload: Dict) -> str:
    """Return a human-friendly source label for the payload."""
    collection_cfg = get_collection(collection_name) or {}
    collection_source_label = (collection_cfg.get("source_label") or "").strip()
    if collection_source_label:
        return collection_source_label

    labels_cfg = load_source_labels()

    subtype = str(payload.get("subtype") or "").lower()
    source_type = str(payload.get("source_type") or "").lower()
    doc_type = str(payload.get("doc_type") or "").lower()
    source_file = payload.get("source_file") or "unknown source"

    subtype_map = labels_cfg.get("subtype", {})
    source_type_map = labels_cfg.get("source_type", {})
    doc_type_map = labels_cfg.get("doc_type", {})

    if subtype in subtype_map:
        return subtype_map[subtype]
    if source_type in source_type_map:
        return source_type_map[source_type]
    if doc_type in doc_type_map:
        return doc_type_map[doc_type]

    return f"Source file: {source_file}"


def get_display_labels(collection_name: str) -> Dict[str, str]:
    """Return display label strings for identifier / name / enum fields."""
    schemas = load_collection_schemas(collection_name)

    identifier_label = "identifier"
    primary_name_label = "name"
    enum_value_label = "value"
    enum_name_label = "label"

    for _, schema in schemas.items():
        if schema.get("identifier"):
            identifier_label = schema["identifier"][0]
        if schema.get("primary_name"):
            primary_name_label = schema["primary_name"][0]
        if schema.get("enum_value"):
            enum_value_label = schema["enum_value"][0]
        if schema.get("enum_name"):
            enum_name_label = schema["enum_name"][0]
        break

    return {
        "identifier": identifier_label,
        "primary_name": primary_name_label,
        "enum_value": enum_value_label,
        "enum_name": enum_name_label,
    }


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def dedupe_repeated_paragraphs(text: str) -> str:
    """Remove duplicate paragraphs (double-newline separated) from text."""
    paragraphs = [p.strip() for p in str(text or "").split("\n\n")]
    cleaned = []
    seen: set = set()

    for p in paragraphs:
        if not p:
            continue
        key = normalize_simple_text(p)
        if key and key in seen:
            continue
        seen.add(key)
        cleaned.append(p)

    return "\n\n".join(cleaned).strip()


def _question_requests_enums(question: str) -> bool:
    """Return True if the question is explicitly asking for enum values."""
    if not question:
        return False
    q = question.lower()
    enum_triggers = [
        "what values", "which values", "allowed values",
        "possible values", "can have", "enum", "values can",
        "values does", "values for",
    ]
    return any(trigger in q for trigger in enum_triggers)


# ---------------------------------------------------------------------------
# Main synthesizer
# ---------------------------------------------------------------------------

def synthesize_answer(payload: Dict, roles: List[str], collection_name: str) -> str:
    """
    Build a human-readable answer string from a retrieved payload.

    Handles six payload shapes:
      1. Structured (doc_type == "structured") — FIX tags, BBG fields, etc.
      2. Enum request ("enum_value" in roles)
      3. Description request ("description" in roles)
      4. Entity row (doc_type == "entity_row") — knowledge base articles
      5. Chunked document (has section_heading / block_types)
      6. Image (source_type == "image" / "standalone_image")
      7. Default structured fallback
    """

    # -----------------------------------------------------------------------
    # 1. STRUCTURED doc_type path (FIX / BBG field records)
    # -----------------------------------------------------------------------
    if infer_doc_type(payload) == "structured":
        identifier_field = payload.get("identifier_field") or "identifier"
        identifier_value = payload.get("identifier")
        primary_name = payload.get("primary_name")
        description = payload.get("description")
        enum_values = payload.get("enum_values") or []

        question = payload.get("_question", "")
        include_enums = "enum_value" in roles or _question_requests_enums(question)

        lines = []

        if identifier_value and primary_name:
            lines.append(f"{identifier_field} {identifier_value} is {primary_name}.")
        elif primary_name:
            lines.append(str(primary_name))
        elif identifier_value:
            lines.append(f"{identifier_field} {identifier_value}.")

        if description:
            description_fields = payload.get("description_fields") or {}
            if isinstance(description_fields, dict) and description_fields:
                lines.append("")
                for field_name, field_value in description_fields.items():
                    lines.append(f"{field_name}: {field_value}")
            else:
                lines.append(f"\nDescription: {description}")
            type_value = payload.get("type")
            if type_value and type_value != "structured":
                lines.append(f"\nType: {type_value}")

        if include_enums and enum_values:
            lines.append("\nAllowed values:")
            for e in enum_values:
                if isinstance(e, dict):
                    val = e.get("enum_value")
                    name = e.get("enum_name")
                    desc = e.get("description")

                    if val and name and desc:
                        lines.append(f"- {val}: {name} — {desc}")
                    elif val and name:
                        lines.append(f"- {val}: {name}")
                    elif val:
                        lines.append(f"- {val}")
                    elif name:
                        lines.append(f"- {name}")
                else:
                    lines.append(f"- {e}")

        answer = "\n\n".join(lines)
        return dedupe_repeated_paragraphs(answer)

    # -----------------------------------------------------------------------
    # Shared label lookups for paths 2–7
    # -----------------------------------------------------------------------
    labels = get_display_labels(collection_name)
    identifier_label = labels["identifier"]
    primary_name_label = labels["primary_name"]
    enum_value_label = labels["enum_value"]
    enum_name_label = labels["enum_name"]

    identifier = payload.get("identifier")
    name = payload.get("primary_name")
    description = payload.get("description")
    enums = payload.get("enum_values")
    doc_type = payload.get("doc_type")
    source_label = get_source_label(collection_name, payload)
    source_type = str(payload.get("source_type") or "").lower()

    # -----------------------------------------------------------------------
    # 2. ENUM REQUEST
    # -----------------------------------------------------------------------
    if "enum_value" in roles:
        if enums:
            values = []
            for e in enums:
                if isinstance(e, dict):
                    val = e.get("Value") or e.get(enum_value_label) or e.get("value")
                    label = e.get("SymbolicName") or e.get(enum_name_label) or e.get("name")
                    if val and label:
                        values.append(f"{val}={label}")
                    elif val:
                        values.append(str(val))
                    elif label:
                        values.append(str(label))
                else:
                    values.append(str(e))

            preview = ", ".join(values[:30])
            if len(values) > 30:
                preview += ", ..."

            if identifier and name:
                return f"Allowed values for {identifier_label} {identifier} ({name}) are: {preview}"
            return f"Allowed values are: {preview}"

        return "No enumerated values found."

    # -----------------------------------------------------------------------
    # 3. DESCRIPTION REQUEST
    # -----------------------------------------------------------------------
    if "description" in roles:
        if identifier and name:
            return f"{identifier_label} {identifier} ({name}): {description}"
        if name:
            return f"{name}: {description}"
        return description or "No description available."

    # -----------------------------------------------------------------------
    # 4. ENTITY ROW (knowledge base articles, Obsidian notes, etc.)
    # -----------------------------------------------------------------------
    if doc_type == "entity_row":
        parts = [f"Source: {source_label}"]

        if name:
            parts.append(name)
        if description:
            parts.append(description)

        related_titles = payload.get("related_titles") or []
        if related_titles:
            related_preview = "\n".join(f"- {t}" for t in related_titles[:5])
            parts.append(f"Related articles:\n{related_preview}")

        return "\n\n".join(parts) if parts else "No answer found."

    # -----------------------------------------------------------------------
    # 5. CHUNKED DOCUMENT (doc, pdf, md with section_heading / block_types)
    # -----------------------------------------------------------------------
    is_chunked_doc_like = (
        bool(payload.get("section_heading") or payload.get("block_types"))
        and doc_type != "entity_row"
        and str(payload.get("file_type") or "").lower() != "image"
        and source_type not in ["image", "standalone_image"]
    )

    if is_chunked_doc_like:
        parts = [f"Source: {source_label}"]

        if name:
            parts.append(name)

        desc_text = str(description or payload.get("text") or "").strip()
        desc_text = dedupe_repeated_paragraphs(desc_text)

        if desc_text:
            lines = [ln.rstrip() for ln in desc_text.splitlines()]
            cleaned = []
            title_norm = normalize_simple_text(name)
            last_heading_norm = None
            heading_like_norms = {"steps to resolve"}

            for ln in lines:
                stripped = ln.strip()
                if not stripped:
                    cleaned.append("")
                    continue

                stripped_norm = normalize_simple_text(stripped)

                if stripped_norm == title_norm:
                    continue

                if title_norm and stripped_norm == f"title {title_norm}":
                    continue

                if stripped.lower().startswith("title:"):
                    maybe_title = stripped.split(":", 1)[1].strip()
                    if normalize_simple_text(maybe_title) == title_norm:
                        continue

                is_heading_like = (
                    stripped_norm in heading_like_norms
                    or stripped.endswith(":")
                )

                if is_heading_like:
                    if stripped_norm == last_heading_norm:
                        continue
                    last_heading_norm = stripped_norm

                cleaned.append(stripped)

            desc_text = "\n".join(cleaned).strip()
            if desc_text:
                parts.append(desc_text)

        related_titles = payload.get("related_titles") or []
        if related_titles:
            related_preview = "\n".join(f"- {t}" for t in related_titles[:5])
            parts.append(f"Related notes:\n{related_preview}")

        answer = "\n\n".join(parts) if parts else "No answer found."
        return dedupe_repeated_paragraphs(answer)

    # -----------------------------------------------------------------------
    # 6. IMAGE
    # -----------------------------------------------------------------------
    if source_type in ["image", "standalone_image"]:
        parts = [f"Source: {source_label}"]

        file_name = payload.get("file_name") or payload.get("source_file")
        image_mode = payload.get("image_mode")
        ocr_text = payload.get("ocr_text") or payload.get("text") or ""
        caption = payload.get("caption") or ""
        _doc_type = payload.get("doc_type")

        if file_name:
            parts.append(f"Image: {file_name}")

        meta_bits = []
        if payload.get("format"):
            meta_bits.append(f"format={payload.get('format')}")
        if payload.get("width"):
            meta_bits.append(f"width={payload.get('width')}")
        if payload.get("height"):
            meta_bits.append(f"height={payload.get('height')}")

        summary_bits = []
        if _doc_type:
            summary_bits.append(f"doc_type={_doc_type}")
        if image_mode:
            summary_bits.append(f"image_mode={image_mode}")

        if summary_bits:
            parts.append("Image classification: " + ", ".join(summary_bits))

        if meta_bits:
            parts.append("Image metadata: " + ", ".join(meta_bits))

        if caption:
            parts.append(caption)

        if ocr_text:
            parts.append(ocr_text[:1500].strip())

        return "\n\n".join(parts) if parts else "No answer found."

    # -----------------------------------------------------------------------
    # 7. DEFAULT STRUCTURED FALLBACK
    # -----------------------------------------------------------------------
    parts = [f"Source: {source_label}"]

    if identifier and name:
        parts.append(f"The {primary_name_label} with {identifier_label} {identifier} is {name}.")
    elif name:
        parts.append(f"{primary_name_label}: {name}.")
    elif identifier:
        parts.append(f"{identifier_label}: {identifier}.")

    if description:
        parts.append(f"Its description is: {description.rstrip('.')}.")

    return " ".join(parts) if parts else "No answer found."


# ---------------------------------------------------------------------------
# Legacy shim — kept for any callers that still use build_answer()
# ---------------------------------------------------------------------------

def build_answer(points: List, roles: List[str]) -> str:
    """
    Simplified answer builder for legacy callers.
    Prefers synthesize_answer() via the first point's payload.
    """
    if not points:
        return "No answer found"

    p = points[0].payload

    if roles:
        parts = []

        if "identifier" in p and "primary_name" in p:
            parts.append(f"Tag {p['identifier']} is {p['primary_name']}")

        if "description" in roles and "description" in p:
            parts.append(p["description"])

        if "enum_value" in roles and p.get("enum_values"):
            enum_texts = []
            for e in p["enum_values"]:
                if isinstance(e, dict):
                    vals = list(e.values())
                    if len(vals) >= 2:
                        enum_texts.append(f"{vals[0]}={vals[1]}")
                    else:
                        enum_texts.append(str(vals[0]))
                else:
                    enum_texts.append(str(e))
            parts.append(" | ".join(enum_texts))

        return " | ".join(parts)

    parts = []

    if "identifier" in p and "primary_name" in p:
        parts.append(f"Tag {p['identifier']} is {p['primary_name']}")
    elif "identifier" in p:
        parts.append(f"{p['identifier']}")
    elif "primary_name" in p:
        parts.append(f"{p['primary_name']}")

    if "description" in p:
        parts.append(f"{p['description']}")

    if p.get("enum_values"):
        enum_texts = []
        for e in p["enum_values"]:
            if isinstance(e, dict):
                enum_texts.append(", ".join(str(v) for v in e.values()))
            else:
                enum_texts.append(str(e))
        parts.append(" | ".join(enum_texts))

    return " | ".join(parts) if parts else "No answer found"
