from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, List, Optional


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()

def _image_identity(file_path, source_file):
    source_key = Path(str(file_path or source_file)).stem
    source_key = re.sub(r"[^a-zA-Z0-9_]+", "_", source_key).strip("_").lower()

    identifier = source_key
    identifier_field = "image_file"
    identifier_namespace = "image_file"
    identifier_kind = "generated"
    link_keys = [f"{identifier_namespace}:{identifier}"]

    return {
        "identifier": identifier,
        "identifier_field": identifier_field,
        "identifier_namespace": identifier_namespace,
        "identifier_kind": identifier_kind,
        "link_keys": link_keys,
        "related_link_keys": [],
    }


def _build_combined_text(parsed: Dict[str, Any]) -> str:
    file_name = _safe_str(parsed.get("file_name"))
    doc_type = _safe_str(parsed.get("doc_type"))
    image_mode = _safe_str(parsed.get("image_mode"))

    meta = parsed.get("meta") or {}
    content = parsed.get("content") or {}

    ocr_text = _safe_str(content.get("ocr_text"))
    caption = _safe_str(content.get("caption"))

    parts: List[str] = []

    if file_name:
        parts.append(f"File: {file_name}")

    if doc_type:
        parts.append(f"Document Type: {doc_type}")

    if image_mode:
        parts.append(f"Image Mode: {image_mode}")

    meta_bits = []
    if meta.get("format"):
        meta_bits.append(f"format={_safe_str(meta.get('format'))}")
    if meta.get("width"):
        meta_bits.append(f"width={meta.get('width')}")
    if meta.get("height"):
        meta_bits.append(f"height={meta.get('height')}")
    if meta.get("color_mode"):
        meta_bits.append(f"color_mode={_safe_str(meta.get('color_mode'))}")

    if meta_bits:
        parts.append("Image Metadata: " + ", ".join(meta_bits))

    if caption:
        parts.append("Caption:")
        parts.append(caption)

    if ocr_text:
        parts.append("OCR Text:")
        parts.append(ocr_text)

    return "\n\n".join(part for part in parts if _safe_str(part))


def serialize_image(
    parsed: Dict[str, Any],
    file_path: str | Path,
    template_config: Optional[Dict[str, Any]] = None,
    file_tags: Optional[List[str]] = None,
    collection_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    template_config = template_config or {}
    file_tags = file_tags or []

    file_path = Path(file_path)
    content = parsed.get("content") or {}
    meta = parsed.get("meta") or {}

    combined_text = _build_combined_text(parsed)

    source_file = _safe_str(parsed.get("file_name") or file_path.name)
    source_path = _safe_str(parsed.get("file_path") or str(file_path))
    identity = _image_identity(source_path, source_file)
    primary_name = Path(source_file).stem

    doc = {
        "text": combined_text,
        **identity,
        "file_type": "image",
        "source_type": _safe_str(parsed.get("source_type") or "standalone_image"),
        "primary_name": primary_name,
        "description": combined_text,
        "source_file": source_file,
        "file_name": source_file,
        "file_path": source_path,
        "doc_type": _safe_str(parsed.get("doc_type")),
        "image_mode": _safe_str(parsed.get("image_mode")),
        "format": _safe_str(meta.get("format")),
        "width": meta.get("width"),
        "height": meta.get("height"),
        "color_mode": _safe_str(meta.get("color_mode")),
        "ocr_text": _safe_str(content.get("ocr_text")),
        "caption": _safe_str(content.get("caption")),
        "file_tags": file_tags,
        "collection_name": collection_name,
    }

    from core.payload_utils import enrich_payload_with_common_fields
    enrich_payload_with_common_fields(doc, source_path, template_config)

    return [doc]