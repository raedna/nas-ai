"""
core/payload_utils.py
---------------------
Shared payload enrichment for all serializers.

Adds common cross-linking and retrieval fields to any chunk payload:
  - category      : top-level folder name (searchable, folded into nlp_text)
  - folder_path   : full relative path from collection root (stored only)
  - primary_name_field : schema field name for primary_name (display label)
  - type_field    : schema field name for type (display label)

Call enrich_payload_with_common_fields() from any serializer after
building the base payload dict. No hardcoding — all values derived
from file_path + template_config + existing payload fields.
"""

from pathlib import Path
from typing import Dict, Any, Optional


def enrich_payload_with_common_fields(
    payload: Dict[str, Any],
    file_path: str,
    template_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Enrich a chunk payload with common fields used for cross-linking
    and display. Safe to call on any serializer output — only adds
    fields that are not already present.

    Args:
        payload:         The chunk payload dict to enrich (mutated in place).
        file_path:       Absolute path to the source file.
        template_config: The template_config dict from the ingestion pipeline.
                         Should contain 'collection_root' if available.

    Returns:
        The enriched payload dict (same object, mutated).
    """
    template_config = template_config or {}

    # ------------------------------------------------------------------
    # category + folder_path — derived from file_path + collection_root
    # ------------------------------------------------------------------
    if not payload.get("category") and file_path:
        try:
            fp = Path(file_path)
            collection_root = template_config.get("collection_root")
            if collection_root:
                rel = fp.relative_to(Path(collection_root))
                parts = list(rel.parts[:-1])  # drop filename
            else:
                parts = [fp.parent.name] if fp.parent.name else []

            if parts:
                payload["category"] = parts[0]
                payload["folder_path"] = "/".join(parts)
            else:
                payload["category"] = None
                payload["folder_path"] = None
        except Exception:
            payload["category"] = None
            payload["folder_path"] = None

    # ------------------------------------------------------------------
    # primary_name_field — use existing value if already set,
    # otherwise derive from template_config hint (set by table/xml
    # serializers) or leave None.
    # ------------------------------------------------------------------
    if "primary_name_field" not in payload:
        payload["primary_name_field"] = template_config.get("primary_name_field") or None

    # ------------------------------------------------------------------
    # type_field — same pattern as primary_name_field
    # ------------------------------------------------------------------
    if "type_field" not in payload:
        payload["type_field"] = template_config.get("type_field") or None

    return payload