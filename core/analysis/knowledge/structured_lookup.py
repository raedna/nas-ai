from typing import Any, Dict, List, Optional

from core.collection_config import load_collections
from core.db import fetchall, fetchone


def get_fix_dictionary_collections() -> List[str]:
    collections = load_collections()
    matches: List[str] = []

    for name, cfg in collections.items():
        searchable = " ".join([
            name,
            str(cfg.get("source_label", "")),
            str(cfg.get("notes", "")),
            str(cfg.get("routing_description", "")),
        ]).lower()

        if "fix" in searchable or "fix" in name.lower():
            matches.append(name)

    return matches


def _find_collections_by_hint(*hints: str) -> List[str]:
    collections = load_collections()
    matches: List[str] = []

    for name, cfg in collections.items():
        searchable = " ".join([
            name,
            str(cfg.get("source_label", "")),
            str(cfg.get("notes", "")),
            str(cfg.get("routing_description", "")),
            str(cfg.get("source_file", "")),
            str(cfg.get("path", "")),
        ]).lower()

        if all(h.lower() in searchable for h in hints):
            matches.append(name)

    return matches


def get_fix_field_collections() -> List[str]:
    matches = _find_collections_by_hint("fix", "field")
    return matches or get_fix_dictionary_collections()


def get_fix_enum_collections() -> List[str]:
    matches = _find_collections_by_hint("fix", "enum")
    return matches or get_fix_dictionary_collections()

def lookup_fix_tag_by_name(tag_name: str) -> Optional[Dict[str, Any]]:
    """
    Look up a FIX dictionary field by primary_name.

    Used mainly for OCR repair:
    if OCR reads "6 BodyLength 242", dictionary can tell us
    BodyLength is actually tag 9, not tag 6.
    """
    tag_name = str(tag_name or "").strip()

    if not tag_name:
        return None

    collections = get_fix_dictionary_collections()

    return fetchone("""
        SELECT collection_name, payload
        FROM chunks
        WHERE collection_name = ANY(%s)
          AND payload->>'source_file' = '_merged_fields'
          AND payload->>'identifier_namespace' = 'tag'
          AND lower(payload->>'primary_name') = lower(%s)
        LIMIT 1
    """, (collections, tag_name))

def lookup_fix_tag(tag: str) -> Optional[Dict[str, Any]]:
    """
    Lookup FIX field definition by tag number from merged FIX fields.
    """
    if not tag:
        return None

    collections = get_fix_dictionary_collections()

    return fetchone("""
        SELECT collection_name, payload
        FROM chunks
        WHERE collection_name = ANY(%s)
          AND payload->>'identifier' = %s
          AND payload->>'source_file' = '_merged_fields'
          AND payload->>'identifier_namespace' = 'tag'
        LIMIT 1
    """, (collections, str(tag)))


def lookup_fix_enum(tag: str, value: str) -> Optional[Dict[str, Any]]:
    """
    Lookup FIX enum value from the enum_values array inside _merged_fields.
    """
    if not tag or value is None:
        return None

    collections = get_fix_dictionary_collections()

    return fetchone("""
        SELECT
            c.collection_name,
            c.payload AS field_payload,
            enum_item AS enum_payload
        FROM chunks c
        CROSS JOIN LATERAL jsonb_array_elements(c.payload->'enum_values') AS enum_item
        WHERE c.collection_name = ANY(%s)
          AND c.payload->>'identifier' = %s
          AND c.payload->>'source_file' = '_merged_fields'
          AND c.payload->>'identifier_namespace' = 'tag'
          AND enum_item->>'enum_value' = %s
        LIMIT 1
    """, (collections, str(tag), str(value)))