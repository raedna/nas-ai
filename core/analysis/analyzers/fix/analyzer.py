from typing import Any, Dict, List

from core.analysis.input.fix_input_normalizer import parse_fix_input
from core.analysis.knowledge.structured_lookup import lookup_fix_tag, lookup_fix_enum
from core.analysis.analyzers.fix.business_object import build_fix_business_object
from core.analysis.analyzers.fix.summary_builder import build_fix_summary


def _payload(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {}
    return row.get("payload") or row.get("field_payload") or {}


def _enum_payload(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {}
    return row.get("enum_payload") or {}


def analyze_fix_message(raw: str) -> Dict[str, Any]:
    pairs = parse_fix_input(raw)

    decoded_rows: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for item in pairs:
        tag = item.get("tag", "")
        value = item.get("value", "")

        tag_row = lookup_fix_tag(tag)
        enum_row = lookup_fix_enum(tag, value)

        tag_payload = _payload(tag_row)
        enum_payload = _enum_payload(enum_row)

        if not tag_payload:
            warnings.append(f"No FIX field definition found for tag {tag}.")

        decoded_rows.append({
            "tag": tag,
            "tag_name": tag_payload.get("primary_name", ""),
            "value": value,
            "value_name": enum_payload.get("enum_name", ""),
            "value_description": enum_payload.get("description", ""),
            "description": tag_payload.get("description", ""),
            "source": tag_payload.get("source_file", ""),
        })

    business_object = build_fix_business_object(decoded_rows)
    summary = build_fix_summary(business_object)

    return {
        "input_type": "fix",
        "parsed_count": len(pairs),
        "summary": summary,
        "business_object": business_object,
        "decoded_rows": decoded_rows,
        "warnings": warnings,
    }
