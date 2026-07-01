from typing import Any, Dict, List

from core.analysis.input.fix_input_normalizer import parse_fix_input
from core.analysis.knowledge.structured_lookup import lookup_fix_tag, lookup_fix_enum
from core.analysis.analyzers.fix.business_object import build_fix_business_object
from core.analysis.analyzers.fix.summary_builder import build_fix_summary
from difflib import SequenceMatcher
from core.analysis.analyzers.fix.value_validator import validate_fix_decoded_rows


def _payload(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {}
    return row.get("payload") or row.get("field_payload") or {}


def _enum_payload(row: Dict[str, Any] | None) -> Dict[str, Any]:
    if not row:
        return {}
    return row.get("enum_payload") or {}

def _clean_compare_text(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _similarity(a: str, b: str) -> float:
    a_clean = _clean_compare_text(a)
    b_clean = _clean_compare_text(b)

    if not a_clean or not b_clean:
        return 0.0

    return SequenceMatcher(None, a_clean, b_clean).ratio()


def _infer_enum_from_ocr_tail(tag_payload: dict, value_tail: str) -> dict:
    """
    Dictionary-based OCR repair.

    If OCR misreads the enum value but captures the value name badly,
    use the field's enum_values to infer the intended enum.
    No hardcoded FIX meanings.
    """
    if not tag_payload or not value_tail:
        return {}

    enum_values = tag_payload.get("enum_values") or []
    if not isinstance(enum_values, list):
        return {}

    best = None
    best_score = 0.0

    for enum_item in enum_values:
        if not isinstance(enum_item, dict):
            continue

        enum_name = enum_item.get("enum_name", "")
        enum_description = enum_item.get("description", "")

        score = max(
            _similarity(value_tail, enum_name),
            _similarity(value_tail, enum_description),
        )

        if score > best_score:
            best = enum_item
            best_score = score

    # Keep this reasonably strict so we don't invent meanings.
    if best and best_score >= 0.55:
        return {
            "enum_name": best.get("enum_name", ""),
            "enum_value": best.get("enum_value", ""),
            "description": best.get("description", ""),
            "ocr_inferred": True,
            "ocr_score": round(best_score, 3),
        }

    return {}

def _is_allowed_custom_enum_value(tag_payload: Dict[str, Any], value: str) -> bool:
    """
    Detect dictionary-described custom enum ranges without hardcoding tag numbers.

    Example descriptions may say values 4000+ are reserved for
    user-defined or bilaterally agreed values.
    """
    try:
        numeric_value = int(str(value).strip())
    except Exception:
        return False

    enum_values = tag_payload.get("enum_values") or []

    text_parts = [
        str(tag_payload.get("description") or ""),
        str(tag_payload.get("text") or ""),
        str(tag_payload.get("notes") or ""),
    ]

    for enum_item in enum_values:
        if isinstance(enum_item, dict):
            text_parts.append(str(enum_item.get("description") or ""))
            text_parts.append(str(enum_item.get("enum_name") or ""))

    combined = " ".join(text_parts).lower()

    custom_terms = [
        "user defined",
        "user-defined",
        "bilaterally agreed",
        "bilateral",
        "mutually agreed",
        "reserved",
    ]

    has_custom_language = any(term in combined for term in custom_terms)

    # Common FIX wording: 4000+, 4000 and above, values >= 4000, etc.
    allows_4000_plus = (
        "4000+" in combined
        or "4000 and above" in combined
        or "4000 or above" in combined
        or ">= 4000" in combined
        or "greater than or equal to 4000" in combined
    )

    return has_custom_language and allows_4000_plus and numeric_value >= 4000

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

        # If OCR produced an invalid enum value, try dictionary-based repair
        # using the remaining OCR text from the row.
        if not enum_payload:
            inferred_enum = _infer_enum_from_ocr_tail(
                tag_payload,
                item.get("value_tail", ""),
            )

            if inferred_enum:
                value = inferred_enum.get("enum_value", value)
                enum_payload = inferred_enum

        tag_warning = ""

        if not tag_payload:
            tag_warning = f"Custom or unknown FIX tag {tag}: no dictionary definition found."

        # Enum validation
        enum_values = tag_payload.get("enum_values") or []
        has_enums = isinstance(enum_values, list) and len(enum_values) > 0

        enum_valid = ""
        enum_warning = ""

        if has_enums:
            valid_enum_values = {
                str(enum_item.get("enum_value"))
                for enum_item in enum_values
                if isinstance(enum_item, dict) and enum_item.get("enum_value") is not None
            }

            if str(value) in valid_enum_values:
                enum_valid = True
            elif enum_payload:
                enum_valid = True
            else:
                tag_description = str(tag_payload.get("description") or "").strip()

                expected_preview = ", ".join(sorted(valid_enum_values)[:20])
                if len(valid_enum_values) > 20:
                    expected_preview += ", ..."

                if tag_description:
                    enum_valid = "Review"
                    enum_warning = (
                        f"Value '{value}' is not listed in the enum dictionary for tag {tag} "
                        f"({tag_payload.get('primary_name', '')}). "
                        f"Check the tag description for allowed custom/range values. "
                        f"Listed enum values include: {expected_preview}"
                    )
                else:
                    enum_valid = False
                    enum_warning = (
                        f"Value '{value}' is not valid for enum tag {tag} "
                        f"({tag_payload.get('primary_name', '')}). "
                        f"Expected one of: {expected_preview}"
                    )

                    warnings.append(enum_warning)

        decoded_rows.append({
            "tag": tag,
            "tag_name": tag_payload.get("primary_name", ""),
            "value": value,
            "value_name": enum_payload.get("enum_name", ""),
            "value_description": enum_payload.get("description", ""),
            "description": tag_payload.get("description", ""),
            "source": tag_payload.get("source_file", ""),
            "tag_known": bool(tag_payload),
            "tag_status": "Known" if tag_payload else "Custom/Unknown",
            "tag_warning": tag_warning,
            "has_enums": has_enums,
            "enum_valid": enum_valid,
            "enum_warning": enum_warning,
            "ocr_inferred": enum_payload.get("ocr_inferred", False),
            "ocr_score": enum_payload.get("ocr_score", ""),
        })

    dictionary_hits = sum(1 for r in decoded_rows if r.get("tag_name"))
    dictionary_misses = sum(1 for r in decoded_rows if not r.get("tag_name"))
    enum_hits = sum(1 for r in decoded_rows if r.get("value_name"))

    business_object = build_fix_business_object(decoded_rows)
    summary = build_fix_summary(business_object)

    return {
        "input_type": "fix",
        "parsed_count": len(pairs),
        "dictionary_hits": dictionary_hits,
        "dictionary_misses": dictionary_misses,
        "enum_hits": enum_hits,
        "summary": summary,
        "business_object": business_object,
        "decoded_rows": decoded_rows,
        "warnings": warnings,
    }