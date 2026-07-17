from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from core.analysis.analyzers.fix.analyzer import analyze_fix_message


def _row_occurrence_key(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, int], Dict[str, Any]]:
    """
    Index rows by tag + occurrence number.

    This preserves repeated FIX tags:
      448#1, 448#2, 448#3
    """
    counts = defaultdict(int)
    indexed = {}

    for row in rows:
        tag = str(row.get("tag") or "").strip()
        if not tag:
            continue

        counts[tag] += 1
        occurrence = counts[tag]

        indexed[(tag, occurrence)] = row

    return indexed


def _display_value(row: Dict[str, Any]) -> str:
    if not row:
        return ""

    value = str(row.get("value") or "").strip()
    value_name = str(row.get("value_name") or "").strip()

    if value and value_name:
        return f"{value} / {value_name}"

    return value or value_name


def _normalized_compare_value(row: Dict[str, Any]) -> str:
    """
    Compare decoded value + decoded meaning.

    This lets repaired rows compare correctly:
      54=1 / Buy
      54=BUY repaired to 1 / Buy
    """
    if not row:
        return ""

    value = str(row.get("value") or "").strip()
    value_name = str(row.get("value_name") or "").strip()

    return f"{value}|{value_name}".lower()


def _get_nested(data: Dict[str, Any], *keys: str) -> str:
    current: Any = data

    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)

    return str(current or "").strip()


def _same_non_empty(a: str, b: str) -> bool:
    return bool(a and b and a == b)


def _parse_fix_time(value: str) -> Optional[datetime]:
    value = str(value or "").strip()

    if not value:
        return None

    formats = [
        "%Y%m%d-%H:%M:%S.%f",
        "%Y%m%d-%H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def _time_difference_seconds(bo_a: Dict[str, Any], bo_b: Dict[str, Any]) -> Optional[float]:
    # Prefer TransactTime, then SendingTime.
    a_time = (
        _get_nested(bo_a, "trade", "transaction_time")
        or _get_nested(bo_a, "message", "sending_time")
    )
    b_time = (
        _get_nested(bo_b, "trade", "transaction_time")
        or _get_nested(bo_b, "message", "sending_time")
    )

    dt_a = _parse_fix_time(a_time)
    dt_b = _parse_fix_time(b_time)

    if not dt_a or not dt_b:
        return None

    return round((dt_b - dt_a).total_seconds(), 6)

def category_for_tag(tag: str) -> str:
    tag = str(tag or "").strip()

    envelope = {
        "8", "9", "10", "34", "43", "52", "122",
    }

    routing = {
        "49", "56", "115", "128", "142", "144",
    }

    identifiers = {
        "11", "17", "37", "41", "76", "198", "526",
    }

    instrument = {
        "22", "48", "55", "65", "106", "167", "200",
        "201", "202", "205", "206", "207", "223",
    }

    order_details = {
        "15", "21", "38", "40", "44", "54", "59",
        "99", "100", "110", "111", "126",
    }

    execution_state = {
        "6", "14", "31", "32", "39", "150", "151",
    }

    timing = {
        "60", "75", "168", "432",
    }

    settlement = {
        "63", "64", "120", "119", "155",
    }

    parties = {
        "447", "448", "452", "453", "523", "802", "803",
    }

    if tag in envelope:
        return "Message Envelope"
    if tag in routing:
        return "Routing"
    if tag in identifiers:
        return "Business Identifiers"
    if tag in instrument:
        return "Instrument"
    if tag in order_details:
        return "Order Details"
    if tag in execution_state:
        return "Execution State"
    if tag in timing:
        return "Timing"
    if tag in settlement:
        return "Settlement"
    if tag in parties:
        return "Parties"

    return "Other"


def _assess_relationship(
    analysis_a: Dict[str, Any],
    analysis_b: Dict[str, Any],
) -> Dict[str, Any]:
    bo_a = analysis_a.get("business_object") or {}
    bo_b = analysis_b.get("business_object") or {}

    reasons: List[str] = []

    # Security relationship checks
    symbol_a = _get_nested(bo_a, "trade", "symbol")
    symbol_b = _get_nested(bo_b, "trade", "symbol")

    security_id_a = _get_nested(bo_a, "trade", "security_id")
    security_id_b = _get_nested(bo_b, "trade", "security_id")

    security_id_source_a = _get_nested(bo_a, "trade", "security_id_source")
    security_id_source_b = _get_nested(bo_b, "trade", "security_id_source")

    if _same_non_empty(security_id_a, security_id_b):
        reasons.append("Same SecurityID")

    if _same_non_empty(security_id_source_a, security_id_source_b):
        reasons.append("Same SecurityIDSource")

    if _same_non_empty(symbol_a, symbol_b):
        reasons.append("Same Symbol")

    # Order/execution relationship checks
    clordid_a = _get_nested(bo_a, "order", "client_order_id")
    clordid_b = _get_nested(bo_b, "order", "client_order_id")

    orderid_a = _get_nested(bo_a, "order", "order_id")
    orderid_b = _get_nested(bo_b, "order", "order_id")

    secondary_order_id_a = _get_nested(bo_a, "order", "secondary_order_id")
    secondary_order_id_b = _get_nested(bo_b, "order", "secondary_order_id")

    secondary_clordid_a = _get_nested(bo_a, "order", "secondary_client_order_id")
    secondary_clordid_b = _get_nested(bo_b, "order", "secondary_client_order_id")

    execid_a = _get_nested(bo_a, "order", "execution_id")
    execid_b = _get_nested(bo_b, "order", "execution_id")

    if _same_non_empty(clordid_a, clordid_b):
        reasons.append("Same ClOrdID")

    if _same_non_empty(orderid_a, orderid_b):
        reasons.append("Same OrderID")

    if _same_non_empty(secondary_order_id_a, secondary_order_id_b):
        reasons.append("Same SecondaryOrderID")

    if _same_non_empty(secondary_clordid_a, secondary_clordid_b):
        reasons.append("Same SecondaryClOrdID")

    if _same_non_empty(execid_a, execid_b):
        reasons.append("Same ExecID")

    # Routing relationship checks
    sender_a = _get_nested(bo_a, "message", "sender")
    target_a = _get_nested(bo_a, "message", "target")
    sender_b = _get_nested(bo_b, "message", "sender")
    target_b = _get_nested(bo_b, "message", "target")

    routing_reversed = bool(sender_a and target_a and sender_b and target_b and sender_a == target_b and target_a == sender_b)

    if routing_reversed:
        reasons.append("Routing is reversed")

    strong_identifier_reasons = {
        "Same ClOrdID",
        "Same OrderID",
        "Same SecondaryOrderID",
        "Same SecondaryClOrdID",
        "Same ExecID",
    }

    has_strong_identifier = any(reason in strong_identifier_reasons for reason in reasons)
    has_security_match = any(reason in {"Same SecurityID", "Same Symbol"} for reason in reasons)

    if has_strong_identifier and has_security_match:
        relationship = "Likely related"
    elif has_strong_identifier:
        relationship = "Likely related"
    elif has_security_match:
        relationship = "Possibly related"
    else:
        relationship = "Weak / unrelated"

    msgtype_a = _get_nested(bo_a, "message", "type")
    msgtype_b = _get_nested(bo_b, "message", "type")
    exec_type_b = _get_nested(bo_b, "order", "execution_type")
    ord_status_b = _get_nested(bo_b, "order", "order_status")

    interpretation = ""

    if relationship == "Likely related" and routing_reversed:
        if msgtype_a == "NewOrderSingle" and msgtype_b == "ExecutionReport":
            if exec_type_b or ord_status_b:
                interpretation = (
                    "Message B appears to be an acknowledgement/response to Message A "
                    f"with ExecType '{exec_type_b}' and OrdStatus '{ord_status_b}'."
                )
            else:
                interpretation = "Message B appears to be an acknowledgement/response to Message A."
        else:
            interpretation = "Message B appears to be a response to Message A based on reversed routing and matching identifiers."

    elif relationship == "Likely related":
        interpretation = "Messages appear to belong to the same order/execution sequence."

    elif relationship == "Possibly related":
        interpretation = "Messages share security details but do not have a strong matching order/execution identifier."

    else:
        interpretation = "Messages do not have enough matching identifiers to confirm they are related."

    time_difference = _time_difference_seconds(bo_a, bo_b)

    return {
        "relationship": relationship,
        "reasons": reasons,
        "routing_reversed": routing_reversed,
        "time_difference_seconds": time_difference,
        "interpretation": interpretation,
    }

def _is_repeating_group_tag(tag: str) -> bool:
    """
    Tags where occurrence numbers are useful to show in the UI.
    This does not affect comparison logic; it only affects display.
    """
    repeating_tags = {
        # Parties
        "448",  # PartyID
        "447",  # PartyIDSource
        "452",  # PartyRole
        "453",  # NoPartyIDs

        # Party sub IDs
        "523",  # PartySubID
        "803",  # PartySubIDType
        "802",  # NoPartySubIDs

        # Common allocation / leg repeating groups, useful later
        "78",   # NoAllocs
        "79",   # AllocAccount
        "80",   # AllocQty
        "555",  # NoLegs
        "600",  # LegSymbol
        "624",  # LegSide
    }

    return str(tag) in repeating_tags

def _combined_warning(row: Dict[str, Any] | None) -> str:
    if not row:
        return ""

    warnings = []

    for key in ("tag_warning", "enum_warning", "ocr_repair_warning"):
        value = str(row.get(key) or "").strip()
        if value:
            warnings.append(value)

    return " | ".join(warnings)

def compare_fix_messages(raw_a: str, raw_b: str) -> Dict[str, Any]:
    analysis_a = analyze_fix_message(raw_a)
    analysis_b = analyze_fix_message(raw_b)

    rows_a = analysis_a.get("decoded_rows") or []
    rows_b = analysis_b.get("decoded_rows") or []

    indexed_a = _row_occurrence_key(rows_a)
    indexed_b = _row_occurrence_key(rows_b)

    # Preserve Message A order first, then append Message B-only keys
    # in their original Message B order.
    all_keys = []

    for key in indexed_a.keys():
        if key not in all_keys:
            all_keys.append(key)

    for key in indexed_b.keys():
        if key not in all_keys:
            all_keys.append(key)

    tag_occurrence_counts = defaultdict(int)

    for tag, occurrence in all_keys:
        tag_occurrence_counts[tag] = max(tag_occurrence_counts[tag], occurrence)

    comparison_rows: List[Dict[str, Any]] = []

    for tag, occurrence in all_keys:
        row_a = indexed_a.get((tag, occurrence), {})
        row_b = indexed_b.get((tag, occurrence), {})

        value_a = _normalized_compare_value(row_a)
        value_b = _normalized_compare_value(row_b)

        if row_a and not row_b:
            status = "Only in A"
        elif row_b and not row_a:
            status = "Only in B"
        elif value_a == value_b:
            status = "Same"
        else:
            status = "Different"

        tag_name = (
            row_a.get("tag_name")
            or row_b.get("tag_name")
            or ""
        )

        show_occurrence = (
            tag_occurrence_counts[tag] > 1
            or _is_repeating_group_tag(tag)
        )

        display_key = f"{tag}#{occurrence}" if show_occurrence else tag

        comparison_rows.append({
            "key": f"{tag}#{occurrence}",
            "display_key": display_key,
            "tag": tag,
            "tag_name": tag_name,
            "occurrence": occurrence,
            "category": category_for_tag(tag),
            "value_a": row_a.get("value", ""),
            "value_name_a": row_a.get("value_name", ""),
            "display_a": _display_value(row_a),
            "value_b": row_b.get("value", ""),
            "value_name_b": row_b.get("value_name", ""),
            "display_b": _display_value(row_b),
            "status": status,
            "warning_a": _combined_warning(row_a),
            "warning_b": _combined_warning(row_b),
        })

    difference_rows = [
        row for row in comparison_rows
        if row.get("status") != "Same"
    ]

    difference_counts_by_category = dict(
        Counter(row.get("category", "Other") for row in difference_rows)
    )

    relationship = _assess_relationship(analysis_a, analysis_b)

    if comparison_rows and len(difference_rows) == 0:
        relationship = {
            **relationship,
            "relationship": "Exact duplicate",
            "strength": "exact",
            "interpretation": (
                "These FIX messages are identical across all compared tag occurrences."
            ),
            "warning": "",
        }

    summary = (
        f"{relationship.get('relationship')}. "
        f"Compared {len(comparison_rows)} tag occurrences. "
        f"Found {len(difference_rows)} differences."
    )

    return {
        "input_type": "fix_compare",
        "summary": summary,
        "relationship": relationship,
        "analysis_a": analysis_a,
        "analysis_b": analysis_b,
        "comparison_rows": comparison_rows,
        "difference_rows": difference_rows,
        "difference_count": len(difference_rows),
        "total_count": len(comparison_rows),
        "difference_counts_by_category": difference_counts_by_category,
        "warnings": {
            "a": analysis_a.get("warnings") or [],
            "b": analysis_b.get("warnings") or [],
        },
    }