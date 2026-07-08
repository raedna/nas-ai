from typing import Any, Dict, List

from core.analysis.input.fix_message_splitter import split_fix_messages
from core.analysis.analyzers.fix.analyzer import analyze_fix_message


def _get_nested(data: Dict[str, Any], *keys: str) -> str:
    current: Any = data

    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)

    return str(current or "").strip()

def _to_float(value: Any):
    value = str(value or "").strip()

    if not value:
        return None

    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None

def _format_number(value: float) -> str:
    if value == int(value):
        return f"{int(value)}"
    return f"{value:g}"

def _build_sequence_warnings(messages: List[Dict[str, Any]]) -> List[str]:
    warnings: List[str] = []

    seen_exec_ids = set()
    last_cum_qty_by_order = {}
    previous_seq_num_by_route = {}

    for msg in messages:
        message_index = msg.get("message_index")
        sender = msg.get("sender") or ""
        target = msg.get("target") or ""
        route_key = f"{sender}->{target}"

        msg_seq_num = _to_float(msg.get("msg_seq_num"))

        if sender and target and msg_seq_num is not None:
            previous_seq_num = previous_seq_num_by_route.get(route_key)

            if previous_seq_num is not None and msg_seq_num > previous_seq_num + 1:
                warnings.append(
                    f"Message {message_index}: MsgSeqNum jumped from {previous_seq_num:g} to {msg_seq_num:g} "
                    f"for route {route_key}. There may be missing messages."
                )

            if previous_seq_num is not None and msg_seq_num <= previous_seq_num:
                warnings.append(
                    f"Message {message_index}: MsgSeqNum {msg_seq_num:g} is not greater than previous "
                    f"MsgSeqNum {previous_seq_num:g} for route {route_key}. Confirm sequence order."
                )

            previous_seq_num_by_route[route_key] = msg_seq_num
        order_key = (
            msg.get("order_id")
            or msg.get("cl_ord_id")
            or f"message-{message_index}"
        )

        exec_id = msg.get("exec_id")
        if exec_id:
            if exec_id in seen_exec_ids:
                warnings.append(
                    f"Message {message_index}: duplicate ExecID {exec_id} found in the sequence."
                )
            seen_exec_ids.add(exec_id)

        order_qty = _to_float(msg.get("order_qty"))
        cum_qty = _to_float(msg.get("cum_qty"))
        leaves_qty = _to_float(msg.get("leaves_qty"))

        if order_qty is not None and cum_qty is not None and leaves_qty is None:
            expected_leaves_qty = order_qty - cum_qty

            if expected_leaves_qty >= 0:
                warnings.append(
                    f"Message {message_index}: LeavesQty is missing, but OrderQty ({_format_number(order_qty)}) "
                    f"and CumQty ({_format_number(cum_qty)}) are present. Expected LeavesQty may be "
                    f"{_format_number(expected_leaves_qty)}. Confirm OCR/source."
                )

        if cum_qty is not None:
            previous_cum_qty = last_cum_qty_by_order.get(order_key)

            if previous_cum_qty is not None and cum_qty < previous_cum_qty:
                warnings.append(
                    f"Message {message_index}: CumQty decreased from {previous_cum_qty:g} to {cum_qty:g} "
                    f"for order {order_key}. Confirm sequence order or OCR."
                )

            last_cum_qty_by_order[order_key] = cum_qty

    return warnings

def _sequence_sort_key(msg: Dict[str, Any]):
    time_value = str(msg.get("transact_time") or msg.get("sending_time") or "").strip()
    seq_num = _to_float(msg.get("msg_seq_num"))

    return (
        0 if time_value else 1,
        time_value,
        seq_num if seq_num is not None else 999999999,
        msg.get("message_index") or 999999999,
    )

def _get_tag_value(decoded_rows: List[Dict[str, Any]], tag: str) -> str:
    tag = str(tag)

    for row in decoded_rows or []:
        if str(row.get("tag") or "") == tag:
            return str(row.get("value") or "").strip()

    return ""

def _message_group_key(msg: Dict[str, Any]) -> str:
    """
    Conservative grouping key.

    Prefer order identifiers that should represent the same lifecycle.
    ExecID is not used as the primary group because every fill can have a unique ExecID.
    """
    order_id = str(msg.get("order_id") or "").strip()
    secondary_order_id = str(msg.get("secondary_order_id") or "").strip()
    cl_ord_id = str(msg.get("cl_ord_id") or "").strip()

    if order_id:
        return f"order_id:{order_id}"

    if secondary_order_id:
        return f"secondary_order_id:{secondary_order_id}"

    if cl_ord_id:
        return f"cl_ord_id:{cl_ord_id}"

    return f"ungrouped:message:{msg.get('message_index')}"


def _message_group_label(msg: Dict[str, Any]) -> str:
    order_id = str(msg.get("order_id") or "").strip()
    secondary_order_id = str(msg.get("secondary_order_id") or "").strip()
    cl_ord_id = str(msg.get("cl_ord_id") or "").strip()

    parts = []

    if cl_ord_id:
        parts.append(f"ClOrdID {cl_ord_id}")

    if order_id:
        parts.append(f"OrderID {order_id}")

    if secondary_order_id:
        parts.append(f"SecondaryOrderID {secondary_order_id}")

    if parts:
        return " / ".join(parts)

    return f"Ungrouped message {msg.get('message_index')}"


def _message_identifier_tokens(msg: Dict[str, Any]) -> List[str]:
    tokens: List[str] = []

    for field, prefix in [
        ("cl_ord_id", "cl_ord_id"),
        ("order_id", "order_id"),
        ("secondary_order_id", "secondary_order_id"),
    ]:
        value = str(msg.get(field) or "").strip()
        if value:
            tokens.append(f"{prefix}:{value}")

    return tokens


def _build_message_groups(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Build related groups using linked identifiers.

    Example:
    - Message 1 only has ClOrdID CL1
    - Message 2 has ClOrdID CL1 and OrderID ORD1

    These should be one group because they share ClOrdID CL1.
    """
    groups: List[Dict[str, Any]] = []

    for msg in messages:
        tokens = _message_identifier_tokens(msg)

        matching_groups = []

        for group in groups:
            group_tokens = set(group.get("identifier_tokens") or [])
            if tokens and group_tokens.intersection(tokens):
                matching_groups.append(group)

        if not matching_groups:
            if tokens:
                group_key = tokens[0]
            else:
                group_key = f"ungrouped:message:{msg.get('message_index')}"

            group = {
                "group_key": group_key,
                "group_label": "",
                "identifier_tokens": list(tokens),
                "message_count": 0,
                "message_indexes": [],
                "cl_ord_ids": [],
                "order_ids": [],
                "secondary_order_ids": [],
                "exec_ids": [],
                "messages": [],
            }

            groups.append(group)

        else:
            group = matching_groups[0]

            # Merge any additional matching groups into the first group.
            for extra_group in matching_groups[1:]:
                for key in [
                    "identifier_tokens",
                    "message_indexes",
                    "cl_ord_ids",
                    "order_ids",
                    "secondary_order_ids",
                    "exec_ids",
                    "messages",
                ]:
                    for value in extra_group.get(key) or []:
                        if value not in group[key]:
                            group[key].append(value)

                group["message_count"] += extra_group.get("message_count") or 0
                groups.remove(extra_group)

            for token in tokens:
                if token not in group["identifier_tokens"]:
                    group["identifier_tokens"].append(token)

        group["message_count"] += 1
        group["message_indexes"].append(msg.get("message_index"))
        group["messages"].append(msg)

        for source_field, target_field in [
            ("cl_ord_id", "cl_ord_ids"),
            ("order_id", "order_ids"),
            ("secondary_order_id", "secondary_order_ids"),
            ("exec_id", "exec_ids"),
        ]:
            value = str(msg.get(source_field) or "").strip()
            if value and value not in group[target_field]:
                group[target_field].append(value)

    for group in groups:
        label_parts = []

        if group.get("cl_ord_ids"):
            label_parts.append("ClOrdID " + ", ".join(group["cl_ord_ids"]))

        if group.get("order_ids"):
            label_parts.append("OrderID " + ", ".join(group["order_ids"]))

        if group.get("secondary_order_ids"):
            label_parts.append("SecondaryOrderID " + ", ".join(group["secondary_order_ids"]))

        group["group_label"] = " / ".join(label_parts) or f"Ungrouped message {group['message_indexes'][0]}"

        for msg in group.get("messages") or []:
            msg["group_key"] = group["group_key"]
            msg["group_label"] = group["group_label"]

    groups.sort(
        key=lambda group: (
            1 if str(group.get("group_key") or "").startswith("ungrouped:") else 0,
            min(group.get("message_indexes") or [999999]),
        )
    )

    return groups

def analyze_fix_sequence(raw_text: str) -> Dict[str, Any]:
    """
    Analyze a pasted/uploaded block that may contain multiple FIX messages.

    First version:
    - split into message candidates
    - analyze each message using the existing single-message analyzer
    - return a simple timeline-ready structure
    """
    split_messages = split_fix_messages(raw_text)

    analyzed_messages: List[Dict[str, Any]] = []

    for split_msg in split_messages:
        analysis = analyze_fix_message(split_msg.get("raw_text") or "")
        business_object = analysis.get("business_object") or {}
        decoded_rows = analysis.get("decoded_rows") or []

        analyzed_messages.append({
            "message_index": split_msg.get("message_index"),
            "split_reason": split_msg.get("split_reason"),
            "raw_text": split_msg.get("raw_text") or "",
            "summary": analysis.get("summary") or "",
            "parsed_count": analysis.get("parsed_count", 0),
            "warnings": analysis.get("warnings") or [],
            "business_object": business_object,
            "decoded_rows": decoded_rows,

            "msg_type": _get_nested(business_object, "message", "type"),
            "sender": _get_nested(business_object, "message", "sender"),
            "target": _get_nested(business_object, "message", "target"),
            "msg_seq_num": _get_nested(business_object, "message", "message_sequence_number"),
            "sending_time": _get_nested(business_object, "message", "sending_time"),

            "cl_ord_id": (
                _get_nested(business_object, "order", "client_order_id")
                or _get_tag_value(decoded_rows, "11")
            ),
            "order_id": (
                _get_nested(business_object, "order", "order_id")
                or _get_tag_value(decoded_rows, "37")
            ),
            "secondary_order_id": (
                _get_nested(business_object, "order", "secondary_order_id")
                or _get_tag_value(decoded_rows, "198")
            ),
            "exec_id": (
                _get_nested(business_object, "order", "execution_id")
                or _get_tag_value(decoded_rows, "17")
            ),
            "exec_type": _get_nested(business_object, "order", "execution_type"),
            "ord_status": _get_nested(business_object, "order", "order_status"),

            "symbol": _get_nested(business_object, "trade", "symbol"),
            "security_id": _get_nested(business_object, "trade", "security_id"),
            "side": _get_nested(business_object, "trade", "side"),
            "exec_broker": _get_tag_value(decoded_rows, "76"),
            "ex_destination": _get_tag_value(decoded_rows, "100"),
            "security_exchange": _get_tag_value(decoded_rows, "207"),
            "security_type": _get_tag_value(decoded_rows, "167"),
            "security_desc": _get_tag_value(decoded_rows, "107"),
            "issuer": _get_tag_value(decoded_rows, "106"),
            "currency": _get_tag_value(decoded_rows, "15"),
            "order_qty": (
                _get_nested(business_object, "trade", "order_quantity")
                or _get_tag_value(decoded_rows, "38")
            ),
            "last_qty": (
                _get_nested(business_object, "trade", "last_quantity")
                or _get_tag_value(decoded_rows, "32")
            ),
            "last_px": (
                _get_nested(business_object, "trade", "last_price")
                or _get_tag_value(decoded_rows, "31")
            ),
            "cum_qty": (
                _get_nested(business_object, "trade", "cumulative_quantity")
                or _get_tag_value(decoded_rows, "14")
            ),
            "leaves_qty": (
                _get_nested(business_object, "trade", "leaves_quantity")
                or _get_tag_value(decoded_rows, "151")
            ),
            "transact_time": _get_nested(business_object, "trade", "transaction_time"),
        })

    sequence_messages = sorted(analyzed_messages, key=_sequence_sort_key)

    groups = _build_message_groups(sequence_messages)

    timeline_lines = []

    for msg in sequence_messages:
        parts = []

        msg_type = msg.get("msg_type") or "Unknown message"
        sender = msg.get("sender") or "?"
        target = msg.get("target") or "?"
        time = msg.get("transact_time") or msg.get("sending_time") or ""

        parts.append(f"Message {msg.get('message_index')}: {msg_type}")
        parts.append(f"{sender} → {target}")

        if time:
            parts.append(f"at {time}")

        if msg.get("symbol"):
            parts.append(f"Symbol {msg.get('symbol')}")

        if msg.get("side"):
            parts.append(f"Side {msg.get('side')}")

        if msg.get("order_qty"):
            parts.append(f"OrderQty {msg.get('order_qty')}")

        if msg.get("exec_type"):
            parts.append(f"ExecType {msg.get('exec_type')}")

        if msg.get("ord_status"):
            parts.append(f"OrdStatus {msg.get('ord_status')}")

        if msg.get("last_qty"):
            parts.append(f"LastQty {msg.get('last_qty')}")

        if msg.get("cum_qty"):
            parts.append(f"CumQty {msg.get('cum_qty')}")

        if msg.get("leaves_qty"):
            parts.append(f"LeavesQty {msg.get('leaves_qty')}")

        timeline_lines.append(". ".join(parts) + ".")

    summary = (
        f"Analyzed {len(analyzed_messages)} FIX message"
        f"{'' if len(analyzed_messages) == 1 else 's'} "
        f"across {len(groups)} related group"
        f"{'' if len(groups) == 1 else 's'}."
    )

    sequence_warnings = _build_sequence_warnings(sequence_messages)

    warning_count = (
        sum(len(msg.get("warnings") or []) for msg in analyzed_messages)
        + len(sequence_warnings)
    )

    if warning_count:
        summary += f" Found {warning_count} warning(s) across the sequence."

    return {
        "input_type": "fix_sequence",
        "summary": summary,
        "timeline_summary": "\n".join(timeline_lines),
        "message_count": len(analyzed_messages),
        "group_count": len(groups),
        "groups": groups,
        "messages": analyzed_messages,
        "warnings": [
            warning
            for msg in analyzed_messages
            for warning in (msg.get("warnings") or [])
        ] + sequence_warnings,
    }