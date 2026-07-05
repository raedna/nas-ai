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

        #if order_qty is not None and cum_qty is not None and leaves_qty is not None:
        #    expected = cum_qty + leaves_qty
#
        #    if abs(expected - order_qty) > 0.0001:
        #        warnings.append(
        #            f"Message {message_index}: CumQty + LeavesQty ({cum_qty:g} + {leaves_qty:g} = {expected:g}) "
        #            f"does not equal OrderQty ({order_qty:g}). Confirm source/OCR."
        #        )

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

        analyzed_messages.append({
            "message_index": split_msg.get("message_index"),
            "split_reason": split_msg.get("split_reason"),
            "raw_text": split_msg.get("raw_text") or "",
            "summary": analysis.get("summary") or "",
            "parsed_count": analysis.get("parsed_count", 0),
            "warnings": analysis.get("warnings") or [],
            "business_object": business_object,
            "decoded_rows": analysis.get("decoded_rows") or [],

            "msg_type": _get_nested(business_object, "message", "type"),
            "sender": _get_nested(business_object, "message", "sender"),
            "target": _get_nested(business_object, "message", "target"),
            "msg_seq_num": _get_nested(business_object, "message", "message_sequence_number"),
            "sending_time": _get_nested(business_object, "message", "sending_time"),

            "cl_ord_id": _get_nested(business_object, "order", "client_order_id"),
            "order_id": _get_nested(business_object, "order", "order_id"),
            "exec_id": _get_nested(business_object, "order", "execution_id"),
            "exec_type": _get_nested(business_object, "order", "execution_type"),
            "ord_status": _get_nested(business_object, "order", "order_status"),

            "symbol": _get_nested(business_object, "trade", "symbol"),
            "security_id": _get_nested(business_object, "trade", "security_id"),
            "side": _get_nested(business_object, "trade", "side"),
            "order_qty": _get_nested(business_object, "trade", "order_quantity"),
            "last_qty": _get_nested(business_object, "trade", "last_quantity"),
            "last_px": _get_nested(business_object, "trade", "last_price"),
            "cum_qty": _get_nested(business_object, "trade", "cumulative_quantity"),
            "leaves_qty": _get_nested(business_object, "trade", "leaves_quantity"),
            "transact_time": _get_nested(business_object, "trade", "transaction_time"),
        })

    sequence_messages = sorted(analyzed_messages, key=_sequence_sort_key)

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
        f"{'' if len(analyzed_messages) == 1 else 's'}."
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
        "messages": analyzed_messages,
        "warnings": [
            warning
            for msg in sequence_messages
            for warning in (msg.get("warnings") or [])
        ] + sequence_warnings,
    }