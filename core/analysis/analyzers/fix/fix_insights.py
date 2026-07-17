from typing import Any, Dict, List, Optional


STATUS_FIELDS = {
    "msg_type": {
        "tag": "35",
        "label": "MsgType",
        "category": "lifecycle",
    },
    "exec_trans_type": {
        "tag": "20",
        "label": "ExecTransType",
        "category": "lifecycle",
    },
    "ord_status": {
        "tag": "39",
        "label": "OrdStatus",
        "category": "lifecycle",
    },
    "exec_type": {
        "tag": "150",
        "label": "ExecType",
        "category": "lifecycle",
    },
}

QUANTITY_FIELDS = {
    "order_qty": {
        "tag": "38",
        "label": "OrderQty",
        "category": "quantity",
    },
    "last_qty": {
        "tag": "32",
        "label": "LastQty",
        "category": "quantity",
    },
    "cum_qty": {
        "tag": "14",
        "label": "CumQty",
        "category": "quantity",
    },
    "leaves_qty": {
        "tag": "151",
        "label": "LeavesQty",
        "category": "quantity",
    },
}

REJECT_FIELDS = {
    "ord_rej_reason": {
        "tag": "103",
        "label": "OrdRejReason",
        "category": "reject",
    },
    "cxl_rej_reason": {
        "tag": "102",
        "label": "CxlRejReason",
        "category": "reject",
    },
    "exec_restatement_reason": {
        "tag": "378",
        "label": "ExecRestatementReason",
        "category": "restatement",
    },
    "text": {
        "tag": "58",
        "label": "Text",
        "category": "reject",
    },
}

IDENTITY_FIELDS = {
    "cl_ord_id": {
        "tag": "11",
        "label": "ClOrdID",
        "category": "identity",
    },
    "orig_cl_ord_id": {
        "tag": "41",
        "label": "OrigClOrdID",
        "category": "identity",
    },
    "order_id": {
        "tag": "37",
        "label": "OrderID",
        "category": "identity",
    },
    "secondary_order_id": {
        "tag": "198",
        "label": "SecondaryOrderID",
        "category": "identity",
    },
    "exec_id": {
        "tag": "17",
        "label": "ExecID",
        "category": "identity",
    },
    "msg_seq_num": {
        "tag": "34",
        "label": "MsgSeqNum",
        "category": "sequence",
    },
}

WATCH_FIELDS = {
    **STATUS_FIELDS,
    **QUANTITY_FIELDS,
    **REJECT_FIELDS,
    **IDENTITY_FIELDS,
}


MSG_TYPE_LABELS = {
    "D": "NewOrderSingle",
    "F": "OrderCancelRequest",
    "G": "OrderCancelReplaceRequest",
    "8": "ExecutionReport",
    "9": "OrderCancelReject",
    "3": "Reject",
    "j": "BusinessMessageReject",
}


EXEC_TYPE_LABELS = {
    "0": "New",
    "1": "PartialFill",
    "2": "Fill",
    "4": "Canceled",
    "5": "Replaced",
    "8": "Rejected",
    "D": "Restated",
    "F": "Trade",
    "G": "TradeCorrect",
    "H": "TradeCancel",
    "I": "OrderStatus",
}


ORD_STATUS_LABELS = {
    "0": "New",
    "1": "PartiallyFilled",
    "2": "Filled",
    "4": "Canceled",
    "5": "Replaced",
    "6": "PendingCancel",
    "8": "Rejected",
    "A": "PendingNew",
    "E": "PendingReplace",
}


NORMAL_ORD_STATUS_TRANSITIONS = {
    ("A", "0"),  # PendingNew -> New
    ("0", "1"),  # New -> PartiallyFilled
    ("0", "2"),  # New -> Filled
    ("1", "1"),  # PartiallyFilled -> PartiallyFilled
    ("1", "2"),  # PartiallyFilled -> Filled
    ("0", "6"),  # New -> PendingCancel
    ("1", "6"),  # PartiallyFilled -> PendingCancel
    ("6", "4"),  # PendingCancel -> Canceled
    ("0", "E"),  # New -> PendingReplace
    ("1", "E"),  # PartiallyFilled -> PendingReplace
    ("E", "5"),  # PendingReplace -> Replaced
    ("0", "4"),  # New -> Canceled
    ("1", "4"),  # PartiallyFilled -> Canceled
}


NORMAL_EXEC_TYPE_TRANSITIONS = {
    ("0", "1"),  # New -> PartialFill
    ("0", "2"),  # New -> Fill
    ("1", "1"),  # PartialFill -> PartialFill
    ("1", "2"),  # PartialFill -> Fill
    ("0", "4"),  # New -> Canceled
    ("1", "4"),  # PartialFill -> Canceled
    ("0", "5"),  # New -> Replaced
    ("1", "5"),  # PartialFill -> Replaced
    ("1", "F"),  # PartialFill -> Trade
    ("F", "F"),  # Trade -> Trade
}


CANCEL_OR_REPLACE_MSG_TYPES = {"F", "G", "9"}
CANCEL_OR_REPLACE_EXEC_TYPES = {"4", "5", "G", "H"}
REJECT_MSG_TYPES = {"3", "j", "9"}
REJECT_EXEC_TYPES = {"8"}
RESTATEMENT_EXEC_TYPES = {"D", "G", "H"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> Optional[float]:
    raw = _clean(value).replace(",", "")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _format_value(value: Any) -> str:
    raw = _clean(value)
    return raw if raw else "<blank>"


def _decode_field_value(field: str, value: Any) -> str:
    raw = _clean(value)

    if field == "msg_type":
        return MSG_TYPE_LABELS.get(raw, raw)

    if field == "exec_type":
        return EXEC_TYPE_LABELS.get(raw, raw)

    if field == "ord_status":
        return ORD_STATUS_LABELS.get(raw, raw)

    return raw


def _value_changed(before: Any, after: Any) -> bool:
    return _clean(before) != _clean(after)


def _message_label(message: Dict[str, Any]) -> str:
    index = message.get("message_index")
    if index is not None:
        return f"Message {index}"
    return "Message"


def _change_entry(
    before: Dict[str, Any],
    after: Dict[str, Any],
    field: str,
    severity: str,
    summary: str,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    meta = WATCH_FIELDS.get(field, {})
    return {
        "from_message_index": before.get("message_index"),
        "to_message_index": after.get("message_index"),
        "field": field,
        "tag": meta.get("tag"),
        "label": meta.get("label", field),
        "category": category or meta.get("category", "general"),
        "severity": severity,
        "before": before.get(field),
        "after": after.get(field),
        "summary": summary,
    }


def _is_replace_or_restatement_context(message: Dict[str, Any]) -> bool:
    msg_type = _clean(message.get("msg_type"))
    exec_type = _clean(message.get("exec_type"))

    return (
        msg_type in {"G"}
        or exec_type in {"5", "D", "G", "H"}
        or bool(_clean(message.get("orig_cl_ord_id")))
    )


def _is_cancel_replace_or_update_context(message: Dict[str, Any]) -> bool:
    msg_type = _clean(message.get("msg_type"))
    exec_type = _clean(message.get("exec_type"))

    return (
        msg_type in CANCEL_OR_REPLACE_MSG_TYPES
        or exec_type in CANCEL_OR_REPLACE_EXEC_TYPES
        or _is_replace_or_restatement_context(message)
    )


def _is_reject_context(message: Dict[str, Any]) -> bool:
    msg_type = _clean(message.get("msg_type"))
    exec_type = _clean(message.get("exec_type"))

    return msg_type in REJECT_MSG_TYPES or exec_type in REJECT_EXEC_TYPES


def _is_fill_or_execution_context(message: Dict[str, Any]) -> bool:
    msg_type = _clean(message.get("msg_type"))
    exec_type = _clean(message.get("exec_type"))
    ord_status = _clean(message.get("ord_status"))

    return (
        msg_type == "8"
        or exec_type in {"1", "2", "F"}
        or ord_status in {"1", "2"}
    )


def _analyze_status_change(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    before_msg_type = _clean(before.get("msg_type"))
    after_msg_type = _clean(after.get("msg_type"))
    before_exec_type = _clean(before.get("exec_type"))
    after_exec_type = _clean(after.get("exec_type"))
    before_ord_status = _clean(before.get("ord_status"))
    after_ord_status = _clean(after.get("ord_status"))

    if _value_changed(before.get("msg_type"), after.get("msg_type")):
        severity = "warning"
        if after_msg_type in REJECT_MSG_TYPES or after_msg_type in CANCEL_OR_REPLACE_MSG_TYPES:
            severity = "critical"

        changes.append(_change_entry(
            before,
            after,
            "msg_type",
            severity,
            (
                f"MsgType changed from "
                f"{_decode_field_value('msg_type', before_msg_type)} "
                f"to {_decode_field_value('msg_type', after_msg_type)}."
            ),
        ))

    if _value_changed(before.get("ord_status"), after.get("ord_status")):
        transition = (before_ord_status, after_ord_status)
        severity = "info" if transition in NORMAL_ORD_STATUS_TRANSITIONS else "warning"

        if before_ord_status in {"2", "4", "8"} and after_ord_status in {"0", "1", "2"}:
            severity = "critical"

        changes.append(_change_entry(
            before,
            after,
            "ord_status",
            severity,
            (
                f"OrdStatus changed from "
                f"{_decode_field_value('ord_status', before_ord_status)} "
                f"to {_decode_field_value('ord_status', after_ord_status)}."
            ),
        ))

    if _value_changed(before.get("exec_type"), after.get("exec_type")):
        transition = (before_exec_type, after_exec_type)
        severity = "info" if transition in NORMAL_EXEC_TYPE_TRANSITIONS else "warning"

        if after_exec_type in REJECT_EXEC_TYPES or after_exec_type in CANCEL_OR_REPLACE_EXEC_TYPES:
            severity = "critical"

        changes.append(_change_entry(
            before,
            after,
            "exec_type",
            severity,
            (
                f"ExecType changed from "
                f"{_decode_field_value('exec_type', before_exec_type)} "
                f"to {_decode_field_value('exec_type', after_exec_type)}."
            ),
        ))

    if _value_changed(before.get("exec_trans_type"), after.get("exec_trans_type")):
        changes.append(_change_entry(
            before,
            after,
            "exec_trans_type",
            "warning",
            (
                f"ExecTransType changed from "
                f"{_format_value(before.get('exec_trans_type'))} "
                f"to {_format_value(after.get('exec_trans_type'))}."
            ),
        ))

    if _is_fill_or_execution_context(before) and _is_cancel_replace_or_update_context(after):
        changes.append({
            "from_message_index": before.get("message_index"),
            "to_message_index": after.get("message_index"),
            "field": "lifecycle_direction",
            "tag": None,
            "label": "Lifecycle Direction",
            "category": "lifecycle",
            "severity": "warning",
            "before": None,
            "after": None,
            "summary": (
                "Flow changed from execution/fill activity to cancel, replace, "
                "correction, or update activity. Confirm this was expected."
            ),
        })

    if _is_reject_context(after):
        changes.append({
            "from_message_index": before.get("message_index"),
            "to_message_index": after.get("message_index"),
            "field": "reject_signal",
            "tag": None,
            "label": "Reject Signal",
            "category": "reject",
            "severity": "critical",
            "before": None,
            "after": None,
            "summary": (
                "Reject or cancel-reject signal detected. Check reject reason, "
                "reference tag, message type, and Text field if available."
            ),
        })

    return changes


def _analyze_order_qty_change(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not _value_changed(before.get("order_qty"), after.get("order_qty")):
        return []

    if _is_replace_or_restatement_context(after):
        severity = "warning"
        explanation = (
            "OrderQty changed, and replace/restatement context was detected. "
            "Confirm the amend is expected and OrigClOrdID links correctly."
        )
    else:
        severity = "critical"
        explanation = (
            "OrderQty changed without clear replace/restatement context. "
            "This may indicate mixed related messages, incorrect linkage, or an unexpected amend."
        )

    return [
        _change_entry(
            before,
            after,
            "order_qty",
            severity,
            (
                f"OrderQty changed from {_format_value(before.get('order_qty'))} "
                f"to {_format_value(after.get('order_qty'))}. {explanation}"
            ),
        )
    ]


def _analyze_quantity_consistency(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    before_cum = _to_float(before.get("cum_qty"))
    after_cum = _to_float(after.get("cum_qty"))
    before_leaves = _to_float(before.get("leaves_qty"))
    after_leaves = _to_float(after.get("leaves_qty"))
    after_order_qty = _to_float(after.get("order_qty"))

    correction_context = _is_replace_or_restatement_context(after)

    if before_cum is not None and after_cum is not None:
        if after_cum < before_cum and not correction_context:
            changes.append(_change_entry(
                before,
                after,
                "cum_qty",
                "critical",
                (
                    f"CumQty decreased from {_format_value(before.get('cum_qty'))} "
                    f"to {_format_value(after.get('cum_qty'))} without clear correction/restatement context."
                ),
            ))
        elif after_cum > before_cum:
            changes.append(_change_entry(
                before,
                after,
                "cum_qty",
                "info",
                (
                    f"CumQty increased from {_format_value(before.get('cum_qty'))} "
                    f"to {_format_value(after.get('cum_qty'))}."
                ),
            ))

    if before_leaves is not None and after_leaves is not None:
        if after_leaves > before_leaves and not correction_context:
            changes.append(_change_entry(
                before,
                after,
                "leaves_qty",
                "warning",
                (
                    f"LeavesQty increased from {_format_value(before.get('leaves_qty'))} "
                    f"to {_format_value(after.get('leaves_qty'))} without clear replace/restatement context."
                ),
            ))
        elif after_leaves < before_leaves:
            changes.append(_change_entry(
                before,
                after,
                "leaves_qty",
                "info",
                (
                    f"LeavesQty decreased from {_format_value(before.get('leaves_qty'))} "
                    f"to {_format_value(after.get('leaves_qty'))}."
                ),
            ))

    if (
        after_order_qty is not None
        and after_cum is not None
        and after_leaves is not None
    ):
        if abs(after_order_qty - (after_cum + after_leaves)) > 0.000001:
            changes.append({
                "from_message_index": before.get("message_index"),
                "to_message_index": after.get("message_index"),
                "field": "quantity_balance",
                "tag": None,
                "label": "Quantity Balance",
                "category": "quantity",
                "severity": "critical",
                "before": None,
                "after": None,
                "summary": (
                    f"Quantity balance does not match on {_message_label(after)}: "
                    f"OrderQty {_format_value(after.get('order_qty'))} is not equal to "
                    f"CumQty {_format_value(after.get('cum_qty'))} + "
                    f"LeavesQty {_format_value(after.get('leaves_qty'))}."
                ),
            })

    if _value_changed(before.get("last_qty"), after.get("last_qty")):
        changes.append(_change_entry(
            before,
            after,
            "last_qty",
            "info",
            (
                f"LastQty changed from {_format_value(before.get('last_qty'))} "
                f"to {_format_value(after.get('last_qty'))}."
            ),
        ))

    return changes


def _analyze_reject_reason_fields(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    for field in REJECT_FIELDS:
        if _clean(after.get(field)) and _value_changed(before.get(field), after.get(field)):
            changes.append(_change_entry(
                before,
                after,
                field,
                "critical",
                (
                    f"{WATCH_FIELDS[field]['label']} appeared or changed from "
                    f"{_format_value(before.get(field))} to {_format_value(after.get(field))}."
                ),
            ))

    return changes


def analyze_message_pair(
    before: Dict[str, Any],
    after: Dict[str, Any],
) -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    changes.extend(_analyze_status_change(before, after))
    changes.extend(_analyze_order_qty_change(before, after))
    changes.extend(_analyze_quantity_consistency(before, after))
    changes.extend(_analyze_reject_reason_fields(before, after))

    return changes

def _message_relationship_tokens(message: Dict[str, Any]) -> set[str]:
    """Return conservative identifiers that can link messages into one lifecycle."""
    tokens: set[str] = set()

    cl_ord_id = _clean(message.get("cl_ord_id"))
    orig_cl_ord_id = _clean(message.get("orig_cl_ord_id"))
    order_id = _clean(message.get("order_id"))
    secondary_order_id = _clean(message.get("secondary_order_id"))

    # OrigClOrdID links back to another message's ClOrdID.
    if cl_ord_id:
        tokens.add(f"cl_ord_id:{cl_ord_id}")

    if orig_cl_ord_id:
        tokens.add(f"cl_ord_id:{orig_cl_ord_id}")

    if order_id:
        tokens.add(f"order_id:{order_id}")

    if secondary_order_id:
        tokens.add(f"secondary_order_id:{secondary_order_id}")

    return tokens


def _build_relationship_groups(
    messages: List[Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    """Create connected message groups from shared order identifiers."""
    groups: List[Dict[str, Any]] = []

    for message in messages:
        message_tokens = _message_relationship_tokens(message)

        matching_indexes = [
            index
            for index, group in enumerate(groups)
            if message_tokens
            and group["tokens"].intersection(message_tokens)
        ]

        if not matching_indexes:
            groups.append({
                "tokens": set(message_tokens),
                "messages": [message],
            })
            continue

        primary_index = matching_indexes[0]
        primary_group = groups[primary_index]
        primary_group["messages"].append(message)
        primary_group["tokens"].update(message_tokens)

        # A message can bridge two previously separate groups.
        for extra_index in reversed(matching_indexes[1:]):
            extra_group = groups.pop(extra_index)
            primary_group["messages"].extend(extra_group["messages"])
            primary_group["tokens"].update(extra_group["tokens"])

    message_order = {
        id(message): position
        for position, message in enumerate(messages)
    }

    normalized_groups = []

    for group in groups:
        ordered_messages = sorted(
            group["messages"],
            key=lambda message: message_order.get(id(message), 999999),
        )
        normalized_groups.append(ordered_messages)

    normalized_groups.sort(
        key=lambda group: min(
            message_order.get(id(message), 999999)
            for message in group
        )
    )

    return normalized_groups


def _relationship_findings(
    groups: List[List[Dict[str, Any]]],
    total_messages: int,
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    if total_messages <= 1:
        return findings

    for group in groups:
        indexes = [
            message.get("message_index")
            for message in group
            if message.get("message_index") is not None
        ]

        if len(group) == 1:
            message_index = indexes[0] if indexes else None

            other_indexes = [
                other.get("message_index")
                for other_group in groups
                if other_group is not group
                for other in other_group
                if other.get("message_index") is not None
            ]

            other_text = ", ".join(
                f"Message {index}" for index in other_indexes
            ) or "the other workspace messages"

            findings.append({
                "from_message_index": message_index,
                "to_message_index": None,
                "field": "relationship",
                "tag": None,
                "label": "Message Relationship",
                "category": "relationship",
                "severity": "warning",
                "before": None,
                "after": None,
                "summary": (
                    f"Message {message_index} is not related to {other_text} "
                    "based on the available order identifiers."
                ),
            })
            continue

        message_text = ", ".join(
            f"Message {index}" for index in indexes
        )

        findings.append({
            "from_message_index": indexes[0] if indexes else None,
            "to_message_index": indexes[-1] if indexes else None,
            "field": "relationship",
            "tag": None,
            "label": "Message Relationship",
            "category": "relationship",
            "severity": "info",
            "before": None,
            "after": None,
            "summary": (
                f"{message_text} are related and will be evaluated "
                "as one message lifecycle."
            ),
        })

    return findings

def build_sequence_insights(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    clean_messages = [
        message
        for message in messages
        if isinstance(message, dict)
    ]

    relationship_groups = _build_relationship_groups(clean_messages)
    relationship_changes = _relationship_findings(
        relationship_groups,
        len(clean_messages),
    )

    sequence_changes: List[Dict[str, Any]] = []

    # Compare only messages belonging to the same related group.
    for group in relationship_groups:
        if len(group) < 2:
            continue

        for index in range(1, len(group)):
            before = group[index - 1]
            after = group[index]
            sequence_changes.extend(
                analyze_message_pair(before, after)
            )

    # Relationship findings appear before lifecycle/value changes.
    changes = relationship_changes + sequence_changes

    warnings = [
        change
        for change in changes
        if change.get("severity") in {"warning", "critical"}
    ]

    critical_count = sum(
        1 for change in changes
        if change.get("severity") == "critical"
    )
    warning_count = sum(
        1 for change in changes
        if change.get("severity") == "warning"
    )
    info_count = sum(
        1 for change in changes
        if change.get("severity") == "info"
    )

    related_group_count = sum(
        1 for group in relationship_groups
        if len(group) > 1
    )
    unrelated_message_count = sum(
        1 for group in relationship_groups
        if len(group) == 1
    )

    if not clean_messages:
        summary = "No FIX messages available for sequence insights."
    else:
        summary = (
            f"Analyzed {len(clean_messages)} messages across "
            f"{len(relationship_groups)} relationship group(s). "
            f"Found {related_group_count} related group(s) and "
            f"{unrelated_message_count} unrelated message(s). "
            f"Generated {len(sequence_changes)} within-group sequence insight(s)."
        )

    return {
        "summary": summary,
        "message_count": len(clean_messages),
        "changes": changes,
        "warnings": warnings,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "info_count": info_count,
        "relationship_group_count": len(relationship_groups),
        "related_group_count": related_group_count,
        "unrelated_message_count": unrelated_message_count,
        "relationship_groups": [
            [
                message.get("message_index")
                for message in group
            ]
            for group in relationship_groups
        ],
    }