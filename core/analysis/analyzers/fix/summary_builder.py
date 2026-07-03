from typing import Any, Dict, List


def _display_message_type(message_type: str) -> str:
    if not message_type:
        return "FIX message"

    # ExecutionReport -> Execution Report
    spaced = ""
    for i, ch in enumerate(str(message_type)):
        if i > 0 and ch.isupper() and not str(message_type)[i - 1].isupper():
            spaced += " "
        spaced += ch

    return spaced


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _is_present(value: Any) -> bool:
    value = _clean(value)
    return value not in {"", "0", "0.0", "None", "null"}


def _append_field(fields: List[str], label: str, value: Any) -> None:
    value = _clean(value)
    if value:
        fields.append(f"{label}: {value}")


def _line(title: str, fields: List[str]) -> str:
    if not fields:
        return ""
    return f"{title}: " + ". ".join(fields) + "."


def build_fix_summary(business_object: Dict[str, Any]) -> str:
    message = business_object.get("message", {}) or {}
    trade = business_object.get("trade", {}) or {}
    order = business_object.get("order", {}) or {}
    parties = business_object.get("parties") or []

    sections: List[str] = []

    # 1. Message routing
    message_type = _display_message_type(_clean(message.get("type")))
    sender = _clean(message.get("sender"))
    target = _clean(message.get("target"))

    if sender and target:
        sections.append(f"Message: {message_type} sent from {sender} to {target}.")
    elif sender:
        sections.append(f"Message: {message_type} sent from {sender}.")
    elif target:
        sections.append(f"Message: {message_type} received by {target}.")
    else:
        sections.append(f"Message: {message_type}.")

    # 2. Execution / order
    execution_fields: List[str] = []

    _append_field(execution_fields, "ExecType", order.get("execution_type"))
    _append_field(execution_fields, "OrdStatus", order.get("order_status"))
    _append_field(execution_fields, "Side", trade.get("side"))
    _append_field(execution_fields, "LastQty", trade.get("last_quantity"))
    _append_field(execution_fields, "CumQty", trade.get("cumulative_quantity"))
    _append_field(execution_fields, "LeavesQty", trade.get("leaves_quantity"))
    _append_field(execution_fields, "AvgPx", trade.get("average_price"))
    _append_field(execution_fields, "LastPx", trade.get("last_price"))
    _append_field(execution_fields, "OrderQty", trade.get("order_quantity"))
    _append_field(execution_fields, "OrderPx", trade.get("order_price"))

    currency = _clean(trade.get("currency"))
    if currency:
        execution_fields.append(f"CCY: {currency}")

    execution_line = _line("Execution / Order", execution_fields)
    if execution_line:
        sections.append(execution_line)

    # 3. Security / instrument
    security_fields: List[str] = []

    _append_field(security_fields, "Symbol", trade.get("symbol"))
    _append_field(security_fields, "SecurityIDSource", trade.get("security_id_source"))
    _append_field(security_fields, "SecurityID", trade.get("security_id"))
    _append_field(security_fields, "SecurityDesc", trade.get("security_description"))
    _append_field(security_fields, "Issuer", trade.get("issuer"))
    _append_field(security_fields, "SecurityType", trade.get("security_type"))
    _append_field(security_fields, "SecuritySubType", trade.get("security_sub_type"))
    _append_field(security_fields, "SecurityExchange", trade.get("security_exchange"))
    _append_field(security_fields, "MaturityDate", trade.get("maturity_date"))

    if _is_present(trade.get("coupon_rate")):
        _append_field(security_fields, "CouponRate", trade.get("coupon_rate"))

    _append_field(security_fields, "ContractMultiplier", trade.get("contract_multiplier"))
    _append_field(security_fields, "LastMkt", trade.get("last_market"))

    security_line = _line("Security", security_fields)
    if security_line:
        sections.append(security_line)

    # 4. Identifiers / timing
    identifier_fields: List[str] = []

    _append_field(identifier_fields, "ClOrdID", order.get("client_order_id"))
    _append_field(identifier_fields, "SecondaryClOrdID", order.get("secondary_client_order_id"))
    _append_field(identifier_fields, "OrderID", order.get("order_id"))
    _append_field(identifier_fields, "SecondaryOrderID", order.get("secondary_order_id"))
    _append_field(identifier_fields, "ExecID", order.get("execution_id"))
    _append_field(identifier_fields, "ExecRefID", order.get("execution_ref_id"))
    _append_field(identifier_fields, "Account", order.get("account"))
    _append_field(identifier_fields, "TransactTime", trade.get("transaction_time"))
    _append_field(identifier_fields, "TradeDate", trade.get("trade_date"))
    _append_field(identifier_fields, "SettlDate", trade.get("settlement_date"))
    _append_field(identifier_fields, "SettlType", trade.get("settlement_type"))
    _append_field(identifier_fields, "SendingTime", message.get("sending_time"))
    _append_field(identifier_fields, "MsgSeqNum", message.get("message_sequence_number"))
    _append_field(identifier_fields, "ExecBroker", order.get("execution_broker"))


    identifier_line = _line("Identifiers / Timing", identifier_fields)
    if identifier_line:
        sections.append(identifier_line)

    # 5. Additional routing
    routing_fields: List[str] = []

    _append_field(routing_fields, "OnBehalfOfCompID", message.get("on_behalf_of"))
    _append_field(routing_fields, "DeliverToCompID", message.get("deliver_to"))
    _append_field(routing_fields, "SenderLocationID", message.get("sender_location"))
    _append_field(routing_fields, "OnBehalfOfLocationID", message.get("on_behalf_of_location"))
    _append_field(routing_fields, "MessageEncoding", message.get("message_encoding"))

    routing_line = _line("Additional Routing", routing_fields)
    if routing_line:
        sections.append(routing_line)

    # 6. Parties
    if isinstance(parties, list) and parties:
        party_lines = []

        for party in parties:
            if not isinstance(party, dict):
                continue

            party_fields = []
            _append_field(party_fields, "PartyID", party.get("party_id"))
            _append_field(party_fields, "Source", party.get("party_id_source"))

            role = _clean(party.get("party_role"))
            role_name = _clean(party.get("party_role_name"))

            if role and role_name:
                party_fields.append(f"Role: {role} / {role_name}")
            elif role:
                party_fields.append(f"Role: {role}")
            elif role_name:
                party_fields.append(f"Role: {role_name}")

            if party_fields:
                party_lines.append("; ".join(party_fields))

        if party_lines:
            sections.append("Parties: " + " | ".join(party_lines) + ".")

    return "\n\n".join(sections)