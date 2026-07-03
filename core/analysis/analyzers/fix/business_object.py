from typing import Any, Dict, List, Optional


def _find_by_tag_name(decoded_rows: List[Dict[str, Any]], tag_name: str) -> Optional[Dict[str, Any]]:
    for row in decoded_rows:
        if row.get("tag_name") == tag_name:
            return row
    return None


def _value(decoded_rows: List[Dict[str, Any]], tag_name: str) -> str:
    row = _find_by_tag_name(decoded_rows, tag_name)
    if not row:
        return ""
    return str(row.get("value") or "")


def _value_name(decoded_rows: List[Dict[str, Any]], tag_name: str) -> str:
    row = _find_by_tag_name(decoded_rows, tag_name)
    if not row:
        return ""
    return str(row.get("value_name") or row.get("value") or "")

def _build_parties(decoded_rows):
    """
    Build PartyID repeating groups.

    FIX party group is normally:
      453 NoPartyIDs
      448 PartyID
      447 PartyIDSource
      452 PartyRole

    A new PartyID starts a new party entry.
    """
    parties = []
    current = None

    for row in decoded_rows:
        tag_name = row.get("tag_name")
        value = row.get("value")
        value_name = row.get("value_name")

        if not tag_name:
            continue

        if tag_name == "PartyID":
            if current:
                parties.append(current)

            current = {
                "party_id": value,
                "party_id_source": "",
                "party_role": "",
                "party_role_name": "",
            }

        elif tag_name == "PartyIDSource":
            if current is None:
                current = {
                    "party_id": "",
                    "party_id_source": "",
                    "party_role": "",
                    "party_role_name": "",
                }

            current["party_id_source"] = value

        elif tag_name == "PartyRole":
            if current is None:
                current = {
                    "party_id": "",
                    "party_id_source": "",
                    "party_role": "",
                    "party_role_name": "",
                }

            current["party_role"] = value
            current["party_role_name"] = value_name or ""

    if current:
        parties.append(current)

    return parties


def build_fix_business_object(decoded_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a deterministic business object from decoded FIX rows.

    No hardcoded tag numbers.
    No hardcoded enum meanings.
    Uses decoded tag_name and value_name from the FIX dictionary.
    """

    message_type = _value_name(decoded_rows, "MsgType")
    side = _value_name(decoded_rows, "Side")
    exec_type = _value_name(decoded_rows, "ExecType")
    order_status = _value_name(decoded_rows, "OrdStatus")
    parties = _build_parties(decoded_rows)

    return {
        "message": {
            "type": _value_name(decoded_rows, "MsgType") or _value(decoded_rows, "MsgType"),
            "begin_string": _value(decoded_rows, "BeginString"),
            "body_length": _value(decoded_rows, "BodyLength"),
            "message_sequence_number": _value(decoded_rows, "MsgSeqNum"),

            # Message routing
            "sender": _value(decoded_rows, "SenderCompID"),
            "target": _value(decoded_rows, "TargetCompID"),
            "on_behalf_of": _value(decoded_rows, "OnBehalfOfCompID"),
            "deliver_to": _value(decoded_rows, "DeliverToCompID"),
            "sender_location": _value(decoded_rows, "SenderLocationID"),
            "on_behalf_of_location": _value(decoded_rows, "OnBehalfOfLocationID"),

            # Message timing / encoding
            "sending_time": _value(decoded_rows, "SendingTime"),
            "receive_date": _value(decoded_rows, "ReceiveDate"),
            "receive_time": _value(decoded_rows, "ReceiveTime"),
            "message_encoding": _value(decoded_rows, "MessageEncoding"),
        },

        "trade": {
            # Execution direction / instrument
            "side": _value_name(decoded_rows, "Side") or _value(decoded_rows, "Side"),

            # Security / instrument identifiers
            "symbol": _value(decoded_rows, "Symbol"),
            "security_id": _value(decoded_rows, "SecurityID"),
            "security_id_source": _value_name(decoded_rows, "SecurityIDSource") or _value(decoded_rows, "SecurityIDSource"),
            "security_description": _value(decoded_rows, "SecurityDesc"),
            "issuer": _value(decoded_rows, "Issuer"),
            "security_type": _value(decoded_rows, "SecurityType"),
            "security_sub_type": _value(decoded_rows, "SecuritySubType"),
            "security_exchange": _value(decoded_rows, "SecurityExchange"),
            "maturity_date": _value(decoded_rows, "MaturityDate"),
            "coupon_rate": _value(decoded_rows, "CouponRate"),
            "contract_multiplier": _value(decoded_rows, "ContractMultiplier"),

            # Quantities / prices
            "last_quantity": _value(decoded_rows, "LastQty"),
            "last_price": _value(decoded_rows, "LastPx"),
            "average_price": _value(decoded_rows, "AvgPx"),
            "cumulative_quantity": _value(decoded_rows, "CumQty"),
            "leaves_quantity": _value(decoded_rows, "LeavesQty"),
            "order_quantity": _value(decoded_rows, "OrderQty"),
            "order_price": _value(decoded_rows, "Price"),
            "currency": _value(decoded_rows, "Currency"),

            # Dates / market
            "trade_date": _value(decoded_rows, "TradeDate"),
            "settlement_date": _value(decoded_rows, "SettlDate"),
            "settlement_type": _value_name(decoded_rows, "SettlType") or _value(decoded_rows, "SettlType"),
            "transaction_time": _value(decoded_rows, "TransactTime"),
            "last_market": _value(decoded_rows, "LastMkt"),
        },

        "order": {
            # Order / execution identifiers
            "client_order_id": _value(decoded_rows, "ClOrdID"),
            "secondary_client_order_id": _value(decoded_rows, "SecondaryClOrdID"),
            "order_id": _value(decoded_rows, "OrderID"),
            "secondary_order_id": _value(decoded_rows, "SecondaryOrderID"),
            "execution_id": _value(decoded_rows, "ExecID"),
            "execution_ref_id": _value(decoded_rows, "ExecRefID"),
            "execution_broker": _value(decoded_rows, "ExecBroker"),

            # Status
            "execution_type": _value_name(decoded_rows, "ExecType") or _value(decoded_rows, "ExecType"),
            "order_status": _value_name(decoded_rows, "OrdStatus") or _value(decoded_rows, "OrdStatus"),

            # Client / account
            "account": _value(decoded_rows, "Account"),
        },

        "parties": parties,
    }