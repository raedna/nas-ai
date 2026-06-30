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

    return {
        "message": {
            "type": message_type,
            "begin_string": _value(decoded_rows, "BeginString"),
            "sender": _value(decoded_rows, "SenderCompID"),
            "target": _value(decoded_rows, "TargetCompID"),
            "sending_time": _value(decoded_rows, "SendingTime"),
        },
        "trade": {
            "side": side,
            "symbol": _value(decoded_rows, "Symbol"),
            "security_id": _value(decoded_rows, "SecurityID"),
            "security_id_source": _value_name(decoded_rows, "SecurityIDSource"),
            "last_quantity": _value(decoded_rows, "LastQty"),
            "last_price": _value(decoded_rows, "LastPx"),
            "order_quantity": _value(decoded_rows, "OrderQty"),
            "order_price": _value(decoded_rows, "Price"),
            "currency": _value(decoded_rows, "Currency"),
            "transaction_time": _value(decoded_rows, "TransactTime"),
        },
        "order": {
            "client_order_id": _value(decoded_rows, "ClOrdID"),
            "order_id": _value(decoded_rows, "OrderID"),
            "execution_id": _value(decoded_rows, "ExecID"),
            "execution_type": exec_type,
            "order_status": order_status,
            "account": _value(decoded_rows, "Account"),
        },
        "parties": {
            "party_ids": [
                row.get("value")
                for row in decoded_rows
                if row.get("tag_name") == "PartyID"
            ],
            "party_roles": [
                row.get("value_name") or row.get("value")
                for row in decoded_rows
                if row.get("tag_name") == "PartyRole"
            ],
        },
    }