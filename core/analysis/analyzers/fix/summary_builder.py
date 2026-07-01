from typing import Any, Dict

def _display_message_type(message_type: str) -> str:
    if not message_type:
        return "FIX message"

    # ExecutionReport -> Execution Report
    spaced = ""
    for i, ch in enumerate(message_type):
        if i > 0 and ch.isupper() and not message_type[i - 1].isupper():
            spaced += " "
        spaced += ch

    return spaced


def build_fix_summary(business_object: Dict[str, Any]) -> str:
    message = business_object.get("message", {})
    trade = business_object.get("trade", {})
    order = business_object.get("order", {})
    parties = business_object.get("parties") or []

    message_type = message.get("type") or "FIX message"
    side = trade.get("side")
    symbol = trade.get("symbol") or trade.get("security_id")
    qty = trade.get("last_quantity") or trade.get("order_quantity")
    price = trade.get("last_price") or trade.get("order_price")
    currency = trade.get("currency")
    exec_type = order.get("execution_type")
    order_status = order.get("order_status")

    parts = []

    parts.append(f"This is a FIX {_display_message_type(message_type)}.")

    trade_bits = []

    if exec_type:
        trade_bits.append(f"execution type is {exec_type}")

    if order_status:
        trade_bits.append(f"order status is {order_status}")

    if side or symbol or qty or price:
        sentence = "It reports"

        if side:
            sentence += f" {side.lower()} execution"

        if symbol:
            sentence += f" for {symbol}"

        if qty:
            sentence += f" with quantity {qty}"

        if price:
            sentence += f" at price {price}"

        if currency:
            sentence += f" {currency}"

        sentence += "."
        parts.append(sentence)

    if trade_bits:
        parts.append("The " + " and ".join(trade_bits) + ".")

    if order.get("account"):
        parts.append(f"The account/client reference is {order['account']}.")

    if message.get("sender"):
        parts.append(f"The sender/broker is {message['sender']}.")

    if message.get("target"):
        parts.append(f"The target/counterparty is {message['target']}.")

    if isinstance(parties, list) and parties:
        party_phrases = []

        for party in parties:
            if not isinstance(party, dict):
                continue

            party_id = party.get("party_id")
            party_role_name = party.get("party_role_name")
            party_role = party.get("party_role")

            if party_id and party_role_name:
                party_phrases.append(f"{party_role_name}: {party_id}")
            elif party_id and party_role:
                party_phrases.append(f"role {party_role}: {party_id}")
            elif party_id:
                party_phrases.append(str(party_id))

        if party_phrases:
            parts.append("Parties include " + ", ".join(party_phrases) + ".")

    return " ".join(parts)