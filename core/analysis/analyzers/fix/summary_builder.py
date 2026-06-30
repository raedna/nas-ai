from typing import Any, Dict


def build_fix_summary(business_object: Dict[str, Any]) -> str:
    message = business_object.get("message", {})
    trade = business_object.get("trade", {})
    order = business_object.get("order", {})
    parties = business_object.get("parties", {})

    message_type = message.get("type") or "FIX message"
    side = trade.get("side")
    symbol = trade.get("symbol") or trade.get("security_id")
    qty = trade.get("last_quantity") or trade.get("order_quantity")
    price = trade.get("last_price") or trade.get("order_price")
    currency = trade.get("currency")
    exec_type = order.get("execution_type")
    order_status = order.get("order_status")

    parts = []

    parts.append(f"This is a FIX {message_type}.")

    trade_bits = []

    if exec_type:
        trade_bits.append(f"execution type is {exec_type}")

    if order_status:
        trade_bits.append(f"order status is {order_status}")

    if side or symbol or qty or price:
        sentence = "It reports"

        if side:
            sentence += f" a {side.lower()}"

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

    if parties.get("party_ids"):
        parts.append(f"Party IDs present: {', '.join(parties['party_ids'])}.")

    if parties.get("party_roles"):
        parts.append(f"Party roles present: {', '.join(parties['party_roles'])}.")

    return " ".join(parts)