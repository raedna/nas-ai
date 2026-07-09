from typing import Any, Dict, List, Optional


IMPORTANT_FIX_FIELDS = {
    # Identity / routing
    "msg_type": {
        "label": "Message Type",
        "tag": "35",
        "category": "identity",
        "importance": "high",
    },
    "sender": {
        "label": "SenderCompID",
        "tag": "49",
        "category": "route",
        "importance": "high",
    },
    "target": {
        "label": "TargetCompID",
        "tag": "56",
        "category": "route",
        "importance": "high",
    },
    "msg_seq_num": {
        "label": "MsgSeqNum",
        "tag": "34",
        "category": "sequence",
        "importance": "high",
    },

    # Order identifiers
    "cl_ord_id": {
        "label": "ClOrdID",
        "tag": "11",
        "category": "order_identity",
        "importance": "critical",
    },
    "order_id": {
        "label": "OrderID",
        "tag": "37",
        "category": "order_identity",
        "importance": "critical",
    },
    "secondary_order_id": {
        "label": "SecondaryOrderID",
        "tag": "198",
        "category": "order_identity",
        "importance": "medium",
    },
    "exec_id": {
        "label": "ExecID",
        "tag": "17",
        "category": "execution_identity",
        "importance": "critical",
    },

    # Lifecycle
    "exec_type": {
        "label": "ExecType",
        "tag": "150",
        "category": "lifecycle",
        "importance": "critical",
    },
    "ord_status": {
        "label": "OrdStatus",
        "tag": "39",
        "category": "lifecycle",
        "importance": "critical",
    },

    # Quantities / price
    "order_qty": {
        "label": "OrderQty",
        "tag": "38",
        "category": "quantity",
        "importance": "critical",
    },
    "last_qty": {
        "label": "LastQty",
        "tag": "32",
        "category": "execution",
        "importance": "critical",
    },
    "last_px": {
        "label": "LastPx",
        "tag": "31",
        "category": "execution",
        "importance": "critical",
    },
    "cum_qty": {
        "label": "CumQty",
        "tag": "14",
        "category": "quantity",
        "importance": "critical",
    },
    "leaves_qty": {
        "label": "LeavesQty",
        "tag": "151",
        "category": "quantity",
        "importance": "critical",
    },
    "avg_px": {
        "label": "AvgPx",
        "tag": "6",
        "category": "execution",
        "importance": "high",
    },

    # Instrument
    "symbol": {
        "label": "Symbol",
        "tag": "55",
        "category": "instrument",
        "importance": "high",
    },
    "security_id": {
        "label": "SecurityID",
        "tag": "48",
        "category": "instrument",
        "importance": "high",
    },
    "security_exchange": {
        "label": "SecurityExchange",
        "tag": "207",
        "category": "instrument",
        "importance": "medium",
    },
    "security_type": {
        "label": "SecurityType",
        "tag": "167",
        "category": "instrument",
        "importance": "medium",
    },
    "currency": {
        "label": "Currency",
        "tag": "15",
        "category": "instrument",
        "importance": "medium",
    },

    # Broker / destination
    "exec_broker": {
        "label": "ExecBroker",
        "tag": "76",
        "category": "route",
        "importance": "high",
    },
    "ex_destination": {
        "label": "ExDestination",
        "tag": "100",
        "category": "route",
        "importance": "high",
    },

    # Side / time
    "side": {
        "label": "Side",
        "tag": "54",
        "category": "order_terms",
        "importance": "critical",
    },
    "sending_time": {
        "label": "SendingTime",
        "tag": "52",
        "category": "time",
        "importance": "high",
    },
    "transact_time": {
        "label": "TransactTime",
        "tag": "60",
        "category": "time",
        "importance": "high",
    },
}