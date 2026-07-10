import hashlib
import json
from typing import Any, Dict, Tuple
from core.db import get_conn, fetchall, fetchone, execute

def _analysis_source_hash(messages: list) -> str:
    raw_parts = []

    for msg in messages or []:
        raw_parts.append(str(msg.get("raw_text") or "").strip())

    raw_text = "\n---FIX_MESSAGE---\n".join(raw_parts).strip()

    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

def _get_tag_value(decoded_rows: list, tag: str) -> str:
    tag = str(tag)

    for row in decoded_rows or []:
        if str(row.get("tag") or "") == tag:
            return str(row.get("value") or "").strip()

    return ""


def _get_nested(data: dict, *keys: str) -> str:
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)

    return str(current or "").strip()


def _enrich_single_message(msg: Dict[str, Any]) -> Dict[str, Any]:
    business_object = msg.get("business_object") or {}
    decoded_rows = msg.get("decoded_rows") or []

    return {
        **msg,

        "msg_type": (
            _get_nested(business_object, "message", "type")
            or _get_tag_value(decoded_rows, "35")
        ),
        "msg_seq_num": (
            _get_nested(business_object, "message", "message_sequence_number")
            or _get_tag_value(decoded_rows, "34")
        ),
        "sender": (
            _get_nested(business_object, "message", "sender")
            or _get_tag_value(decoded_rows, "49")
        ),
        "target": (
            _get_nested(business_object, "message", "target")
            or _get_tag_value(decoded_rows, "56")
        ),
        "sending_time": (
            _get_nested(business_object, "message", "sending_time")
            or _get_tag_value(decoded_rows, "52")
        ),
        "transact_time": (
            _get_nested(business_object, "trade", "transaction_time")
            or _get_tag_value(decoded_rows, "60")
        ),

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
        "exec_type": (
            _get_nested(business_object, "order", "execution_type")
            or _get_tag_value(decoded_rows, "150")
        ),
        "ord_status": (
            _get_nested(business_object, "order", "order_status")
            or _get_tag_value(decoded_rows, "39")
        ),

        "symbol": (
            _get_nested(business_object, "trade", "symbol")
            or _get_tag_value(decoded_rows, "55")
        ),
        "security_id": (
            _get_nested(business_object, "trade", "security_id")
            or _get_tag_value(decoded_rows, "48")
        ),
        "security_id_source": (
            _get_nested(business_object, "trade", "security_id_source")
            or _get_tag_value(decoded_rows, "22")
        ),
        "side": (
            _get_nested(business_object, "trade", "side")
            or _get_tag_value(decoded_rows, "54")
        ),
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
        "avg_px": (
            _get_nested(business_object, "trade", "average_price")
            or _get_tag_value(decoded_rows, "6")
        ),
        "cum_qty": (
            _get_nested(business_object, "trade", "cumulative_quantity")
            or _get_tag_value(decoded_rows, "14")
        ),
        "leaves_qty": (
            _get_nested(business_object, "trade", "leaves_quantity")
            or _get_tag_value(decoded_rows, "151")
        ),
        "exec_broker": (
            _get_nested(business_object, "trade", "exec_broker")
            or _get_tag_value(decoded_rows, "76")
        ),
        "ex_destination": (
            _get_nested(business_object, "trade", "ex_destination")
            or _get_tag_value(decoded_rows, "100")
        ),
        "security_exchange": (
            _get_nested(business_object, "trade", "security_exchange")
            or _get_tag_value(decoded_rows, "207")
        ),
        "security_type": (
            _get_nested(business_object, "trade", "security_type")
            or _get_tag_value(decoded_rows, "167")
        ),
        "security_desc": (
            _get_nested(business_object, "trade", "security_description")
            or _get_tag_value(decoded_rows, "107")
        ),
        "issuer": (
            _get_nested(business_object, "trade", "issuer")
            or _get_tag_value(decoded_rows, "106")
        ),
        "currency": (
            _get_nested(business_object, "trade", "currency")
            or _get_tag_value(decoded_rows, "15")
        ),
    }

def _json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


def _json_list(value: Any) -> str:
    return json.dumps(value or [], ensure_ascii=False)

def _analysis_source_hash(messages: list) -> str:
    raw_parts = []

    for msg in messages or []:
        raw_parts.append(str(msg.get("raw_text") or "").strip())

    raw_text = "\n---FIX_MESSAGE---\n".join(raw_parts).strip()

    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

def save_fix_analysis_result(
    result: Dict[str, Any],
    analysis_mode: str,
    source_type: str = "manual",
    source_name: str = "",
    save_note: str = "",
) -> Tuple[int, bool]:
    """
    Save a FIX analysis result into:
    - analysis_sessions
    - analysis_messages
    - analysis_message_tags

    Returns:
        (session_id, created)
        created = False when an identical analysis already exists and was skipped.
    """
    messages = result.get("messages") or []

    # Single-message analysis result does not naturally have result["messages"].
    # Normalize it into one message-like object.
    if not messages and result.get("input_type") != "fix_sequence":
        messages = [_enrich_single_message({
            "message_index": 1,
            "raw_text": result.get("raw_text") or "",
            "summary": result.get("summary") or "",
            "warnings": result.get("warnings") or [],
            "business_object": result.get("business_object") or {},
            "decoded_rows": result.get("decoded_rows") or [],
        })]

    source_hash = _analysis_source_hash(messages)
    warnings = result.get("warnings") or []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM analysis_sessions
                WHERE source_hash = %s
                LIMIT 1
                """,
                (source_hash,),
            )

            existing = cur.fetchone()

            if existing:
                existing_session_id = existing[0]

                if save_note:
                    cur.execute(
                        """
                        UPDATE analysis_sessions
                        SET save_note = %s
                        WHERE id = %s
                        """,
                        (save_note, existing_session_id),
                    )

                return existing_session_id, False

            cur.execute(
                """
                INSERT INTO analysis_sessions (
                    analyzer_type,
                    analysis_mode,
                    source_type,
                    source_name,
                    source_hash,
                    save_note,
                    summary,
                    warning_count,
                    message_count,
                    group_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    "FIX Message",
                    analysis_mode,
                    source_type,
                    source_name,
                    source_hash,
                    save_note,
                    result.get("summary") or "",
                    len(warnings),
                    len(messages),
                    result.get("group_count") or 0,
                ),
            )

            session_id = cur.fetchone()[0]

            for msg in messages:
                business_object = msg.get("business_object") or {}

                cur.execute(
                    """
                    INSERT INTO analysis_messages (
                        session_id,
                        message_index,
                        group_key,
                        group_label,
                        raw_text,
                        summary,
                        msg_type,
                        msg_seq_num,
                        sender,
                        target,
                        sending_time,
                        transact_time,
                        cl_ord_id,
                        order_id,
                        secondary_order_id,
                        exec_id,
                        exec_type,
                        ord_status,
                        symbol,
                        security_id,
                        security_id_source,
                        exec_broker,
                        ex_destination,
                        security_exchange,
                        security_type,
                        security_desc,
                        issuer,
                        currency,
                        side,
                        order_qty,
                        last_qty,
                        last_px,
                        avg_px,
                        cum_qty,
                        leaves_qty,
                        warnings,
                        business_object
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb
                    )
                    RETURNING id
                    """,
                    (
                        session_id,
                        msg.get("message_index"),
                        msg.get("group_key"),
                        msg.get("group_label"),
                        msg.get("raw_text") or "",
                        msg.get("summary") or "",
                        msg.get("msg_type"),
                        msg.get("msg_seq_num"),
                        msg.get("sender"),
                        msg.get("target"),
                        msg.get("sending_time"),
                        msg.get("transact_time"),
                        msg.get("cl_ord_id"),
                        msg.get("order_id"),
                        msg.get("secondary_order_id"),
                        msg.get("exec_id"),
                        msg.get("exec_type"),
                        msg.get("ord_status"),
                        msg.get("symbol"),
                        msg.get("security_id"),
                        msg.get("security_id_source"),
                        msg.get("exec_broker"),
                        msg.get("ex_destination"),
                        msg.get("security_exchange"),
                        msg.get("security_type"),
                        msg.get("security_desc"),
                        msg.get("issuer"),
                        msg.get("currency"),
                        msg.get("side"),
                        msg.get("order_qty"),
                        msg.get("last_qty"),
                        msg.get("last_px"),
                        msg.get("avg_px"),
                        msg.get("cum_qty"),
                        msg.get("leaves_qty"),
                        _json_list(msg.get("warnings") or []),
                        _json(business_object),
                    ),
                )

                message_id = cur.fetchone()[0]

                decoded_rows = msg.get("decoded_rows") or []

                for index, row in enumerate(decoded_rows):
                    cur.execute(
                        """
                        INSERT INTO analysis_message_tags (
                            message_id,
                            position_index,
                            tag,
                            tag_name,
                            value,
                            value_name,
                            value_description,
                            description,
                            tag_status,
                            tag_warning,
                            has_enums,
                            enum_valid,
                            enum_warning,
                            ocr_original_tag,
                            ocr_tag_repaired,
                            ocr_repair_warning,
                            ocr_inferred,
                            ocr_score,
                            source
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            message_id,
                            index,
                            row.get("tag"),
                            row.get("tag_name"),
                            row.get("value"),
                            row.get("value_name"),
                            row.get("value_description"),
                            row.get("description"),
                            row.get("tag_status"),
                            row.get("tag_warning"),
                            row.get("has_enums"),
                            str(row.get("enum_valid") or ""),
                            row.get("enum_warning"),
                            row.get("ocr_original_tag"),
                            bool(row.get("ocr_tag_repaired")),
                            row.get("ocr_repair_warning"),
                            bool(row.get("ocr_inferred")),
                            row.get("ocr_score"),
                            row.get("source"),
                        ),
                    )

        conn.commit()

    return session_id, True

def list_fix_analysis_sessions(limit: int = 20):
    return fetchall(
        """
        SELECT
            id,
            analysis_mode,
            source_type,
            source_name,
            save_note,
            summary,
            warning_count,
            message_count,
            group_count,
            created_at
        FROM analysis_sessions
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )


def get_fix_analysis_session(session_id: int):
    return fetchone(
        """
        SELECT
            id,
            save_note,
            analysis_mode,
            source_type,
            source_name,
            summary,
            warning_count,
            message_count,
            group_count,
            created_at
        FROM analysis_sessions
        WHERE id = %s
        """,
        (session_id,),
    )

def update_fix_analysis_session_note(session_id: int, save_note: str) -> None:
    execute(
        """
        UPDATE analysis_sessions
        SET save_note = %s
        WHERE id = %s
        """,
        (save_note, session_id),
    )

def delete_fix_analysis_session(session_id: int) -> None:
    execute(
        """
        DELETE FROM analysis_sessions
        WHERE id = %s
        """,
        (session_id,),
    )

def list_fix_analysis_messages(session_id: int):
    return fetchall(
        """
        SELECT
            id,
            session_id,
            message_index,
            group_key,
            group_label,
            raw_text,
            summary,
            msg_type,
            msg_seq_num,
            sender,
            target,
            sending_time,
            transact_time,
            cl_ord_id,
            order_id,
            secondary_order_id,
            exec_id,
            exec_type,
            ord_status,
            symbol,
            security_id,
            security_id_source,
            side,
            order_qty,
            last_qty,
            last_px,
            avg_px,
            cum_qty,
            leaves_qty,
            warnings,
            business_object,
            created_at
        FROM analysis_messages
        WHERE session_id = %s
        ORDER BY message_index
        """,
        (session_id,),
    )


def list_fix_message_tags(message_id: int):
    return fetchall(
        """
        SELECT
            id,
            message_id,
            position_index,
            tag,
            tag_name,
            value,
            value_name,
            value_description,
            description,
            tag_status,
            tag_warning,
            has_enums,
            enum_valid,
            enum_warning,
            ocr_original_tag,
            ocr_tag_repaired,
            ocr_repair_warning,
            ocr_inferred,
            ocr_score,
            source,
            created_at
        FROM analysis_message_tags
        WHERE message_id = %s
        ORDER BY position_index
        """,
        (message_id,),
    )

def _clean_match_value(value: Any) -> str:
    return str(value or "").strip()


def find_related_saved_fix_messages(
    message: Dict[str, Any],
    exclude_session_id: int | None = None,
    limit: int = 20,
):
    """
    Find previously saved FIX messages related to the supplied analyzed message.

    This is related-message detection, not duplicate detection.
    Ranking is based on route + security + broker/venue fields.
    """
    sender = _clean_match_value(message.get("sender"))
    target = _clean_match_value(message.get("target"))
    symbol = _clean_match_value(message.get("symbol"))
    security_id = _clean_match_value(message.get("security_id"))
    security_exchange = _clean_match_value(message.get("security_exchange"))
    security_type = _clean_match_value(message.get("security_type"))
    exec_broker = _clean_match_value(message.get("exec_broker"))
    ex_destination = _clean_match_value(message.get("ex_destination"))

    cl_ord_id = _clean_match_value(message.get("cl_ord_id"))
    order_id = _clean_match_value(message.get("order_id"))
    exec_id = _clean_match_value(message.get("exec_id"))

    rules = []

    # Exact order/execution chain matches
    if cl_ord_id:
        rules.append(("exact", "Same ClOrdID", "m.cl_ord_id = %s", [cl_ord_id]))

    if order_id:
        rules.append(("exact", "Same OrderID", "m.order_id = %s", [order_id]))

    if exec_id:
        rules.append(("exact", "Same ExecID", "m.exec_id = %s", [exec_id]))

    # Strong related matches
    if sender and target and security_id:
        rules.append((
            "strong",
            "Same Sender/Target/SecurityID",
            "m.sender = %s AND m.target = %s AND m.security_id = %s",
            [sender, target, security_id],
        ))

    if sender and target and symbol and security_exchange:
        rules.append((
            "strong",
            "Same Sender/Target/Symbol/SecurityExchange",
            "m.sender = %s AND m.target = %s AND m.symbol = %s AND m.security_exchange = %s",
            [sender, target, symbol, security_exchange],
        ))

    if sender and target and exec_broker and symbol:
        rules.append((
            "strong",
            "Same Sender/Target/ExecBroker/Symbol",
            "m.sender = %s AND m.target = %s AND m.exec_broker = %s AND m.symbol = %s",
            [sender, target, exec_broker, symbol],
        ))

    if sender and target and ex_destination and symbol:
        rules.append((
            "strong",
            "Same Sender/Target/ExDestination/Symbol",
            "m.sender = %s AND m.target = %s AND m.ex_destination = %s AND m.symbol = %s",
            [sender, target, ex_destination, symbol],
        ))

    # Medium related matches
    if sender and target and symbol:
        rules.append((
            "medium",
            "Same Sender/Target/Symbol",
            "m.sender = %s AND m.target = %s AND m.symbol = %s",
            [sender, target, symbol],
        ))

    if security_id:
        rules.append((
            "medium",
            "Same SecurityID",
            "m.security_id = %s",
            [security_id],
        ))

    if symbol and security_exchange:
        rules.append((
            "medium",
            "Same Symbol/SecurityExchange",
            "m.symbol = %s AND m.security_exchange = %s",
            [symbol, security_exchange],
        ))

    if exec_broker and symbol:
        rules.append((
            "medium",
            "Same ExecBroker/Symbol",
            "m.exec_broker = %s AND m.symbol = %s",
            [exec_broker, symbol],
        ))

    if ex_destination and symbol:
        rules.append((
            "medium",
            "Same ExDestination/Symbol",
            "m.ex_destination = %s AND m.symbol = %s",
            [ex_destination, symbol],
        ))

    # Weak related matches
    if sender and target:
        rules.append((
            "weak",
            "Same Sender/Target",
            "m.sender = %s AND m.target = %s",
            [sender, target],
        ))

    if symbol:
        rules.append((
            "weak",
            "Same Symbol",
            "m.symbol = %s",
            [symbol],
        ))

    if security_type and security_exchange:
        rules.append((
            "weak",
            "Same SecurityType/SecurityExchange",
            "m.security_type = %s AND m.security_exchange = %s",
            [security_type, security_exchange],
        ))

    if not rules:
        return []

    strength_rank = {
        "exact": 0,
        "strong": 1,
        "medium": 2,
        "weak": 3,
    }

    found = {}

    for match_strength, match_reason, where_clause, params in rules:
        extra_filter = ""
        query_params = list(params)

        if exclude_session_id is not None:
            extra_filter = "AND s.id <> %s"
            query_params.append(exclude_session_id)

        rows = fetchall(
            f"""
            SELECT
                s.id AS session_id,
                s.analysis_mode,
                s.created_at,
                m.id AS message_id,
                m.message_index,
                m.msg_type,
                m.msg_seq_num,
                m.sender,
                m.target,
                m.symbol,
                m.security_id,
                m.security_id_source,
                m.security_exchange,
                m.security_type,
                m.security_desc,
                m.issuer,
                m.currency,
                m.exec_broker,
                m.ex_destination,
                m.cl_ord_id,
                m.order_id,
                m.exec_id
            FROM analysis_messages m
            JOIN analysis_sessions s ON s.id = m.session_id
            WHERE {where_clause}
              {extra_filter}
            ORDER BY s.created_at DESC, m.message_index
            LIMIT %s
            """,
            tuple(query_params + [limit]),
        )

        for row in rows:
            key = (row.get("session_id"), row.get("message_id"))

            candidate = {
                **row,
                "match_strength": match_strength,
                "match_reason": match_reason,
                "match_rank": strength_rank.get(match_strength, 99),
            }

            existing = found.get(key)

            if existing is None or candidate["match_rank"] < existing["match_rank"]:
                found[key] = candidate

    results = list(found.values())

    results.sort(
        key=lambda row: (
            row.get("match_rank", 99),
            str(row.get("created_at") or ""),
            row.get("session_id") or 0,
            row.get("message_index") or 0,
        ),
        reverse=False,
    )

    return results[:limit]

def build_related_match_messages_from_result(result: Dict[str, Any]):
    """
    Convert an analysis result into message dictionaries that can be checked
    against previously saved FIX messages.
    """
    if not result:
        return []

    if result.get("input_type") == "fix_sequence":
        return result.get("messages") or []

    if result.get("input_type") == "fix_compare":
        return []

    return [_enrich_single_message({
        "message_index": 1,
        "raw_text": result.get("raw_text") or "",
        "summary": result.get("summary") or "",
        "warnings": result.get("warnings") or [],
        "business_object": result.get("business_object") or {},
        "decoded_rows": result.get("decoded_rows") or [],
    })]

def get_fix_analysis_message(message_id: int):
    return fetchone(
        """
        SELECT
            id,
            session_id,
            message_index,
            raw_text,
            summary,
            msg_type,
            msg_seq_num,
            sender,
            target,
            cl_ord_id,
            order_id,
            exec_id,
            symbol,
            security_id,
            security_exchange,
            security_type,
            ex_destination
        FROM analysis_messages
        WHERE id = %s
        """,
        (message_id,),
    )
