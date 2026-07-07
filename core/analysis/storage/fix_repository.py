import hashlib
import json
from typing import Any, Dict

from core.db import get_conn

def _analysis_source_hash(messages: list) -> str:
    raw_parts = []

    for msg in messages or []:
        raw_parts.append(str(msg.get("raw_text") or "").strip())

    raw_text = "\n---FIX_MESSAGE---\n".join(raw_parts).strip()

    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()
    
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
) -> int:
    """
    Save a FIX analysis result into:
    - analysis_sessions
    - analysis_messages
    - analysis_message_tags

    Returns:
        session_id
    """
    messages = result.get("messages") or []

    # Single-message analysis result does not naturally have result["messages"].
    # Normalize it into one message-like object.
    if not messages and result.get("input_type") != "fix_sequence":
        messages = [{
            "message_index": 1,
            "raw_text": result.get("raw_text") or "",
            "summary": result.get("summary") or "",
            "warnings": result.get("warnings") or [],
            "business_object": result.get("business_object") or {},
            "decoded_rows": result.get("decoded_rows") or [],
        }]

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
                return existing[0]

            cur.execute(
                """
                INSERT INTO analysis_sessions (
                    analyzer_type,
                    analysis_mode,
                    source_type,
                    source_name,
                    source_hash,
                    summary,
                    warning_count,
                    message_count,
                    group_count
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    "FIX Message",
                    analysis_mode,
                    source_type,
                    source_name,
                    source_hash,
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

    return session_id
