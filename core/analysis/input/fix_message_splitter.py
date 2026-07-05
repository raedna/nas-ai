import re
from typing import Any, Dict, List

from core.analysis.input.fix_input_normalizer import SOH, normalize_fix_input


def _looks_like_raw_fix(text: str) -> bool:
    text = str(text or "")
    return bool(re.search(r"\b8\s*=\s*FIX\.\d", text)) or SOH in text or "|" in text


def _split_raw_fix_messages(text: str) -> List[Dict[str, Any]]:
    """
    Split raw FIX text into individual messages.

    Handles:
    - real SOH separators
    - OCR SOH text normalized by normalize_fix_input()
    - pipe-delimited FIX
    - multiple messages starting with 8=FIX...
    """
    normalized = normalize_fix_input(text)

    # Convert to a stable visible separator for easier regex splitting.
    visible = normalized.replace(SOH, "|")

    # Keep the leading 8=FIX marker with each split message.
    starts = [m.start() for m in re.finditer(r"(?<![A-Za-z0-9])8\s*=\s*FIX\.\d(?:\.\d)?", visible)]

    if not starts:
        return []

    messages: List[Dict[str, Any]] = []

    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(visible)
        raw_message = visible[start:end].strip("|\n\r\t ")

        if raw_message:
            messages.append({
                "message_index": len(messages) + 1,
                "raw_text": raw_message,
                "split_reason": "raw_fix_begin_string",
            })

    return messages


def _split_ocr_table_messages(text: str) -> List[Dict[str, Any]]:
    """
    Split OCR/table-style FIX text into individual message blocks.

    This is intentionally conservative:
    a new message starts when we see a BeginString row after already collecting rows.

    Example table OCR:
        Tag
        Tag Name
        Value
        Value Name
        8
        BeginString
        FIX.4.4
        35
        MsgType
        D
        ...
        8
        BeginString
        FIX.4.4
        ...
    """
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]

    if not lines:
        return []

    messages: List[Dict[str, Any]] = []
    current: List[str] = []

    def flush(reason: str):
        nonlocal current

        block = "\n".join(current).strip()

        if block:
            messages.append({
                "message_index": len(messages) + 1,
                "raw_text": block,
                "split_reason": reason,
            })

        current = []

    for line in lines:
        lower = line.lower()

        # Table-style message boundary:
        # if BeginString appears after some existing message content, start new block.
        if "beginstring" in lower and current:
            flush("ocr_table_begin_string")

        current.append(line)

    flush("end_of_input")

    # Avoid returning one huge block if it does not look like table/FIX content.
    if len(messages) == 1:
        block_lower = messages[0]["raw_text"].lower()
        if "beginstring" not in block_lower and "msgtype" not in block_lower:
            return []

    return messages


def split_fix_messages(text: str) -> List[Dict[str, Any]]:
    """
    Split pasted/uploaded text into individual FIX message candidates.

    Returns:
        [
            {
                "message_index": 1,
                "raw_text": "...",
                "split_reason": "...",
            },
            ...
        ]

    If no clear multi-message boundary is found, returns one message block
    so the caller can still analyze it with analyze_fix_message().
    """
    text = str(text or "").strip()

    if not text:
        return []

    raw_messages = _split_raw_fix_messages(text)

    if raw_messages:
        return raw_messages

    table_messages = _split_ocr_table_messages(text)

    if table_messages:
        return table_messages

    return [{
        "message_index": 1,
        "raw_text": text,
        "split_reason": "single_message_fallback",
    }]