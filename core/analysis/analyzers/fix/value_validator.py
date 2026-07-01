import re
from typing import Any, Dict, List


NUMERIC_TAG_NAMES = {
    "OrderQty",
    "LastQty",
    "LastPx",
    "Price",
    "AvgPx",
    "CumQty",
    "LeavesQty",
    "BodyLength",
    "MsgSeqNum",
}


DATETIME_TAG_NAMES = {
    "SendingTime",
    "TransactTime",
}


def validate_fix_decoded_rows(decoded_rows: List[Dict[str, Any]]) -> List[str]:
    warnings: List[str] = []

    for row in decoded_rows:
        tag = row.get("tag", "")
        tag_name = row.get("tag_name", "")
        value = str(row.get("value") or "").strip()

        if not value or not tag_name:
            continue

        if tag_name in NUMERIC_TAG_NAMES:
            if not re.fullmatch(r"-?\d+(\.\d+)?", value):
                warnings.append(
                    f"Possible OCR issue: tag {tag} ({tag_name}) has value '{value}', "
                    "but this field is expected to be numeric."
                )

        if tag_name in DATETIME_TAG_NAMES:
            if not re.fullmatch(r"\d{8}-\d{2}:\d{2}:\d{2}(\.\d+)?", value):
                warnings.append(
                    f"Possible OCR issue: tag {tag} ({tag_name}) has value '{value}', "
                    "but this field is expected to look like YYYYMMDD-HH:MM:SS."
                )

    return warnings