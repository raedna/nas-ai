import re
from typing import Dict, List


SOH = "\x01"


def normalize_fix_input(raw: str) -> str:
    text = raw or ""

    text = text.replace("\\x01", SOH)
    text = text.replace("^A", SOH)
    text = text.replace("<SOH>", SOH)
    text = text.replace("[SOH]", SOH)
    text = text.replace(" SOH ", SOH)
    text = text.replace("SOH", SOH)
    text = text.replace("|", SOH)

    return text.strip()


def parse_fix_input(raw: str) -> List[Dict[str, str]]:
    """
    Extract tag/value pairs only.
    No FIX knowledge. No hardcoded meanings.
    """

    text = normalize_fix_input(raw)
    pairs: List[Dict[str, str]] = []

    # Raw / pipe / SOH format: 35=8, 54=2, etc.
    for part in re.split(r"[\x01\r\n]+", text):
        part = part.strip()
        if not part or "=" not in part:
            continue

        tag, value = part.split("=", 1)
        tag = tag.strip()
        value = value.strip()

        if tag.isdigit():
            pairs.append({"tag": tag, "value": value})

    if pairs:
        return pairs

    # Formatted/table-like pasted text fallback.
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        cols = re.split(r"\t+|\s{2,}", line)
        if len(cols) >= 3 and cols[0].isdigit():
            pairs.append({
                "tag": cols[0].strip(),
                "value": cols[2].strip(),
            })

    return pairs