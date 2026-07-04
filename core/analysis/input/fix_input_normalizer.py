import re
from typing import Dict, List


SOH = "\x01"

def reconstruct_vertical_fix_table_lines(lines: List[str]) -> List[str]:
    """
    Reconstruct OCR output where a FIX table is read vertically as separate cells.

    Example OCR:
        35
        MsgType
        8
        ExecutionReport

    Becomes:
        35 MsgType 8 ExecutionReport

    Also handles rows where the tag number is missing but field name/value exist:
        BeginString
        FIX.4.4

    Becomes:
        BeginString FIX.4.4
    """
    cleaned = [str(line or "").strip() for line in lines]
    cleaned = [line for line in cleaned if line]

    # Drop common table headers.
    header_words = {
        "tag",
        "tag name",
        "name",
        "value",
        "value name",
    }

    cleaned = [
        line for line in cleaned
        if line.strip().lower() not in header_words
    ]

    rows: List[str] = []
    i = 0

    while i < len(cleaned):
        current = cleaned[i]

        # Standard pattern:
        # tag, field name, value, optional value name
        if current.isdigit():
            tag = current
            name = cleaned[i + 1] if i + 1 < len(cleaned) else ""
            value = cleaned[i + 2] if i + 2 < len(cleaned) else ""

            value_name = ""

            # If next item after value is not a tag number, treat it as value name.
            if i + 3 < len(cleaned) and not cleaned[i + 3].isdigit():
                value_name = cleaned[i + 3]
                i += 4
            else:
                i += 3

            row = " ".join(part for part in [tag, name, value, value_name] if part)
            rows.append(row)
            continue

        # Headerless first rows sometimes appear as:
        # BeginString, FIX.4.4
        # BodyLength, 1192
        if i + 1 < len(cleaned) and not cleaned[i + 1].isdigit():
            name = current
            value = cleaned[i + 1]
            rows.append(f"{name} {value}")
            i += 2
            continue

        rows.append(current)
        i += 1

    return rows

def normalize_fix_input(raw: str) -> str:
    text = raw or ""

    # Common real / escaped delimiters
    text = text.replace("\\x01", SOH)
    text = text.replace("^A", SOH)

    # OCR often reads SOH as SOH, S0H, [S0H, etc.
    text = re.sub(r"[\[\(\{]?\s*S[O0]H\s*[\]\)\}]?", SOH, text, flags=re.IGNORECASE)

    text = text.replace("|", SOH)

    # OCR / visible SOH variants
    text = re.sub(r"[\[\(<{]?\s*S[O0]\s*H\s*[\]\)>}]?", SOH, text, flags=re.IGNORECASE)

    # Normalize tag spacing caused by OCR or formatted copy:
    # 35 = 8  -> 35=8
    text = re.sub(r"\b(\d{1,5})\s*=\s*", r"\1=", text)

    # Reduce repeated delimiters
    text = re.sub(r"\x01+", SOH, text)

    return text.strip()

def clean_ocr_table_line(line: str) -> str:
    """
    Light OCR cleanup for screenshot/table text.
    Does not apply FIX meanings. Only removes visual/table noise.
    """
    line = line.strip()

    replacements = {
        "——": " ",
        "—": " ",
        "–": " ",
        "|": " ",
        "[": " ",
        "]": " ",
        "(": " ",
        ")": " ",
        "{": " ",
        "}": " ",
        "“": "",
        "”": "",
        "‘": "",
        "’": "",
        '"': "",
    }

    for old, new in replacements.items():
        line = line.replace(old, new)

    line = re.sub(r"\s+", " ", line)
    return line.strip()


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
    # Supports rows like:
    # Tag   Tag Name        Value       Value Name
    # 35    MsgType         8           ExecutionReport
    # 54    Side            2           Sell
    ocr_lines = [
        line.strip()
        for line in raw.splitlines()
        if line.strip()
    ]

    if len(ocr_lines) >= 6:
        reconstructed_lines = reconstruct_vertical_fix_table_lines(ocr_lines)

        reconstructed_score = sum(
            1 for line in reconstructed_lines
            if re.match(r"^\d{1,5}\s+\S+\s+\S+", line)
        )

        original_score = sum(
            1 for line in ocr_lines
            if re.match(r"^\d{1,5}\s+\S+\s+\S+", line)
        )

        if reconstructed_score > original_score:
            print("=== VERTICAL OCR TABLE RECONSTRUCTED ===", flush=True)
            ocr_lines = reconstructed_lines

    for line in ocr_lines:
        line = line.strip()
        if not line:
            continue

        # Skip obvious headers/separators
        if re.search(r"\btag\b", line, flags=re.IGNORECASE) and re.search(r"\bvalue\b", line, flags=re.IGNORECASE):
            continue
        if set(line) <= {"-", "=", "|", "+", " "}:
            continue

        # Prefer table separators first
        if "|" in line:
            cols = [c.strip() for c in line.split("|") if c.strip()]
        else:
            cols = re.split(r"\t+|\s{2,}", line)

        if len(cols) >= 3 and cols[0].strip().isdigit():
            pairs.append({
                "tag": cols[0].strip(),
                "value": cols[2].strip(),
            })
            continue

        # OCR fallback: row begins with tag and later has a value.
        # Example: "35 MsgType 8 ExecutionReport"
        clean_line = clean_ocr_table_line(line)

        # OCR fallback for rows like:
        # 35 |MsgType BT ——ExecutionReport
        # 49 |SenderCompID [BLPMULT)
        # 56 [TargetCompID = [MOORECRD2-
        m = re.match(r"^(\d{1,5})\s+([A-Za-z][A-Za-z0-9_./-]*)\s+(.+)$", clean_line)
        if m:
            tag = m.group(1).strip()
            value_part = m.group(3).strip()

            # Remove obvious trailing OCR/table junk
            value_part = re.sub(r"\s+", " ", value_part).strip()

            # Take first token as the value column.
            # For FIX table screenshots, column order is: Tag | Tag Name | Value | Value Name
            tokens = [t.strip() for t in value_part.split() if t.strip()]

            # Skip obvious OCR/table separator tokens.
            junk_tokens = {"=", "-", "—", "–", "|"}
            tokens = [t for t in tokens if t not in junk_tokens]

            if tokens:
                value = tokens[0].strip()
                value_tail = " ".join(tokens[1:]).strip()

                pairs.append({
                    "tag": tag,
                    "value": value,
                    "value_tail": value_tail,
                    "raw_line": line,
                    "clean_line": clean_line,
                })

    return pairs


