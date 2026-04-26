import pandas as pd
from pathlib import Path

DEBUG = True

def _cell_text(value):
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _score_header_row(row_values):
    values = [_cell_text(v) for v in row_values]
    non_empty = [v for v in values if v]

    if not non_empty:
        return -1

    unique_count = len(set(non_empty))
    text_like = sum(1 for v in non_empty if not v.isdigit())
    longish = sum(1 for v in non_empty if len(v) > 2)

    # reward:
    # - more populated columns
    # - more unique labels
    # - text-like values
    # - slightly longer labels
    score = (
        len(non_empty) * 3
        + unique_count * 2
        + text_like * 2
        + longish
    )

    return score

import re

def _is_date_like(value):
    v = _cell_text(value)
    if not v:
        return False

    return bool(
        re.match(r"^\d{4}-\d{2}-\d{2}", v) or
        re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", v)
    )

def detect_header_row(raw_df, max_scan_rows=15):
    scan_limit = min(max_scan_rows, len(raw_df))
    best_idx = 0
    best_score = -10**9

    for i in range(scan_limit):
        row_values = raw_df.iloc[i].tolist()
        score = _score_header_row(row_values)

        current = [_cell_text(v) for v in row_values]
        current_non_empty = [v for v in current if v]

        # header rows often have shorter label-like cells
        avg_len = (
            sum(len(v) for v in current_non_empty) / len(current_non_empty)
            if current_non_empty else 0
        )
        if avg_len > 40:
            score -= 8

        date_like_count = sum(1 for v in row_values if _is_date_like(v))
        if date_like_count >= 2:
            score -= date_like_count * 5

        # if next row overlaps heavily, current row is likely data, not header
        if i + 1 < scan_limit:
            next_values = [_cell_text(v) for v in raw_df.iloc[i + 1].tolist()]
            current_set = set(v for v in current if v)
            next_set = set(v for v in next_values if v)

            if current_set and next_set:
                overlap = len(current_set & next_set) / max(len(current_set), 1)
                score -= overlap * 10

        # strongly prefer first row when scores are close
        score -= i * 2

        if score > best_score:
            best_score = score
            best_idx = i

    return best_idx

def _apply_field_filters(rows, template_config=None):
    filters = (template_config or {}).get("filters", {})
    field_filters = filters.get("field_filters", [])

    if not field_filters:
        return rows

    filtered_rows = rows

    for rule in field_filters:
        field = str(rule.get("field") or "").strip()
        mode = str(rule.get("mode") or "").strip().lower()
        values = {
            str(v).strip().lower()
            for v in (rule.get("values") or [])
            if str(v).strip()
        }

        if not field or not values:
            continue

        if mode == "exclude_equals":
            filtered_rows = [
                row for row in filtered_rows
                if str(row.get(field, "")).strip().lower() not in values
            ]

        elif mode == "include_equals":
            filtered_rows = [
                row for row in filtered_rows
                if str(row.get(field, "")).strip().lower() in values
            ]

    return filtered_rows


def parse_table(file_path, template_config=None):
    path = Path(file_path)
    ext = path.suffix.lower()

    # --- Raw load without trusting first row as header ---
    if ext == ".csv":
        raw_df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
            on_bad_lines="skip",
            header=None
        )

    elif ext in [".xlsx", ".xls"]:
        raw_df = pd.read_excel(
            path,
            dtype=str,
            header=None
        )
        raw_df = raw_df.fillna("")

    else:
        raise ValueError(f"Unsupported table format: {ext}")

    # --- Detect header row ---
    header_row_idx = detect_header_row(raw_df)

    if DEBUG:
        print(f"[TABLE PARSER] Detected header row: {header_row_idx}")

    header_values = [_cell_text(v) for v in raw_df.iloc[header_row_idx].tolist()]
    data_df = raw_df.iloc[header_row_idx + 1:].copy()
    data_df.columns = header_values

    # drop fully empty columns
    data_df = data_df.loc[:, [str(c).strip() != "" for c in data_df.columns]]

    # --- Normalize column names ---
    data_df.columns = [str(col).strip() for col in data_df.columns]

    # --- Normalize values ---
    for col in data_df.columns:
        data_df[col] = data_df[col].map(lambda x: str(x).strip() if x is not None else "")

    # --- Convert to row dicts ---
    rows = data_df.to_dict(orient="records")

    if DEBUG:
        print(f"[TABLE PARSER] Rows before filters: {len(rows)}")
        print(f"[TABLE PARSER] Filters: {(template_config or {}).get('filters', {})}")

    rows = _apply_field_filters(rows, template_config)

    if DEBUG:
        print(f"[TABLE PARSER] Rows after filters: {len(rows)}")

    result = {
        "rows": rows,
        "schema": None,
        "columns": list(data_df.columns),
        "source_file": path.name,
        "source_path": str(path),
        "row_count": len(rows),
        "filetype": "table",
        "header_row_index": header_row_idx
    }

    if DEBUG:
        print(f"[TABLE PARSER] Loaded {result['row_count']} rows from {result['source_file']}")
        print(f"[TABLE PARSER] Columns: {result['columns']}")
        if rows:
            print(f"[TABLE PARSER] Row sample: {rows[0]}")

    return result