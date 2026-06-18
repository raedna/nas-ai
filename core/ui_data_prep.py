"""
core/ui_data_prep.py
--------------------
Data Preparation tab for NAS-AI Streamlit UI.

Supports:
- Load 1-2 CSV files and preview
- Column operations: rename, derive (dropdown + custom), drop, filter rows
- Two-file join with key normalization and optional fuzzy matching (rapidfuzz)
- Preview merged result
- Export as CSV (download or save to path for ingestion)

File-based only. API-based ingestion is a separate path (Phase 4).
"""

from __future__ import annotations
import io
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st


# ---------------------------------------------------------------------------
# Fuzzy matching — graceful fallback if rapidfuzz not installed
# ---------------------------------------------------------------------------
try:
    from rapidfuzz import fuzz, process as rfprocess
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_key(val: str, strip: bool, lowercase: bool, prefix: str, strip_prefix: str) -> str:
    """Apply key normalization options to a string value."""
    val = str(val) if val is not None else ""
    if strip_prefix and val.startswith(strip_prefix):
        val = val[len(strip_prefix):].strip()
    if strip:
        val = val.strip()
    if lowercase:
        val = val.lower()
    if prefix:
        val = prefix + val
    return val


def _apply_normalization(series: pd.Series, strip: bool, lowercase: bool, prefix: str, strip_prefix: str) -> pd.Series:
    return series.apply(lambda v: _normalize_key(str(v), strip, lowercase, prefix, strip_prefix))


def _derive_column(df: pd.DataFrame, new_col: str, method: str, custom_expr: str,
                   col_a: str, col_b: str, prefix: str, suffix: str,
                   condition_col: str, condition_val: str, true_col: str, false_col: str) -> pd.DataFrame:
    """Apply a derive operation to create a new column."""
    df = df.copy()
    try:
        if method == "prefix + column":
            df[new_col] = prefix + df[col_a].astype(str)
        elif method == "column + suffix":
            df[new_col] = df[col_a].astype(str) + suffix
        elif method == "concat two columns":
            df[new_col] = df[col_a].astype(str) + df[col_b].astype(str)
        elif method == "if/else (condition)":
            mask = df[condition_col].astype(str).str.strip() == str(condition_val).strip()
            df[new_col] = mask.map({True: df[true_col] if true_col in df.columns else true_col,
                                     False: df[false_col] if false_col in df.columns else false_col})
            # If columns selected use their values; if literals just broadcast
            if true_col in df.columns:
                df.loc[mask, new_col] = df.loc[mask, true_col].astype(str)
            else:
                df.loc[mask, new_col] = str(true_col)
            if false_col in df.columns:
                df.loc[~mask, new_col] = df.loc[~mask, false_col].astype(str)
            else:
                df.loc[~mask, new_col] = str(false_col)
        elif method == "Custom":
            print(f"[DERIVE] custom_expr='{custom_expr}' new_col='{new_col}'")
            try:
                df[new_col] = df.apply(
                    lambda row: eval(custom_expr, {"row": row, "str": str, "int": int, "float": float, "len": len}),
                    axis=1
                )
                print(f"[DERIVE] success, new columns: {list(df.columns)}")
            except Exception as eval_err:
                print(f"[DERIVE ERROR] {eval_err}")
                st.error(f"Expression error: {eval_err}")
                return df
        st.success(f"Column '{new_col}' created.")
    except Exception as e:
        st.error(f"Error deriving column: {e}")
    return df


def _fuzzy_join(left: pd.DataFrame, right: pd.DataFrame,
                left_key: str, right_key: str,
                join_type: str, threshold: int) -> pd.DataFrame:
    """Fuzzy join two dataframes on key columns using rapidfuzz."""
    if not RAPIDFUZZ_AVAILABLE:
        st.error("rapidfuzz not installed. Run: pip install rapidfuzz --break-system-packages")
        return left

    right_keys = right[right_key].astype(str).tolist()
    matches = []

    for lval in left[left_key].astype(str):
        result = rfprocess.extractOne(lval, right_keys, scorer=fuzz.token_sort_ratio)
        if result and result[1] >= threshold:
            matches.append((lval, result[0], result[1]))
        else:
            matches.append((lval, None, 0))

    match_df = pd.DataFrame(matches, columns=[left_key, "_right_match_", "_score_"])
    left_with_match = left.copy()
    left_with_match["_right_match_"] = match_df["_right_match_"].values
    left_with_match["_fuzzy_score_"] = match_df["_score_"].values

    right_renamed = right.rename(columns={right_key: "_right_match_"})
    merged = left_with_match.merge(right_renamed, on="_right_match_", how=join_type.lower())
    merged = merged.drop(columns=["_right_match_"], errors="ignore")
    return merged


# ---------------------------------------------------------------------------
# Session state keys
# ---------------------------------------------------------------------------
_SS_DF1 = "dp_df1"
_SS_DF2 = "dp_df2"
_SS_RESULT = "dp_result"
_SS_FILE1_NAME = "dp_file1_name"
_SS_FILE2_NAME = "dp_file2_name"


def _init_session():
    for key in [_SS_DF1, _SS_DF2, _SS_RESULT, _SS_FILE1_NAME, _SS_FILE2_NAME]:
        if key not in st.session_state:
            st.session_state[key] = None


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def render_data_prep_tab():
    """Render the Data Preparation tab."""
    _init_session()

    st.subheader("Data Preparation")
    st.caption("Load, clean, transform, and join CSV files before ingestion. File-based only — API ingestion is a separate path.")

    # ── SECTION 1: LOAD FILES ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 1. Load Files")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**File 1**")
        f1 = st.file_uploader("Upload CSV (File 1)", type=["csv"], key="dp_upload_1")
        if f1:
            if st.session_state.get(_SS_DF1) is None or st.session_state.get(_SS_FILE1_NAME) != f1.name:
                try:
                    df1_loaded = pd.read_csv(f1)
                    st.session_state[_SS_DF1] = df1_loaded
                    st.session_state[_SS_FILE1_NAME] = f1.name
                    st.success(f"{f1.name} — {len(df1_loaded)} rows, {len(df1_loaded.columns)} columns")
                except Exception as e:
                    st.error(f"Error reading file: {e}")
            else:
                _d = st.session_state[_SS_DF1]
                st.success(f"{f1.name} — {len(_d)} rows, {len(_d.columns)} columns")

        if st.session_state[_SS_DF1] is not None:
            st.dataframe(st.session_state[_SS_DF1].head(20), width='stretch')

    with col2:
        st.markdown("**File 2 (optional — for join)**")
        f2 = st.file_uploader("Upload CSV (File 2)", type=["csv"], key="dp_upload_2")
        if f2:
            if st.session_state.get(_SS_DF2) is None or st.session_state.get(_SS_FILE2_NAME) != f2.name:
                try:
                    df2_loaded = pd.read_csv(f2)
                    st.session_state[_SS_DF2] = df2_loaded
                    st.session_state[_SS_FILE2_NAME] = f2.name
                    st.success(f"{f2.name} — {len(df2_loaded)} rows, {len(df2_loaded.columns)} columns")
                except Exception as e:
                    st.error(f"Error reading file: {e}")
            else:
                _d = st.session_state[_SS_DF2]
                st.success(f"{f2.name} — {len(_d)} rows, {len(_d.columns)} columns")

        if st.session_state[_SS_DF2] is not None:
            st.dataframe(st.session_state[_SS_DF2].head(20), width='stretch')

    if st.session_state[_SS_DF1] is None:
        st.info("Upload at least one CSV file to get started.")
        return

    # Work on a copy — don't mutate session state until confirmed
    df1 = st.session_state[_SS_DF1].copy()
    df2 = st.session_state[_SS_DF2].copy() if st.session_state[_SS_DF2] is not None else None

    # ── SECTION 2: COLUMN OPERATIONS ───────────────────────────────────────
    st.markdown("---")
    st.markdown("### 2. Column Operations")
    st.caption("Operations apply to File 1 first, then File 2 (if loaded), before the join.")

    # ── 2a. Rename columns
    with st.expander("Rename Columns", expanded=False):
        st.markdown("**File 1 columns:**")
        rename_map_1 = {}
        cols_per_row = 3
        col1_cols = list(df1.columns)
        rows = [col1_cols[i:i+cols_per_row] for i in range(0, len(col1_cols), cols_per_row)]
        for row_cols in rows:
            ui_cols = st.columns(cols_per_row)
            for j, col in enumerate(row_cols):
                new_name = ui_cols[j].text_input(f"'{col}'", value=col, key=f"dp_rename1_{col}")
                if new_name and new_name != col:
                    rename_map_1[col] = new_name

        if rename_map_1 and st.button("Apply Rename (File 1)", key="dp_apply_rename1"):
            df1 = df1.rename(columns=rename_map_1)
            st.session_state[_SS_DF1] = df1
            st.success(f"Renamed {len(rename_map_1)} columns in File 1.")
            st.rerun()

        if df2 is not None:
            st.markdown("**File 2 columns:**")
            rename_map_2 = {}
            col2_cols = list(df2.columns)
            rows2 = [col2_cols[i:i+cols_per_row] for i in range(0, len(col2_cols), cols_per_row)]
            for row_cols in rows2:
                ui_cols = st.columns(cols_per_row)
                for j, col in enumerate(row_cols):
                    new_name = ui_cols[j].text_input(f"'{col}'", value=col, key=f"dp_rename2_{col}")
                    if new_name and new_name != col:
                        rename_map_2[col] = new_name

            if rename_map_2 and st.button("Apply Rename (File 2)", key="dp_apply_rename2"):
                df2 = df2.rename(columns=rename_map_2)
                st.session_state[_SS_DF2] = df2
                st.success(f"Renamed {len(rename_map_2)} columns in File 2.")
                st.rerun()

    # ── 2b. Derive new column
    with st.expander("Derive / Modify Column", expanded=False):
        all_cols_1 = list(df1.columns)
        modify_existing = st.checkbox("Modify existing column (overwrite)", key="dp_modify_existing")
        if modify_existing:
            new_col_name = st.selectbox("Column to modify", all_cols_1, key="dp_modify_col_select")
        else:
            new_col_name = st.text_input("New column name", key="dp_derive_name")

        derive_method = st.selectbox("Method", [
            "Add prefix",
            "Add suffix",
            "Combine two columns",
            "Coalesce (use col B if col A is empty/zero)",
            "Uppercase",
            "Lowercase",
            "Strip spaces",
            "If value equals → replace",
            "Custom",
        ], key="dp_derive_method")

        col_a = col_b = prefix_val = suffix_val = ""
        condition_col = condition_val_str = true_col = false_col = ""
        custom_expr = ""
        skip_val = ""
        separator = " "

        if derive_method == "Add prefix":
            prefix_val = st.text_input("Prefix to add (e.g. 'NGC ', 'M')", key="dp_prefix")
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_prefix")
            skip_val = st.text_input("Leave empty if value equals (optional, e.g. '0')", key="dp_skip_val")
            if skip_val:
                st.caption(f"Result: if {col_a} == '{skip_val}' → empty string, else '{prefix_val}' + {col_a}")
                custom_expr = f"'' if str(row['{col_a}']).strip() in ('{skip_val}', '0.0', '{skip_val}.0') else '{prefix_val}' + str(int(float(row['{col_a}']))) if str(row['{col_a}']).replace('.','').isdigit() else '{prefix_val}' + str(row['{col_a}'])"
            else:
                st.caption(f"Result: '{prefix_val}' + {col_a}")
                custom_expr = f"'{prefix_val}' + str(row['{col_a}'])"
            derive_method = "Custom"

        elif derive_method == "Add suffix":
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_suffix")
            suffix_val = st.text_input("Suffix to add", key="dp_suffix")
            skip_val = st.text_input("Leave empty if value equals (optional)", key="dp_skip_val_suffix")
            if skip_val:
                st.caption(f"Result: if {col_a} == '{skip_val}' → empty string, else {col_a} + '{suffix_val}'")
                custom_expr = f"'' if str(row['{col_a}']).strip() == '{skip_val}' else str(row['{col_a}']) + '{suffix_val}'"
            else:
                st.caption(f"Result: {col_a} + '{suffix_val}'")
                custom_expr = f"str(row['{col_a}']) + '{suffix_val}'"
            derive_method = "Custom"

        elif derive_method == "Combine two columns":
            col_a = st.selectbox("First column", all_cols_1, key="dp_col_a_concat")
            separator = st.text_input("Separator (e.g. ' ', '_', '')", value=" ", key="dp_separator")
            col_b = st.selectbox("Second column", all_cols_1, key="dp_col_b_concat")
            st.caption(f"Result: {col_a} + '{separator}' + {col_b}")
            custom_expr = f"str(row['{col_a}']) + '{separator}' + str(row['{col_b}'])"
            derive_method = "Custom"

        elif derive_method == "Coalesce (use col B if col A is empty/zero)":
            col_a = st.selectbox("Primary column (use this if not empty)", all_cols_1, key="dp_col_a_coalesce")
            col_b = st.selectbox("Fallback column (use this if primary is empty)", all_cols_1, key="dp_col_b_coalesce")
            skip_val = st.text_input("Also treat this value as empty (e.g. '0', 'None')", value="0", key="dp_coalesce_empty")
            st.caption(f"Result: {col_a} if not empty/zero, else {col_b}")
            custom_expr = f"str(row['{col_b}']) if str(row['{col_a}']).strip() in ('', 'None', 'nan', '{skip_val}', '0', '0.0') else str(row['{col_a}'])"
            derive_method = "Custom"

        elif derive_method == "Uppercase":
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_upper")
            st.caption(f"Result: {col_a} → uppercase")
            custom_expr = f"str(row['{col_a}']).upper()"
            derive_method = "Custom"

        elif derive_method == "Lowercase":
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_lower")
            st.caption(f"Result: {col_a} → lowercase")
            custom_expr = f"str(row['{col_a}']).lower()"
            derive_method = "Custom"

        elif derive_method == "Strip spaces":
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_strip")
            st.caption(f"Result: {col_a} → trimmed")
            custom_expr = f"str(row['{col_a}']).strip()"
            derive_method = "Custom"

        elif derive_method == "If value equals → replace":
            col_a = st.selectbox("Column", all_cols_1, key="dp_col_a_replace")
            condition_val_str = st.text_input("If value equals", key="dp_replace_if")
            true_col = st.text_input("Replace with", key="dp_replace_with")
            false_col = st.text_input("Otherwise (leave blank to keep original)", key="dp_replace_else")
            if false_col:
                st.caption(f"If {col_a} == '{condition_val_str}' → '{true_col}', else → '{false_col}'")
                custom_expr = f"'{true_col}' if str(row['{col_a}']).strip() == '{condition_val_str}' else '{false_col}'"
            else:
                st.caption(f"If {col_a} == '{condition_val_str}' → '{true_col}', else keep original")
                custom_expr = f"'{true_col}' if str(row['{col_a}']).strip() == '{condition_val_str}' else str(row['{col_a}'])"
            derive_method = "Custom"

        elif derive_method == "Custom":
            st.caption("Ask Claude for the expression if needed. Use `row['column_name']` to access values.")
            custom_expr = st.text_area("Python expression (evaluated per row)", key="dp_custom_expr", height=80)

        # Store auto-generated expression so button handler can read it
        if custom_expr:
            st.session_state["dp_generated_expr"] = custom_expr

        target_file = st.radio("Apply to", ["File 1", "File 2"], key="dp_derive_target", horizontal=True) if df2 is not None else "File 1"

        if st.button("Add Column", key="dp_add_col") and new_col_name:
            _custom_expr = st.session_state.get("dp_generated_expr") or st.session_state.get("dp_custom_expr", "")
            _derive_method = "Custom"
            if target_file == "File 1":
                _src = st.session_state[_SS_DF1].copy()
                _result = _derive_column(_src, new_col_name, _derive_method, _custom_expr,
                                         col_a, col_b, prefix_val, suffix_val,
                                         condition_col, condition_val_str, true_col, false_col)
                st.session_state[_SS_DF1] = _result
                print(f"[SAVED] df1 columns: {list(_result.columns)}")
            else:
                _src = st.session_state[_SS_DF2].copy()
                _result = _derive_column(_src, new_col_name, _derive_method, _custom_expr,
                                         col_a, col_b, prefix_val, suffix_val,
                                         condition_col, condition_val_str, true_col, false_col)
                st.session_state[_SS_DF2] = _result
            st.rerun()

    # ── 2c. Drop columns
    with st.expander("Drop Columns", expanded=False):
        drop_cols_1 = st.multiselect("Drop from File 1", list(df1.columns), key="dp_drop1")
        if drop_cols_1 and st.button("Drop Selected (File 1)", key="dp_drop1_btn"):
            df1 = df1.drop(columns=drop_cols_1, errors="ignore")
            st.session_state[_SS_DF1] = df1
            st.success(f"Dropped {len(drop_cols_1)} columns from File 1.")
            st.rerun()

        if df2 is not None:
            drop_cols_2 = st.multiselect("Drop from File 2", list(df2.columns), key="dp_drop2")
            if drop_cols_2 and st.button("Drop Selected (File 2)", key="dp_drop2_btn"):
                df2 = df2.drop(columns=drop_cols_2, errors="ignore")
                st.session_state[_SS_DF2] = df2
                st.success(f"Dropped {len(drop_cols_2)} columns from File 2.")
                st.rerun()

    # ── 2d. Filter rows
    with st.expander("Filter Rows", expanded=False):
        filter_target = st.radio("Filter file", ["File 1", "File 2"], key="dp_filter_target", horizontal=True) if df2 is not None else "File 1"
        filter_df = df1 if filter_target == "File 1" else df2
        if filter_df is not None:
            filter_col = st.selectbox("Column", list(filter_df.columns), key="dp_filter_col")
            filter_op = st.selectbox("Condition", [
                "equals", "not equals", "contains", "not contains",
                "is empty", "is not empty", "greater than", "less than"
            ], key="dp_filter_op")
            filter_val = ""
            if filter_op not in ("is empty", "is not empty"):
                filter_val = st.text_input("Value", key="dp_filter_val")

            if st.button("Apply Filter", key="dp_filter_btn"):
                try:
                    col_s = filter_df[filter_col].astype(str)
                    if filter_op == "equals":
                        mask = col_s == filter_val
                    elif filter_op == "not equals":
                        mask = col_s != filter_val
                    elif filter_op == "contains":
                        mask = col_s.str.contains(filter_val, na=False)
                    elif filter_op == "not contains":
                        mask = ~col_s.str.contains(filter_val, na=False)
                    elif filter_op == "is empty":
                        mask = col_s.str.strip() == ""
                    elif filter_op == "is not empty":
                        mask = col_s.str.strip() != ""
                    elif filter_op == "greater than":
                        mask = filter_df[filter_col].astype(float) > float(filter_val)
                    elif filter_op == "less than":
                        mask = filter_df[filter_col].astype(float) < float(filter_val)
                    else:
                        mask = pd.Series([True] * len(filter_df))

                    before = len(filter_df)
                    filtered = filter_df[mask]
                    after = len(filtered)
                    if filter_target == "File 1":
                        st.session_state[_SS_DF1] = filtered
                    else:
                        st.session_state[_SS_DF2] = filtered
                    st.success(f"Filtered: {before} → {after} rows ({before - after} removed).")
                    st.rerun()
                except Exception as e:
                    st.error(f"Filter error: {e}")

    # ── SECTION 3: JOIN ────────────────────────────────────────────────────
    if df2 is not None:
        st.markdown("---")
        st.markdown("### 3. Join Files")

        jc1, jc2 = st.columns(2)
        with jc1:
            left_key = st.selectbox("File 1 join key", list(df1.columns), key="dp_left_key")
        with jc2:
            right_key = st.selectbox("File 2 join key", list(df2.columns), key="dp_right_key")

        join_type = st.selectbox("Join type", ["Left", "Right", "Inner", "Outer"], key="dp_join_type")

        with st.expander("Key Normalization", expanded=True):
            st.caption("Applied to both key columns before matching.")
            nc1, nc2, nc3, nc4 = st.columns(4)
            norm_strip = nc1.checkbox("Strip spaces", value=True, key="dp_norm_strip")
            norm_lower = nc2.checkbox("Lowercase", value=True, key="dp_norm_lower")
            norm_prefix = nc3.text_input("Add prefix (e.g. 'NGC ')", key="dp_norm_prefix")
            norm_strip_prefix = nc4.text_input("Strip prefix (e.g. 'NGC ')", key="dp_norm_strip_prefix")

        use_fuzzy = st.checkbox(
            f"Fuzzy matching {'(rapidfuzz available ✅)' if RAPIDFUZZ_AVAILABLE else '(rapidfuzz NOT installed ❌)'}",
            key="dp_use_fuzzy",
            disabled=not RAPIDFUZZ_AVAILABLE
        )
        fuzzy_threshold = 85
        if use_fuzzy and RAPIDFUZZ_AVAILABLE:
            fuzzy_threshold = st.slider("Fuzzy match threshold (%)", 70, 100, 85, key="dp_fuzzy_threshold")
            st.caption(f"Matches scoring >= {fuzzy_threshold}% similarity will be joined.")

        if st.button("Run Join", key="dp_join_btn", type="primary"):
            try:
                df1_j = df1.copy()
                df2_j = df2.copy()

                # Apply normalization to key columns
                df1_j["_join_key_"] = _apply_normalization(
                    df1_j[left_key], norm_strip, norm_lower, norm_prefix, norm_strip_prefix
                )
                df2_j["_join_key_"] = _apply_normalization(
                    df2_j[right_key], norm_strip, norm_lower, norm_prefix, norm_strip_prefix
                )

                if use_fuzzy and RAPIDFUZZ_AVAILABLE:
                    result = _fuzzy_join(df1_j, df2_j, "_join_key_", "_join_key_",
                                         join_type, fuzzy_threshold)
                else:
                    result = df1_j.merge(df2_j, on="_join_key_", how=join_type.lower())

                result = result.drop(columns=["_join_key_"], errors="ignore")
                st.session_state[_SS_RESULT] = result
                st.success(f"Join complete — {len(result)} rows, {len(result.columns)} columns.")
                st.rerun()
            except Exception as e:
                st.error(f"Join error: {e}")

    # ── SECTION 4: PREVIEW RESULT ──────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 4. Preview Result")

    result = st.session_state.get(_SS_RESULT)
    if result is None:
        result = st.session_state[_SS_DF1].copy()
        st.caption("Showing current File 1 — run join to see merged result.")

    if result is not None:
        st.caption(f"{len(result)} rows × {len(result.columns)} columns")
        preview_n = st.slider("Rows to preview", 10, 200, 50, key="dp_preview_n")
        st.dataframe(result.head(preview_n), width='stretch')

        # Column ops on result
        with st.expander("Post-join Column Operations", expanded=False):

            # Auto-coalesce _x/_y columns
            st.markdown("**Merge Matching Columns (_x / _y)**")
            x_cols = [c for c in result.columns if c.endswith("_x")]
            y_cols = [c for c in result.columns if c.endswith("_y")]
            pairs = [(c, c[:-2] + "_y") for c in x_cols if c[:-2] + "_y" in result.columns]
            if pairs:
                st.caption(f"Found {len(pairs)} matching column pairs: {', '.join(c[:-2] for c,_ in pairs)}")
                prefer = st.radio("When both have values, prefer", ["File 1 (_x)", "File 2 (_y)"], key="dp_coalesce_prefer", horizontal=True)
                if st.button("Merge _x/_y columns", key="dp_coalesce_btn", type="primary"):
                    r = st.session_state[_SS_RESULT].copy()
                    for col_x, col_y in pairs:
                        base = col_x[:-2]
                        if prefer == "File 1 (_x)":
                            r[base] = r[col_x].where(
                                ~r[col_x].isna() & (r[col_x].astype(str).str.strip() != "") & (r[col_x].astype(str).str.strip() != "nan"),
                                r[col_y]
                            )
                        else:
                            r[base] = r[col_y].where(
                                ~r[col_y].isna() & (r[col_y].astype(str).str.strip() != "") & (r[col_y].astype(str).str.strip() != "nan"),
                                r[col_x]
                            )
                        r = r.drop(columns=[col_x, col_y], errors="ignore")
                    st.session_state[_SS_RESULT] = r
                    st.success(f"Merged {len(pairs)} column pairs — dropped {len(pairs)*2} columns, added {len(pairs)} merged columns.")
                    st.rerun()
            else:
                st.caption("No _x/_y column pairs found. Run a join first.")

            st.markdown("---")
            result_cols = list(result.columns)
            rename_map_r = {}
            cols_per_row = 3
            rows_r = [result_cols[i:i+cols_per_row] for i in range(0, len(result_cols), cols_per_row)]
            for row_cols in rows_r:
                ui_cols = st.columns(cols_per_row)
                for j, col in enumerate(row_cols):
                    new_name = ui_cols[j].text_input(f"'{col}'", value=col, key=f"dp_rename_result_{col}")
                    if new_name and new_name != col:
                        rename_map_r[col] = new_name
            if rename_map_r and st.button("Apply Rename", key="dp_apply_rename_result"):
                result = result.rename(columns=rename_map_r)
                st.session_state[_SS_RESULT] = result
                st.success(f"Renamed {len(rename_map_r)} columns.")
                st.rerun()

            st.markdown("---")

            # Drop columns
            st.markdown("**Drop Columns**")
            drop_result_cols = st.multiselect("Select columns to drop", list(result.columns), key="dp_drop_result")
            if drop_result_cols and st.button("Drop from Result", key="dp_drop_result_btn"):
                result = result.drop(columns=drop_result_cols, errors="ignore")
                st.session_state[_SS_RESULT] = result
                st.success(f"Dropped {len(drop_result_cols)} columns.")
                st.rerun()

            st.markdown("---")

            new_col_r = st.text_input("New column name (result)", key="dp_derive_result_name")
            derive_method_r = st.selectbox("Method", [
                "prefix + column", "column + suffix", "concat two columns",
                "if/else (condition)", "Custom"
            ], key="dp_derive_result_method")
            col_a_r = st.selectbox("Column A", list(result.columns), key="dp_col_a_result") if list(result.columns) else ""
            col_b_r = st.selectbox("Column B", list(result.columns), key="dp_col_b_result") if len(result.columns) > 1 else col_a_r
            prefix_r = st.text_input("Prefix", key="dp_prefix_result")
            suffix_r = st.text_input("Suffix", key="dp_suffix_result")
            cond_col_r = st.selectbox("Condition column", list(result.columns), key="dp_cond_col_result") if list(result.columns) else ""
            cond_val_r = st.text_input("Condition value", key="dp_cond_val_result")
            true_r = st.text_input("True value/column", key="dp_true_result")
            false_r = st.text_input("False value/column", key="dp_false_result")
            custom_r = st.text_area("Custom expression", key="dp_custom_result", height=60)

            if st.button("Add Column to Result", key="dp_add_result_col") and new_col_r:
                result = _derive_column(result, new_col_r, derive_method_r, custom_r,
                                        col_a_r, col_b_r, prefix_r, suffix_r,
                                        cond_col_r, cond_val_r, true_r, false_r)
                st.session_state[_SS_RESULT] = result
                st.rerun()

        # ── SECTION 5: EXPORT ──────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 5. Export")

        ec1, ec2 = st.columns(2)
        with ec1:
            csv_bytes = result.to_csv(index=False).encode("utf-8")
            default_name = "prepared_data.csv"
            if st.session_state.get(_SS_FILE1_NAME):
                stem = Path(st.session_state[_SS_FILE1_NAME]).stem
                default_name = f"{stem}_prepared.csv"
            st.download_button(
                label="Download CSV",
                data=csv_bytes,
                file_name=default_name,
                mime="text/csv",
                key="dp_download"
            )

        with ec2:
            save_path = st.text_input(
                "Or save to path (for direct ingestion)",
                placeholder="/Users/.../nas-ai/data/astro_catalog.csv",
                key="dp_save_path"
            )
            if st.button("Save to Path", key="dp_save_btn") and save_path:
                try:
                    Path(save_path).parent.mkdir(parents=True, exist_ok=True)
                    result.to_csv(save_path, index=False)
                    st.success(f"Saved to {save_path} ({len(result)} rows).")
                except Exception as e:
                    st.error(f"Save error: {e}")

        # Reset button
        st.markdown("---")
        if st.button("Reset All", key="dp_reset", type="secondary"):
            for key in [_SS_DF1, _SS_DF2, _SS_RESULT, _SS_FILE1_NAME, _SS_FILE2_NAME]:
                st.session_state[key] = None
            st.rerun()
