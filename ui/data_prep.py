"""ui/data_prep.py — Data Preparation tab.

Load 1-2 CSVs, clean/transform columns, optionally join them, then export.

Ported from the Streamlit "Data Prep" tab (core/ui_data_prep.py), with two
changes:
  1. The "preset" derive methods (prefix/suffix/combine/coalesce/upper/lower/
     strip/if-equals) are implemented as real functions instead of building a
     Python expression string and eval()-ing it — the old approach broke (or
     silently mis-evaluated) on quotes/apostrophes in field values. "Custom"
     still offers a raw eval() box as an explicit, clearly-labeled escape
     hatch.
  2. Export is two modes instead of download/save-path: "Save only" (write
     the CSV to a path) and "Save + Ingest" (write it, then ingest that exact
     file into an existing collection — via core.ui_data.ingest_single_file,
     which does not touch the collection's normal scan folder). Picking a
     target collection pre-fills the save path with that collection's
     configured folder.
"""
import io
import json
from pathlib import Path

import pandas as pd
from nicegui import ui, run

from core.ui_data import collection_stats, collection_path_info, ingest_single_file

try:
    from rapidfuzz import fuzz, process as rfprocess
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

_METRICS = [
    ("total_files", "Files"), ("processed_files", "Processed"),
    ("skipped_files", "Skipped"), ("failed_files", "Failed"), ("total_chunks", "Chunks"),
]


# ===========================================================================
# Upload helpers (same pattern as ui/analysis.py)
# ===========================================================================
def _get_upload_filename(e) -> str:
    file_obj = getattr(e, "file", None)
    return (
        getattr(e, "name", None) or getattr(e, "filename", None)
        or getattr(file_obj, "name", None) or getattr(file_obj, "filename", None)
        or "uploaded.csv"
    )


async def _get_upload_bytes(e) -> bytes:
    content = getattr(e, "content", None)
    if content is not None:
        if hasattr(content, "read"):
            data = content.read()
            if hasattr(data, "__await__"):
                data = await data
            return data
        if isinstance(content, bytes):
            return content
    file_obj = getattr(e, "file", None)
    if file_obj is not None and hasattr(file_obj, "read"):
        data = file_obj.read()
        if hasattr(data, "__await__"):
            data = await data
        return data
    raise ValueError("Could not read uploaded file content.")


# ===========================================================================
# Safe derive presets (no eval on user-supplied text)
# ===========================================================================
def _clean_numeric_str(v) -> str:
    s = str(v)
    try:
        f = float(s)
        return str(int(f)) if f == int(f) else s
    except (ValueError, TypeError):
        return s


def _apply_derive_preset(df, target_col, method, col_a, col_b, prefix_val, suffix_val,
                          separator, skip_val, cond_val, true_val, false_val, custom_expr):
    df = df.copy()
    try:
        if method == "Add prefix":
            def fn(v):
                s = str(v).strip()
                if skip_val and s in (skip_val, f"{skip_val}.0"):
                    return ""
                return prefix_val + _clean_numeric_str(v)
            df[target_col] = df[col_a].apply(fn)

        elif method == "Add suffix":
            def fn(v):
                s = str(v).strip()
                if skip_val and s == skip_val:
                    return ""
                return str(v) + suffix_val
            df[target_col] = df[col_a].apply(fn)

        elif method == "Combine two columns":
            df[target_col] = df[col_a].astype(str) + separator + df[col_b].astype(str)

        elif method == "Coalesce (use col B if col A is empty/zero)":
            empties = {"", "None", "nan", "0", "0.0"}
            if skip_val:
                empties.add(skip_val)

            def fn(row):
                a = str(row[col_a]).strip()
                return str(row[col_b]) if a in empties else a
            df[target_col] = df.apply(fn, axis=1)

        elif method == "Uppercase":
            df[target_col] = df[col_a].astype(str).str.upper()

        elif method == "Lowercase":
            df[target_col] = df[col_a].astype(str).str.lower()

        elif method == "Strip spaces":
            df[target_col] = df[col_a].astype(str).str.strip()

        elif method == "If value equals \u2192 replace":
            mask = df[col_a].astype(str).str.strip() == str(cond_val).strip()
            fallback = df[col_a].astype(str) if not false_val else false_val
            df[target_col] = fallback
            df.loc[mask, target_col] = true_val

        elif method == "Custom":
            df[target_col] = df.apply(
                lambda row: eval(custom_expr, {"row": row, "str": str, "int": int, "float": float, "len": len}),
                axis=1,
            )

        ui.notify(f"Column '{target_col}' set.", type="positive")
    except Exception as e:
        ui.notify(f"Error deriving column: {e}", type="negative")
    return df


def _fuzzy_join(left, right, left_key, right_key, join_type, threshold):
    if not RAPIDFUZZ_AVAILABLE:
        ui.notify("rapidfuzz not installed. Run: pip install rapidfuzz --break-system-packages",
                   type="negative")
        return left
    right_keys = right[right_key].astype(str).tolist()
    matches = []
    for lval in left[left_key].astype(str):
        result = rfprocess.extractOne(lval, right_keys, scorer=fuzz.token_sort_ratio)
        matches.append((result[0], result[1]) if result and result[1] >= threshold else (None, 0))
    left = left.copy()
    left["_right_match_"] = [m[0] for m in matches]
    left["_fuzzy_score_"] = [m[1] for m in matches]
    right_renamed = right.rename(columns={right_key: "_right_match_"})
    merged = left.merge(right_renamed, on="_right_match_", how=join_type.lower())
    return merged.drop(columns=["_right_match_"], errors="ignore")


# ===========================================================================
# Small render helpers
# ===========================================================================
def _df_table(df, n=20):
    if df is None or df.empty:
        ui.label("(no rows)").classes("text-gray-500")
        return
    cols = [{"name": c, "label": c, "field": c, "align": "left"} for c in df.columns]
    rows = df.head(n).astype(str).to_dict("records")
    ui.table(columns=cols, rows=rows, row_key=cols[0]["name"]).classes("w-full").props("dense")


def _column_ops(df, on_apply, key_prefix):
    """Rename / Derive / Drop / Filter, generic over any dataframe. on_apply(new_df) is
    called (and should trigger a refresh) whenever an operation is applied."""
    with ui.expansion("Rename Columns").classes("w-full"):
        inputs = {}
        with ui.grid(columns=3).classes("w-full gap-2"):
            for col in df.columns:
                inputs[col] = ui.input(label=col, value=col).props("dense outlined")

        def do_rename():
            mapping = {c: w.value for c, w in inputs.items() if w.value and w.value != c}
            if not mapping:
                ui.notify("No renames entered.", type="warning")
                return
            on_apply(df.rename(columns=mapping))
        ui.button("Apply Rename", on_click=do_rename).props("unelevated").classes("mt-2")

    with ui.expansion("Derive / Modify Column").classes("w-full"):
        cols = list(df.columns)
        modify_existing = ui.checkbox("Modify existing column (overwrite)")
        new_name_in = ui.input("New column name")
        modify_col_sel = ui.select(cols, label="Column to modify")
        modify_col_sel.bind_visibility_from(modify_existing, "value")
        new_name_in.bind_visibility_from(modify_existing, "value", backward=lambda v: not v)

        method = ui.select([
            "Add prefix", "Add suffix", "Combine two columns",
            "Coalesce (use col B if col A is empty/zero)",
            "Uppercase", "Lowercase", "Strip spaces",
            "If value equals \u2192 replace", "Custom",
        ], value="Add prefix", label="Method").props("outlined dense").classes("w-64")

        col_a_sel = ui.select(cols, label="Column A", value=cols[0] if cols else None).props("outlined dense")
        col_b_sel = ui.select(cols, label="Column B", value=(cols[1] if len(cols) > 1 else (cols[0] if cols else None))).props("outlined dense")
        prefix_in = ui.input("Prefix")
        suffix_in = ui.input("Suffix")
        separator_in = ui.input("Separator", value=" ")
        skip_in = ui.input("Treat this value as empty (optional)")
        cond_in = ui.input("Condition value (If value equals)")
        true_in = ui.input("Replace with (True value)")
        false_in = ui.input("Otherwise (blank = keep original)")
        custom_in = ui.textarea("Custom Python expression \u2014 use row['column_name']").props("outlined")

        for w in (col_a_sel, col_b_sel, prefix_in, suffix_in, skip_in):
            w.bind_visibility_from(method, "value", backward=lambda v: v in (
                "Add prefix", "Add suffix"))
        separator_in.bind_visibility_from(method, "value", backward=lambda v: v == "Combine two columns")
        cond_in.bind_visibility_from(method, "value", backward=lambda v: v == "If value equals \u2192 replace")
        true_in.bind_visibility_from(method, "value", backward=lambda v: v == "If value equals \u2192 replace")
        false_in.bind_visibility_from(method, "value", backward=lambda v: v == "If value equals \u2192 replace")
        custom_in.bind_visibility_from(method, "value", backward=lambda v: v == "Custom")

        def do_derive():
            target = modify_col_sel.value if modify_existing.value else (new_name_in.value or "").strip()
            if not target:
                ui.notify("Enter a new column name (or choose a column to modify).", type="warning")
                return
            new_df = _apply_derive_preset(
                df, target, method.value, col_a_sel.value, col_b_sel.value,
                prefix_in.value or "", suffix_in.value or "", separator_in.value or " ",
                skip_in.value or "", cond_in.value or "", true_in.value or "", false_in.value or "",
                custom_in.value or "",
            )
            on_apply(new_df)
        ui.button("Add Column", on_click=do_derive).props("unelevated").classes("mt-2")

    with ui.expansion("Drop Columns").classes("w-full"):
        drop_sel = ui.select(list(df.columns), multiple=True, label="Columns to drop").props(
            "outlined dense use-chips").classes("w-full")

        def do_drop():
            if not drop_sel.value:
                ui.notify("Select at least one column.", type="warning")
                return
            on_apply(df.drop(columns=drop_sel.value, errors="ignore"))
        ui.button("Drop Selected", on_click=do_drop).props("unelevated").classes("mt-2")

    with ui.expansion("Filter Rows").classes("w-full"):
        fcol = ui.select(list(df.columns), label="Column", value=(list(df.columns)[0] if len(df.columns) else None)).props("outlined dense")
        fop = ui.select(["equals", "not equals", "contains", "not contains",
                          "is empty", "is not empty", "greater than", "less than"],
                         value="equals", label="Condition").props("outlined dense")
        fval = ui.input("Value")
        fval.bind_visibility_from(fop, "value", backward=lambda v: v not in ("is empty", "is not empty"))

        def do_filter():
            try:
                col_s = df[fcol.value].astype(str)
                op = fop.value
                if op == "equals":
                    mask = col_s == fval.value
                elif op == "not equals":
                    mask = col_s != fval.value
                elif op == "contains":
                    mask = col_s.str.contains(fval.value or "", na=False)
                elif op == "not contains":
                    mask = ~col_s.str.contains(fval.value or "", na=False)
                elif op == "is empty":
                    mask = col_s.str.strip() == ""
                elif op == "is not empty":
                    mask = col_s.str.strip() != ""
                elif op == "greater than":
                    mask = df[fcol.value].astype(float) > float(fval.value)
                else:
                    mask = df[fcol.value].astype(float) < float(fval.value)
                before = len(df)
                filtered = df[mask]
                ui.notify(f"Filtered: {before} \u2192 {len(filtered)} rows.", type="positive")
                on_apply(filtered)
            except Exception as e:
                ui.notify(f"Filter error: {e}", type="negative")
        ui.button("Apply Filter", on_click=do_filter).props("unelevated").classes("mt-2")


# ===========================================================================
# Main panel
# ===========================================================================
def render_data_prep_panel():
    names = sorted(r["name"] for r in collection_stats())
    state = {"df1": None, "df2": None, "result": None, "file1_name": None, "file2_name": None}

    root = ui.column().classes("w-full")

    def refresh():
        root.clear()
        with root:
            _build()

    def _build():
        ui.label("Data Preparation").classes("text-lg font-bold")
        ui.label("Load, clean, transform, and join CSV files before ingestion.").classes(
            "text-sm text-gray-500 mb-2")

        # ---- Section 1: Load Files -------------------------------------
        ui.markdown("### 1. Load Files")
        with ui.row().classes("w-full gap-4"):
            with ui.column().classes("flex-grow"):
                ui.label("File 1").classes("font-medium")

                async def on_upload_1(e):
                    name = _get_upload_filename(e)
                    data = await _get_upload_bytes(e)
                    try:
                        state["df1"] = pd.read_csv(io.BytesIO(data))
                        state["file1_name"] = name
                        ui.notify(f"{name} \u2014 {len(state['df1'])} rows, "
                                  f"{len(state['df1'].columns)} columns", type="positive")
                    except Exception as ex:
                        ui.notify(f"Error reading file: {ex}", type="negative")
                    refresh()
                ui.upload(label="Upload CSV (File 1)", on_upload=on_upload_1, auto_upload=True,
                          max_files=1).props("accept=.csv").classes("w-full")
                if state["df1"] is not None:
                    _df_table(state["df1"])

            with ui.column().classes("flex-grow"):
                ui.label("File 2 (optional \u2014 for join)").classes("font-medium")

                async def on_upload_2(e):
                    name = _get_upload_filename(e)
                    data = await _get_upload_bytes(e)
                    try:
                        state["df2"] = pd.read_csv(io.BytesIO(data))
                        state["file2_name"] = name
                        ui.notify(f"{name} \u2014 {len(state['df2'])} rows, "
                                  f"{len(state['df2'].columns)} columns", type="positive")
                    except Exception as ex:
                        ui.notify(f"Error reading file: {ex}", type="negative")
                    refresh()
                ui.upload(label="Upload CSV (File 2)", on_upload=on_upload_2, auto_upload=True,
                          max_files=1).props("accept=.csv").classes("w-full")
                if state["df2"] is not None:
                    _df_table(state["df2"])

        if state["df1"] is None:
            ui.label("Upload at least one CSV file to get started.").classes("text-gray-500 mt-2")
            return

        df1, df2 = state["df1"], state["df2"]

        # ---- Section 2: Column Operations -------------------------------
        ui.separator().classes("my-3")
        ui.markdown("### 2. Column Operations")
        ui.label("Applies to File 1, then File 2 (if loaded), before the join.").classes(
            "text-xs text-gray-500 mb-1")

        def apply1(new_df):
            state["df1"] = new_df
            refresh()
        ui.label("File 1").classes("font-medium mt-1")
        _column_ops(df1, apply1, "f1")

        if df2 is not None:
            def apply2(new_df):
                state["df2"] = new_df
                refresh()
            ui.label("File 2").classes("font-medium mt-3")
            _column_ops(df2, apply2, "f2")

        # ---- Section 3: Join ---------------------------------------------
        if df2 is not None:
            ui.separator().classes("my-3")
            ui.markdown("### 3. Join Files")
            with ui.row().classes("w-full gap-4"):
                left_key = ui.select(list(df1.columns), label="File 1 join key",
                                      value=list(df1.columns)[0]).props("outlined dense")
                right_key = ui.select(list(df2.columns), label="File 2 join key",
                                       value=list(df2.columns)[0]).props("outlined dense")
                join_type = ui.select(["Left", "Right", "Inner", "Outer"], value="Left",
                                       label="Join type").props("outlined dense")

            with ui.expansion("Key Normalization", value=True).classes("w-full"):
                ui.label("Applied to both key columns before matching.").classes(
                    "text-xs text-gray-500")
                with ui.row().classes("gap-4"):
                    norm_strip = ui.checkbox("Strip spaces", value=True)
                    norm_lower = ui.checkbox("Lowercase", value=True)
                    norm_prefix = ui.input("Add prefix")
                    norm_strip_prefix = ui.input("Strip prefix")

            use_fuzzy = ui.checkbox(
                f"Fuzzy matching {'(rapidfuzz available)' if RAPIDFUZZ_AVAILABLE else '(rapidfuzz NOT installed)'}",
            )
            use_fuzzy.enabled = RAPIDFUZZ_AVAILABLE
            fuzzy_threshold = ui.number("Fuzzy match threshold (%)", value=85, min=70, max=100, step=1)
            fuzzy_threshold.bind_visibility_from(use_fuzzy, "value")

            def do_join():
                try:
                    def norm(series):
                        s = series.astype(str)
                        if norm_strip_prefix.value:
                            s = s.apply(lambda v: v[len(norm_strip_prefix.value):].strip()
                                        if v.startswith(norm_strip_prefix.value) else v)
                        if norm_strip.value:
                            s = s.str.strip()
                        if norm_lower.value:
                            s = s.str.lower()
                        if norm_prefix.value:
                            s = norm_prefix.value + s
                        return s

                    d1 = df1.copy()
                    d2 = df2.copy()
                    d1["_join_key_"] = norm(d1[left_key.value])
                    d2["_join_key_"] = norm(d2[right_key.value])

                    if use_fuzzy.value and RAPIDFUZZ_AVAILABLE:
                        result = _fuzzy_join(d1, d2, "_join_key_", "_join_key_",
                                              join_type.value, fuzzy_threshold.value)
                    else:
                        result = d1.merge(d2, on="_join_key_", how=join_type.value.lower())
                    result = result.drop(columns=["_join_key_"], errors="ignore")
                    state["result"] = result
                    ui.notify(f"Join complete \u2014 {len(result)} rows, {len(result.columns)} columns.",
                               type="positive")
                    refresh()
                except Exception as e:
                    ui.notify(f"Join error: {e}", type="negative")
            ui.button("Run Join", on_click=do_join).props("unelevated color=primary").classes("mt-2")

        # ---- Section 4: Preview Result ------------------------------------
        ui.separator().classes("my-3")
        ui.markdown("### 4. Preview Result")
        result = state["result"] if state["result"] is not None else df1
        if state["result"] is None:
            ui.label("Showing File 1 \u2014 run a join to see the merged result.").classes(
                "text-xs text-gray-500")
        ui.label(f"{len(result)} rows \u00d7 {len(result.columns)} columns").classes(
            "text-sm text-gray-600")
        _df_table(result, n=50)

        with ui.expansion("Post-join Column Operations").classes("w-full mt-2"):
            x_cols = [c for c in result.columns if c.endswith("_x")]
            pairs = [(c, c[:-2] + "_y") for c in x_cols if c[:-2] + "_y" in result.columns]
            if pairs:
                ui.label(f"Found {len(pairs)} matching column pair(s): "
                         f"{', '.join(c[:-2] for c, _ in pairs)}").classes("text-xs text-gray-500")
                prefer = ui.radio(["File 1 (_x)", "File 2 (_y)"], value="File 1 (_x)").props("inline")

                def do_coalesce():
                    r = result.copy()
                    for col_x, col_y in pairs:
                        base = col_x[:-2]
                        primary, fallback = (col_x, col_y) if prefer.value == "File 1 (_x)" else (col_y, col_x)
                        pv = r[primary].astype(str).str.strip()
                        r[base] = r[primary].where(~r[primary].isna() & (pv != "") & (pv != "nan"), r[fallback])
                        r = r.drop(columns=[col_x, col_y], errors="ignore")
                    state["result"] = r
                    ui.notify(f"Merged {len(pairs)} column pair(s).", type="positive")
                    refresh()
                ui.button("Merge _x/_y columns", on_click=do_coalesce).props("unelevated").classes("mt-2")
            else:
                ui.label("No _x/_y column pairs found.").classes("text-xs text-gray-500")

            def apply_result(new_df):
                state["result"] = new_df
                refresh()
            _column_ops(result, apply_result, "res")

        # ---- Section 5: Export ---------------------------------------------
        ui.separator().classes("my-3")
        ui.markdown("### 5. Export")

        default_name = f"{Path(state['file1_name']).stem}_prepared.csv" if state["file1_name"] else "prepared_data.csv"

        with ui.row().classes("w-full items-center gap-2"):
            target_coll = ui.select(names, label="Target collection", with_input=True).props(
                "outlined dense").classes("w-64")
            mode = ui.radio(["Save only", "Save + Ingest"], value="Save only").props("inline")

        path_in = ui.input("Save to (full path)", placeholder="/path/to/output.csv").props(
            "outlined dense").classes("w-full")
        filename_in = ui.input("Filename", value=default_name).props("outlined dense").classes("w-64")

        def on_target_change():
            if not target_coll.value:
                return
            info = collection_path_info(target_coll.value)
            if info.get("path"):
                base = Path(info["path"])
                path_in.value = str(base / filename_in.value) if info.get("is_dir") or not base.suffix \
                    else str(base)
        target_coll.on_value_change(lambda: on_target_change())
        filename_in.on_value_change(lambda: on_target_change())

        def on_mode_change():
            if mode.value == "Save only":
                target_coll.value = None
        mode.on_value_change(lambda: on_mode_change())

        export_out = ui.column().classes("w-full mt-2")

        async def do_export():
            if not path_in.value:
                ui.notify("Enter a save path (or pick a target collection).", type="warning")
                return
            if mode.value == "Save + Ingest" and not target_coll.value:
                ui.notify("Pick a target collection to ingest into.", type="warning")
                return

            export_out.clear()
            with export_out:
                ui.spinner(size="lg")

            try:
                full_path = path_in.value
                Path(full_path).parent.mkdir(parents=True, exist_ok=True)
                result.to_csv(full_path, index=False)
            except Exception as e:
                export_out.clear()
                with export_out:
                    ui.label(f"Save error: {e}").classes("text-red-600")
                return

            if mode.value == "Save only":
                export_out.clear()
                with export_out:
                    ui.label(f"Saved to {full_path} ({len(result)} rows).").classes("text-green-700")
                return

            try:
                res = await run.io_bound(ingest_single_file, target_coll.value, full_path)
            except Exception as e:
                export_out.clear()
                with export_out:
                    ui.label(f"Saved to {full_path}, but ingestion failed: {e}").classes("text-red-600")
                return

            res = res or {}
            export_out.clear()
            with export_out:
                ui.label(f"Saved to {full_path} and ingested into '{target_coll.value}'.").classes(
                    "text-green-700 font-medium")
                with ui.row().classes("gap-8 mt-1"):
                    for key, lbl in _METRICS:
                        with ui.column().classes("items-center"):
                            ui.label(str(res.get(key, 0))).classes("text-xl font-bold")
                            ui.label(lbl).classes("text-xs text-gray-500")
                if res.get("_bg_error"):
                    ui.label(f"(background cross-link launch issue: {res['_bg_error']})").classes(
                        "text-xs text-red-500")

        ui.button("Save", on_click=do_export).props("unelevated color=primary").classes("mt-2")

        ui.separator().classes("my-3")

        def do_reset():
            state.update(df1=None, df2=None, result=None, file1_name=None, file2_name=None)
            refresh()
        ui.button("Reset All", on_click=do_reset).props("outline color=negative")

    refresh()