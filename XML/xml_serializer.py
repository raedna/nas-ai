from core.link_index import build_link_index
from core.normalizer import normalize_link_index

DEBUG = True


def xml_serializer(parsed, file_path, template_config, file_tags, collection_name):

    xml_serializer._expected_files = template_config.get("expected_files", 1)

    # buffer state across files
    if not hasattr(xml_serializer, "_all_rows"):
        xml_serializer._all_rows = {}

    if not hasattr(xml_serializer, "_schemas"):
        xml_serializer._schemas = {}

    if not hasattr(xml_serializer, "_files_seen"):
        xml_serializer._files_seen = set()

    xml_serializer._expected_files = template_config.get("expected_files", 1)

    if not hasattr(xml_serializer, "_finalized"):
        xml_serializer._finalized = False

    rows = parsed.get("rows", [])
    schema = parsed.get("schema")

    # 🔥 ensure schema exists — check PostgreSQL first, then LLM, then heuristic
    if not schema:
        from TABLES.schema_inference_table import infer_table_schema
        try:
            schema = infer_table_schema(
                rows,
                collection_name=collection_name,
                source_file=str(file_path)
            )
        except Exception as e:
            # One file's schema failure must NOT abort the whole XML batch:
            # finalize only fires when files_seen == expected (CODE-005
            # cross-file buffering), so count this file as seen before
            # re-raising — the file fails visibly, the rest still finalize.
            xml_serializer._files_seen.add(file_path.name)
            print(f"[XML] schema inference failed for {file_path.name} — file "
                  f"skipped; remaining XML files will still finalize: {e}")
            raise

    if isinstance(rows, dict):
        rows = [rows]

    if not isinstance(rows, list):
        print("❌ BAD ROW TYPE:", type(rows), file_path.name)
        rows = []

    xml_serializer._all_rows[file_path.name] = rows
    xml_serializer._schemas[file_path.name] = schema
    xml_serializer._files_seen.add(file_path.name)

    # never emit here; orchestrator will call finalize()
    return []

def merge_rows_by_version(all_rows, file_kind):
    """
    Merge rows across FIX version files for the same logical record.
    file_kind: 'fields' or 'enums' — determines merge key.
    Returns: dict {merged_key: merged_row}
    """
    import re

    def extract_version(filename):
        m = re.search(r'FIX(\d)(\d)', filename)
        if m:
            return int(m.group(1)) * 10 + int(m.group(2))  # FIX44 -> 44
        return 0  # unknown version, lowest priority

    grouped = {}  # key -> list of (version, row)

    for filename, rows in all_rows.items():
        version = extract_version(filename)
        if version == 0:
            continue  # not a versioned FIX file, skip merge for these rows

        for row in rows:
            if file_kind == "enums":
                key = (row.get("Tag"), row.get("Value"))
            else:
                key = row.get("Tag")

            if key is None:
                continue

            grouped.setdefault(key, []).append((version, row, filename))

    merged = {}
    for key, version_rows in grouped.items():
        version_rows.sort(key=lambda x: x[0])  # ascending version
        latest_version, latest_row, latest_file = version_rows[-1]

        merged_row = dict(latest_row)
        merged_row["_version_history"] = [
            {"version": v, "file": f, "data": r}
            for v, r, f in version_rows
        ]
        merged_row["_latest_version"] = latest_version
        merged[key] = merged_row

    return merged


def _correct_detail_table_keys(all_rows, schemas, min_overlap=0.8, sample=2000):
    """Batch-level schema correction for detail/child tables. Generic relational
    structure only — no domain vocabulary. See call site for rationale."""
    def _cols(src):
        cols = []
        for r in all_rows.get(src, [])[:50]:
            for k in r.keys():
                if k not in cols:
                    cols.append(k)
        return cols

    def _vals(src, col):
        out = set()
        for r in all_rows.get(src, [])[:sample]:
            v = str(r.get(col) or "").strip()
            if v and v.lower() not in ("none", "nan"):
                out.add(v)
        return out

    id_col = {src: (sch.get("identifier") or [None])[0]
              for src, sch in schemas.items() if sch}
    file_cols = {src: _cols(src) for src in all_rows.keys()}

    for src, sch in schemas.items():
        if not sch or src not in file_cols:
            continue
        own_id = id_col.get(src)
        # Own identifier referenced by any OTHER table? then this is a parent
        # table — leave it alone. A SIBLING table (same column is ITS own
        # identifier too, e.g. Enums_FIX42 vs Enums_FIX44 twins) is not a
        # referencing child and must not count.
        if own_id and any(
                own_id in file_cols.get(o, []) and id_col.get(o) != own_id
                for o in file_cols if o != src):
            continue
        for other_src, parent_key in sorted(id_col.items()):
            if other_src == src or not parent_key or parent_key == own_id:
                continue
            if parent_key not in file_cols[src]:
                continue
            child_vals = _vals(src, parent_key)
            parent_vals = _vals(other_src, parent_key)
            if not child_vals or not parent_vals:
                continue
            overlap = len(child_vals & parent_vals) / len(child_vals)
            if overlap < min_overlap:
                continue
            # Reassign: parent key becomes the identifier; remove it from any
            # other role. The displaced pick was the table's most name-like
            # near-unique column — it becomes primary_name when that role is
            # vacant (enum symbolic names, etc.), otherwise 'other'.
            for role, cols_ in list(sch.items()):
                if isinstance(cols_, list) and parent_key in cols_:
                    sch[role] = [c for c in cols_ if c != parent_key]
            sch["identifier"] = [parent_key]
            _dest = "other"
            if own_id:
                if not sch.get("primary_name"):
                    sch["primary_name"] = [own_id]
                    _dest = "primary_name"
                else:
                    sch.setdefault("other", [])
                    if own_id not in sch["other"]:
                        sch["other"].append(own_id)
            print(f"[SCHEMA XML] detail-table key correction: '{src}' identifier "
                  f"-> '{parent_key}' (parent: {other_src}, value overlap "
                  f"{overlap:.0%}); '{own_id}' -> {_dest}")
            break

    # Second pass — discriminator -> enum_value. In a parent-keyed detail
    # table the composite key is (parent_key, discriminator); the link index
    # can only attach detail rows to parent records via the enum_value role.
    # LLM runs place the discriminator unstably (enum_value / aliases / type
    # across runs of the same file). Structural assignment: a NAME-class
    # column (aliases) that is not the key, in a table whose identifier is
    # another table's key column, and which completes a near-unique composite
    # with that key, is the discriminator.
    id_col = {src: (sch.get("identifier") or [None])[0]
              for src, sch in schemas.items() if sch}  # refresh after pass 1
    for src, sch in schemas.items():
        if not sch or src not in file_cols or sch.get("enum_value"):
            continue
        own_id = id_col.get(src)
        if not own_id:
            continue
        # detail table: shares its key column with another table's identifier
        # AND the key repeats locally (not near-unique here).
        _shared = any(id_col.get(o) == own_id for o in schemas if o != src)
        if not _shared:
            continue
        rows_ = all_rows.get(src) or []
        own_vals = [str(r.get(own_id) or "").strip() for r in rows_[:sample]]
        own_vals = [v for v in own_vals if v]
        if not own_vals or len(set(own_vals)) / len(own_vals) >= 0.9:
            continue  # near-unique key -> parent-style table, not detail
        for cand in list(sch.get("aliases") or []):
            pairs = {(str(r.get(own_id) or ""), str(r.get(cand) or ""))
                     for r in rows_[:sample] if str(r.get(cand) or "").strip()}
            n_rows = sum(1 for r in rows_[:sample] if str(r.get(cand) or "").strip())
            if n_rows and len(pairs) / n_rows >= 0.9:
                sch["aliases"] = [c for c in sch["aliases"] if c != cand]
                sch["enum_value"] = [cand]
                print(f"[SCHEMA XML] discriminator correction: '{src}' "
                      f"enum_value -> '{cand}' (composite with '{own_id}' "
                      f"near-unique; was aliases)")
                break


def xml_finalize(file_path, collection_name, file_tags):

    if DEBUG:
        print("🔥 FINALIZE RUNNING")

    all_rows = getattr(xml_serializer, "_all_rows", {})
    schemas = getattr(xml_serializer, "_schemas", {})
    files_seen = getattr(xml_serializer, "_files_seen", set())

    # DEBUG
    print("FILES_SEEN:", files_seen)
    print("ALL_ROWS:", list(all_rows.keys()))

    # only finalize when ALL files parsed
    files_seen = getattr(xml_serializer, "_files_seen", set())
    expected = getattr(xml_serializer, "_expected_files", 1)

    # ONLY run on last file
    if len(files_seen) < expected:
        return []

    all_rows = getattr(xml_serializer, "_all_rows", {})
    schemas = getattr(xml_serializer, "_schemas", {})

    # Cross-file parent-key correction (generic, before merged copies are made):
    # a table that CONTAINS another table's identifier column — with actual
    # value overlap — while its own inferred identifier is referenced by no
    # other table, is a DETAIL table (composite key, e.g. enum rows per tag).
    # No column in such a table is near-unique, so single-file inference cannot
    # get this right; the batch-level structure can. Its identifier becomes the
    # shared parent key; the displaced pick is demoted to 'other'.
    _correct_detail_table_keys(all_rows, schemas)

    # Merge FIX version files by Tag before building link index
    fields_files = {k: v for k, v in all_rows.items() if k.lower().startswith("fields_")}
    enums_files = {k: v for k, v in all_rows.items() if k.lower().startswith("enums_")}
    other_files = {k: v for k, v in all_rows.items() if k not in fields_files and k not in enums_files}

    merged_fields = merge_rows_by_version(fields_files, "fields") if fields_files else {}
    merged_enums = merge_rows_by_version(enums_files, "enums") if enums_files else {}

    # Rebuild all_rows: merged fields/enums as single synthetic file, others untouched
    all_rows_merged = dict(other_files)
    if merged_fields:
        all_rows_merged["_merged_fields"] = list(merged_fields.values())
    if merged_enums:
        all_rows_merged["_merged_enums"] = list(merged_enums.values())

    # only run merge if we actually found versioned FIX files
    if merged_fields or merged_enums:
        all_rows = all_rows_merged

        def _richest_schema(file_keys):
            """Pick the most informative schema among version twins (most
            non-empty roles) — deterministic tie-break by key order. Copying
            blindly from the first file inherited whatever gaps that one
            LLM run happened to leave (e.g. an empty enum_value)."""
            best, best_n = {}, -1
            for k in file_keys:
                sch = schemas.get(k) or {}
                n = sum(1 for v in sch.values() if isinstance(v, list) and v)
                if n > best_n:
                    best, best_n = sch, n
            return best

        schemas = {**schemas,
                   "_merged_fields": _richest_schema(list(fields_files)),
                   "_merged_enums": _richest_schema(list(enums_files))}

    # build merged index across ALL files
    link_index = build_link_index(all_rows, schemas)

    from core.schema_inference import save_schema_to_db
    from pathlib import Path as _Path

    # 🔥 save all schemas to PostgreSQL
    for src_file, schema in schemas.items():
        if schema:
            source_stem = _Path(src_file).stem
            save_schema_to_db(schema, collection_name, source_stem)

    
    if DEBUG:
        print("=== DEBUG LINK INDEX ===")

        print("IDENTIFIER COUNT:", len(link_index.get("identifier", {})))

        print("\nCHECK TAG 22:")
        print(link_index.get("identifier", {}).get("22"))

        print("\nENUM COUNT FOR 22:")
        if link_index.get("identifier", {}).get("22"):
            print(len(link_index["identifier"]["22"].get("enum_values", [])))

        print("=== END DEBUG ===")
    normalized_docs = normalize_link_index(link_index)

    items = []

    for doc in normalized_docs:
        text_parts = []

        if doc.get("primary_name"):
            text_parts.append(doc["primary_name"])

        if doc.get("description"):
            text_parts.append(doc["description"])

        if doc.get("enum_values"):
            enum_texts = []
            for e in doc["enum_values"]:
                if isinstance(e, dict):
                    val = e.get("Value")
                    name = e.get("SymbolicName")

                    if val and name:
                        enum_texts.append(f"{val}={name}")
                    elif name:
                        enum_texts.append(name)
                    elif val:
                        enum_texts.append(val)
                else:
                    enum_texts.append(str(e))

            if enum_texts:
                text_parts.append(", ".join(enum_texts))

        text = "\n\n".join(text_parts).strip()
        if not text:
            continue

        source_files = doc.get("raw", {}).get("source_files", [])
        primary_source_file = source_files[0] if source_files else str(file_path)

        payload = {
            "identifier_field": doc.get("identifier_field"),
            "identifier_namespace": doc.get("identifier_namespace"),
            "identifier": doc.get("identifier"),
            "primary_name": doc.get("primary_name"),
            "primary_name_field": doc.get("primary_name_field"),
            "description": doc.get("description"),
            "enum_values": doc.get("enum_values"),
            "type": doc.get("type"),
            "type_field": doc.get("type_field"),
            "doc_type": doc.get("doc_type"),
            "source_file": primary_source_file,
            "source_files": source_files,
            "ingest_source": primary_source_file,
            "link_keys": doc.get("link_keys", []),
            "related_link_keys": doc.get("related_link_keys", []),
            **file_tags
        }

        items.append({
            "text": text,
            **payload
        })

    # TEMP DEBUG — BEFORE CLEANUP
    print("ALL_ROWS KEYS:", list(all_rows.keys()))
    for fname, rows in all_rows.items():
        print(
            fname,
            "sample keys:",
            rows[0].keys() if isinstance(rows, list) and rows else "EMPTY"
        )

    # Unconditional buffer reset — we only reach here when the full batch
    # finalized (the files_seen >= expected gate above). The old condition
    # compared len(all_rows) == len(files_seen), which is never true after the
    # version merge rebinds all_rows (12 merged keys vs 15 files seen), so
    # state leaked across runs: every file of the NEXT ingest passed the
    # "all files seen" check and triggered a full finalize (re-embedding and
    # re-upserting all chunks per file). CODE-005.
    xml_serializer._all_rows = {}
    xml_serializer._schemas = {}
    xml_serializer._files_seen = set()

    return items


xml_serializer.finalize = xml_finalize