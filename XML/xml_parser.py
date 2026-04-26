import xml.etree.ElementTree as ET
from pathlib import Path

DEBUG = True

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

def parse_xml_rows(file_path, template_config=None, row_tag=None):

    xml_file = Path(file_path)   # ✅ FIXED
    results = []

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # ----------------------------
        # AUTO-DETECT row_tag
        # ----------------------------
        if row_tag is None:
            candidates = {}

            for el in root.iter():
                tag = el.tag
                candidates[tag] = candidates.get(tag, 0) + 1

            candidates.pop(root.tag, None)

            if not candidates:
                raise ValueError("No candidate row tags found")

            row_tag = max(candidates, key=candidates.get)

            print(f"🔍 Auto-detected row_tag: {row_tag}")

        # ----------------------------
        # EXTRACT ROWS
        # ----------------------------
        elements = root.findall(f".//{row_tag}")

        for el in elements:

            row = {}

            # attributes
            for k, v in el.attrib.items():
                row[k.strip()] = str(v).strip()

            # child elements
            for child in el:
                if child.text and child.text.strip():
                    row[child.tag.strip()] = child.text.strip()

            if row:
                row["source_file"] = xml_file.name
                results.append(row)

        results = _apply_field_filters(results, template_config)

        if DEBUG and results:
            print("ROWS SAMPLE:", results[0])

        return {
            "rows": results,
            "schema": None
        }

    except Exception as e:
        print(f"[ERROR] parse_xml_rows failed: {xml_file} → {e}")
        return {
            "rows": results,
            "schema": None
        }
                