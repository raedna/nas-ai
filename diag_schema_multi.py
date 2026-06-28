"""
diag_schema_multi.py — runs structured-output LLM schema inference (qwen-14b) on
several real table files via the REAL parser (parse_table). Read-only, writes nothing.
Shows the role mapping, tags detection, and completeness per file so we can confirm
always-run LLM inference won't regress the structured collections.

Run:  python3 diag_schema_multi.py
Edit FILES below to add/remove paths.
"""
import sys, json, requests
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from TABLES.table_parser import parse_table
from core.local_llm_client import get_local_llm_config

MODEL = "qwen2.5-14b-instruct-1m"

FILES = [
    ("kb_docs (CSV, reference)",
     "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv"),
    ("bbg_test (xlsx)",
     "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/BBG_FIELDS_TEST.xlsx"),
    ("recon_assist_file (xlsx)",
     "/Volumes/raedsync/Documents/OmniVista/Support Desk/RECON/RECON_Moore-PB Mapping_100225.xlsx"),
    ("Ticket_Details (xlsx)",
     "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/Ticket_Details_01192026.xlsx"),
    ("Project Timelines 2 (xlsx)",
     "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/Project Timelines 2.xlsx"),
]

ROLES = ["identifier", "primary_name", "aliases", "description", "type", "tags",
         "enum_value", "enum_name", "reference_identifier", "other"]


def response_format():
    props = {r: {"type": "array", "items": {"type": "string"}} for r in ROLES}
    return {"type": "json_schema", "json_schema": {
        "name": "column_roles", "strict": True,
        "schema": {"type": "object", "properties": props,
                   "required": ROLES, "additionalProperties": False}}}


def infer(rows):
    cfg = get_local_llm_config()
    url = cfg["base_url"].rstrip("/") + "/v1/chat/completions"
    cols = [k for k in rows[0].keys() if k != "source_file"]
    scored = sorted(rows, key=lambda r: sum(
        1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")), reverse=True)
    samples = [{k: str(v)[:80] for k, v in r.items() if k in cols} for r in scored[:5]]
    system = (
        "You are a data schema classifier. Map each column to exactly one role.\n"
        "- identifier: primary unique key\n- primary_name: human-readable name/title\n"
        "- aliases: alternative names/secondary ids\n- description: longer descriptive text/notes\n"
        "- type: category or data type\n"
        "- tags: column of comma/semicolon-separated keywords or category labels per row\n"
        "- enum_value / enum_name / reference_identifier as usual\n- other: anything else\n"
        "Rules: exactly one identifier, exactly one primary_name, every column appears once.")
    user = f"Columns: {cols}\n\nSample rows:\n{json.dumps(samples, indent=2)}"
    r = requests.post(url, timeout=180, json={
        "model": MODEL, "temperature": 0.0, "stream": False,
        "response_format": response_format(),
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": user}]})
    return cols, json.loads(r.json()["choices"][0]["message"]["content"])


def main():
    print("MODEL:", MODEL, "\n")
    for label, path in FILES:
        print("=" * 66); print(label); print(path); print("=" * 66)
        try:
            parsed = parse_table(path)
            rows = parsed.get("rows", [])
            if not rows:
                print("  no rows parsed — skipping\n"); continue
            cols, schema = infer(rows)
            schema = {k: v for k, v in schema.items() if v}  # drop empty roles for readability
            print(json.dumps(schema, indent=2))
            mapped = [c for v in schema.values() for c in v]
            missing = [c for c in cols if c not in mapped]
            dupes = sorted({c for c in mapped if mapped.count(c) > 1})
            print(f"\n  tags -> {schema.get('tags', [])}")
            print(f"  missing: {missing}   duplicates: {dupes}")
        except Exception as e:
            print("  FAILED:", repr(e))
        print()


if __name__ == "__main__":
    main()
