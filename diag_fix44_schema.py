"""
diag_fix44_schema.py — standalone reproduction of the Datatypes_FIX44 schema
failure (DATA-01 / MODEL-01 verification). Runs the exact ingest path:
parse_xml_rows -> llm_infer_schema, with every print visible.

Run on the Mac:  python3 diag_fix44_schema.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")

from pathlib import Path

SRC = Path("/Volumes/raedsync/Documents/OmniVista/FIX Dict")
matches = sorted(SRC.rglob("Datatypes_FIX44*"))
if not matches:
    print("file not found under", SRC)
    sys.exit(1)
fp = matches[0]
print("file:", fp)

from XML.xml_parser import parse_xml_rows
parsed = parse_xml_rows(str(fp))
rows = parsed.get("rows", []) if isinstance(parsed, dict) else parsed
print(f"parsed rows: {len(rows)}")
if rows:
    cols = [k for k in rows[0].keys() if k != "source_file"]
    print("columns:", cols)
    print("sample row:", {k: str(v)[:60] for k, v in rows[0].items()})

from core.schema_inference import llm_infer_schema, load_roles_config
from core.paths import CONFIG_DIR
roles = load_roles_config(CONFIG_DIR / "structured_roles.json")

print("\n--- llm_infer_schema (watch for [SCHEMA LLM] lines) ---")
schema = llm_infer_schema(rows, roles)
print("\nRESULT:", schema)
