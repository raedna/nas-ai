"""
diag_recon_schema.py — re-infer a collection's schema with the new cardinality
signals and show the result (does NOT save). Read-only.
Run:  python3 diag_recon_schema.py [collection]   (default: recon_assist_file)
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from TABLES.table_parser import parse_table
from core.schema_inference import llm_infer_schema, load_roles_config
from core.paths import CONFIG_DIR

COL = sys.argv[1] if len(sys.argv) > 1 else "recon_assist_file"

cfg = json.load(open("config/collections.json")).get(COL, {})
path = cfg.get("path") or (cfg.get("paths") or [None])[0]
print(f"collection: {COL}\nsource: {path}\n")
if not path:
    print("No path in collections.json for this collection."); sys.exit(1)

rows = parse_table(path).get("rows", [])
roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
schema = llm_infer_schema(rows, roles)

if not schema:
    print("llm_infer_schema returned None (heuristic fallback).")
    sys.exit(0)

print("Inferred schema (non-empty roles):")
print(json.dumps({k: v for k, v in schema.items() if v}, indent=2))
print(f"\n  identifier   -> {schema.get('identifier')}")
print(f"  primary_name -> {schema.get('primary_name')}")
print(f"  type         -> {schema.get('type')}")
print("\nExpected for recon: identifier=[Moore file name], "
      "primary_name=[a job-name column], type=[Prime Broker]")
