"""
diag_schema_guardrails.py — validates P1 schema guardrails through the REAL
llm_infer_schema (structured output + cross-role dedup + wide-file fallback),
now defaulting to qwen-14b. Read-only.
Run:  python3 diag_schema_guardrails.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from TABLES.table_parser import parse_table
from core.schema_inference import llm_infer_schema, load_roles_config
from core.paths import CONFIG_DIR

ROLES = load_roles_config(CONFIG_DIR / "structured_roles.json")

FILES = [
    ("kb_docs (clean)",      "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv"),
    ("bbg (clean)",          "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/BBG_FIELDS_TEST.xlsx"),
    ("Ticket_Details (dups)","/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/Ticket_Details_01192026.xlsx"),
    ("Project Timelines (wide -> should fall back)",
                             "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/Project Timelines 2.xlsx"),
]


def main():
    for label, path in FILES:
        print("=" * 64); print(label); print("=" * 64)
        try:
            rows = parse_table(path).get("rows", [])
            cols = [k for k in rows[0].keys() if k != "source_file"] if rows else []
            schema = llm_infer_schema(rows, ROLES)
            if schema is None:
                print("  -> llm_infer_schema returned None (heuristic fallback). "
                      "Expected for very wide sheets.\n")
                continue
            nonempty = {k: v for k, v in schema.items() if v}
            for r, c in nonempty.items():
                print(f"  {r:<20} {c}")
            mapped = [c for v in schema.values() for c in v]
            dupes = sorted({c for c in mapped if mapped.count(c) > 1})
            missing = [c for c in cols if c not in mapped]
            print(f"\n  duplicates across roles: {dupes}  (should be [])")
            print(f"  missing columns:         {missing}  (should be [])")
        except Exception as e:
            print("  FAILED:", repr(e))
        print()


if __name__ == "__main__":
    main()
