"""
diag_chunking_test.py — verifies P0 entity-row chunking on the real KB CSV, offline
(no DB, no LLM). Uses the actual parser + serializer path.
Run:  python3 diag_chunking_test.py
"""
import sys, statistics
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from TABLES.table_parser import parse_table
from TABLES.schema_inference_table import infer_schema
from core.schema_inference import load_roles_config
from core.paths import CONFIG_DIR
from TABLES.table_detector import detect_table_type
from TABLES.table_serializer import process_entity_row_table

CSV = "/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv"
CAP = 2500  # measured embed truncation point


def main():
    rows = parse_table(CSV)["rows"]
    roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
    schema = infer_schema(rows, roles)
    ttype = detect_table_type(rows, schema)
    print(f"rows={len(rows)}  table_type={ttype}")

    docs = process_entity_row_table(rows, schema, "KB_Articles RN_20260225.csv")
    print(f"articles in -> chunks out: {len(rows)} -> {len(docs)}\n")

    # group chunks by identifier (the article key)
    by_id = {}
    for d in docs:
        by_id.setdefault(d.get("identifier"), []).append(d)
    split = {k: v for k, v in by_id.items() if len(v) > 1}

    lens = [len(d.get("text") or "") for d in docs]
    over = [n for n in lens if n > CAP]
    print(f"articles that split into >1 chunk: {len(split)} / {len(by_id)}")
    print(f"chunk char length: median={int(statistics.median(lens))} max={max(lens)}")
    print(f"chunks still OVER {CAP} chars: {len(over)}   (should be 0)")

    # show the biggest splits
    print("\ntop 5 most-chunked articles:")
    for k, v in sorted(split.items(), key=lambda x: -len(x[1]))[:5]:
        title = (v[0].get("primary_name") or "")[:50]
        idxs = sorted(d.get("chunk_index") for d in v)
        print(f"  id={k:<5} chunks={len(v)}  index={idxs}  title={title!r}")

    # integrity checks
    ok_ids = all(len({d.get("identifier") for d in v}) == 1 for v in split.values())
    ok_titles = all(d.get("text", "").lstrip().startswith((d.get("primary_name") or "x")[:20])
                    for v in split.values() for d in v)
    print(f"\nall chunks of an article share one identifier? {ok_ids}")
    print(f"every chunk starts with the article title?       {ok_titles}")
    print(f"chunk_total set correctly?                       "
          f"{all(d.get('chunk_total') == len(v) for v in split.values() for d in v)}")


if __name__ == "__main__":
    main()
