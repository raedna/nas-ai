"""
diag_ner.py — preview CL-03 identifier-mention cross-links (read-only, saves nothing).
Run:  python3 diag_ner.py [source_collection]   (default: obsidian)
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.ner_cross_linker import discover_identifier_mentions, build_gazetteer
from core.query_helpers import load_doc_query_hints
from core.db import fetchall

SRC = sys.argv[1] if len(sys.argv) > 1 else "obsidian"

targets = [r['c'] for r in fetchall(
    "SELECT DISTINCT collection_name AS c FROM chunks WHERE collection_name != %s", (SRC,))]
generic = {t.lower() for t in load_doc_query_hints().get('generic_terms', [])}
gaz = build_gazetteer(targets, generic)
print(f"source: {SRC}")
print(f"gazetteer terms: {len(gaz)} (sample: {[g['term'] for g in list(gaz.values())[:8]]})\n")

cands = discover_identifier_mentions(SRC)
print(f"candidates: {len(cands)}\n")

by_target = {}
for c in cands:
    by_target.setdefault(c['target_collection'], []).append(c)
for tcol, items in sorted(by_target.items(), key=lambda x: -len(x[1])):
    print(f"=== -> {tcol}: {len(items)} links ===")
    for c in items[:15]:
        print(f"  {c['source_identifier'][:45]:45}  ->  {c['target_identifier']}  "
              f"(conf {c['confidence']})")
    if len(items) > 15:
        print(f"  ...and {len(items) - 15} more")
