"""
diag_verify_concepts.py — rebuild + inspect concept vectors (CV-01..04) and re-run
cross-link discovery (CL-01/02). Writes concept_vectors + cross_links to the DB.
Run:  python3 diag_verify_concepts.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.concept_vector_builder import build_concept_vectors
from core.db import fetchall

# Rebuild across a mix: entity_row (kb_docs -> CV-02/03 tags/topics),
# chunked docs (docx/pdf -> CV-01 section_heading), single-source_file (bbg -> CV-04 LLM label).
COLLECTIONS = ["kb_docs", "docx_test", "pdf_test", "obsidian", "bbg_fields"]

print("=== rebuilding concept vectors ===")
for c in COLLECTIONS:
    try:
        n = build_concept_vectors(c)
        print(f"  {c:<16} -> {n} concept vectors")
    except Exception as e:
        print(f"  {c:<16} FAILED: {e!r}")

print("\n=== concept_vectors by collection + group_field ===")
for r in fetchall("""SELECT collection, group_field, COUNT(*) AS n
                     FROM concept_vectors GROUP BY collection, group_field
                     ORDER BY collection"""):
    print(f"  {r['collection']:<16} group_field={r['group_field']:<16} count={r['n']}")

print("\n=== sample group_values (should be tags/topics, not filenames) ===")
for c in COLLECTIONS:
    gv = fetchall("""SELECT DISTINCT group_value FROM concept_vectors
                     WHERE collection=%s LIMIT 12""", (c,))
    vals = [r['group_value'] for r in gv]
    if vals:
        print(f"  {c}:")
        for v in vals:
            print(f"      {v}")

# CL-01/02: re-run cross-link discovery for a source collection and summarize
print("\n=== cross-link discovery (CL-01/02) sample ===")
try:
    from core.cross_link_discoverer import discover_cross_links
    cands = discover_cross_links("obsidian")
    mtypes = {}
    for c in cands:
        mtypes[c["match_type"]] = mtypes.get(c["match_type"], 0) + 1
    print(f"  obsidian candidates: {len(cands)}  by type: {mtypes}")
    for c in [x for x in cands if x["match_type"] == "mention"][:5]:
        print(f"    mention: {c['source_identifier']} -> "
              f"{c['target_collection']}:{c['target_identifier']} (conf {c['confidence']})")
except Exception as e:
    print(f"  discovery FAILED: {e!r}")
