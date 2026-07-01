"""
rebuild_cross_links.py — clear cross_links and rebuild under the new confidence
model (exact-id incl aliases 1.0, structured field-reference 0.95, corroborated
trigram pending, mentions dropped). Run on the Mac:  python3 rebuild_cross_links.py
"""
import sys
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")
from core.db import execute, fetchall
from core.cross_link_discoverer import discover_cross_links
from core.cross_link_store import save_cross_link_candidates, ensure_cross_links_table

SKIP_COLLECTIONS = {"astro_catalog"}

ensure_cross_links_table()
execute("DELETE FROM cross_links", ())
print("cleared cross_links\n")

cols = [r["collection_name"] for r in
        fetchall("SELECT DISTINCT collection_name FROM chunks", ())]
for c in cols:
    if c in SKIP_COLLECTIONS:
        print(f"    skipping {c} ...")
        continue
        
    print(f"  scanning {c} ...", flush=True)
    cand = discover_cross_links(c)
    save_cross_link_candidates(cand)
    print(f"  {c:24s} candidates={len(cand)}", flush=True)

print("\n=== gazetteer NER: identifier mentions in text ===", flush=True)
from core.ner_cross_linker import run_identifier_ner
for c in cols:
    run_identifier_ner(c)

print("\n=== resulting cross_links by status / match_type ===")
for r in fetchall("""SELECT status, match_type, COUNT(*) n FROM cross_links
                     GROUP BY status, match_type ORDER BY status, n DESC""", ()):
    print(f"  {r['status']:16s} {r['match_type']:18s} {r['n']}")
