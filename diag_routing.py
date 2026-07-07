"""
Chapter-2 diagnostics: twin overlap + concept-centroid routing dry-run.

1. kb_docs vs obsidian twin check: how much content is actually duplicated
   (primary_name/title overlap) — decides twin policy (route both vs dedupe).
2. Centroid routing: for the acceptance questions, rank collections by best
   concept-vector centroid similarity — would deterministic routing beat the
   Tier 2 LLM?

Usage: python diag_routing.py
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import fetchall

print("=" * 70)
print("1. kb_docs <-> obsidian twin overlap (normalized title match)")
rows = fetchall("""
    WITH kb AS (SELECT DISTINCT lower(regexp_replace(primary_name, '[^a-zA-Z0-9]', '', 'g')) AS t
                FROM chunks WHERE collection_name='kb_docs' AND primary_name IS NOT NULL),
         ob AS (SELECT DISTINCT lower(regexp_replace(
                    regexp_replace(source_file, '\\.md$', ''), '[^a-zA-Z0-9]', '', 'g')) AS t
                FROM chunks WHERE collection_name='obsidian' AND source_file IS NOT NULL)
    SELECT (SELECT count(*) FROM kb) AS kb_titles,
           (SELECT count(*) FROM ob) AS ob_titles,
           (SELECT count(*) FROM kb JOIN ob USING (t)) AS shared
""", ())
r = rows[0]
print(f"   kb titles={r['kb_titles']}  obsidian notes={r['ob_titles']}  shared={r['shared']}")

print()
print("=" * 70)
print("2. Concept-centroid routing dry-run (top 3 collections per question)")
from core.embedder import embed_text

QUESTIONS = [
    ("PP-01", "that goldman activity file, whats the tidal job for it", "recon_assist_file"),
    ("PP-03", "brodcaster acting up agian", "kb_docs/obsidian"),
    ("PP-06", "goldman prio pull job", "recon_assist_file"),
    ("XC-02", "what tidal job pulls jpm files and how do I check if it ran", "recon+obsidian"),
    ("XC-05", "bad dates alert for citi, which file and what steps", "recon+kb_docs"),
    ("DL-07", "ARD_OPERATING_EXP_PER_ASM_ASK", "bbg_fields"),
    ("DL-08", "message broadcaster down", "kb_docs"),
    ("PR-03", "how to rerun a tidal recon job", "obsidian"),
]

for qid, q, expected in QUESTIONS:
    v = str(embed_text(q))
    rows = fetchall("""
        SELECT collection, MAX(1 - (centroid <=> %s::vector)) AS best_sim
        FROM concept_vectors
        GROUP BY collection
        ORDER BY best_sim DESC
        LIMIT 3
    """, (v,))
    ranked = " | ".join(f"{r['collection']} {r['best_sim']:.3f}" for r in rows)
    # also show WHICH cluster won in the top collection
    top = fetchall("""
        SELECT collection, group_value, 1 - (centroid <=> %s::vector) AS sim
        FROM concept_vectors ORDER BY sim DESC LIMIT 2
    """, (v,))
    clusters = " ; ".join(f"{t['collection']}/{t['group_value']} {t['sim']:.3f}" for t in top)
    print(f"\n  {qid} (expect {expected}):")
    print(f"    collections: {ranked}")
    print(f"    top clusters: {clusters}")
