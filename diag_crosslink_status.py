"""
diag_crosslink_status.py — what's actually in cross_links + background_tasks. Read-only.
Run:  python3 diag_crosslink_status.py
"""
import sys
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

print("=== background_tasks (recent) ===")
try:
    for r in fetchall("""SELECT collection, task_name, status,
                                started_at, finished_at
                         FROM background_tasks ORDER BY id DESC LIMIT 15"""):
        print(f"  {r['collection']:<18} {r['status']:<10} "
              f"start={r['started_at']} end={r['finished_at']}")
except Exception as e:
    print("  (could not read background_tasks:", e, ")")

print("\n=== cross_links: total ===")
tot = fetchall("SELECT COUNT(*) AS n FROM cross_links")[0]['n']
print(f"  {tot} rows")

print("\n=== by status + match_type ===")
for r in fetchall("""SELECT status, match_type, COUNT(*) AS n
                     FROM cross_links GROUP BY status, match_type
                     ORDER BY status, n DESC"""):
    print(f"  {r['status']:<14} {r['match_type']:<18} {r['n']}")

print("\n=== obsidian as SOURCE (outgoing) ===")
for r in fetchall("""SELECT status, match_type, target_collection, COUNT(*) AS n
                     FROM cross_links WHERE source_collection='obsidian'
                     GROUP BY status, match_type, target_collection
                     ORDER BY n DESC"""):
    print(f"  {r['status']:<14} {r['match_type']:<12} -> {r['target_collection']:<18} {r['n']}")

print("\n=== obsidian as TARGET (incoming) ===")
for r in fetchall("""SELECT status, match_type, source_collection, COUNT(*) AS n
                     FROM cross_links WHERE target_collection='obsidian'
                     GROUP BY status, match_type, source_collection
                     ORDER BY n DESC"""):
    print(f"  {r['status']:<14} {r['match_type']:<12} <- {r['source_collection']:<18} {r['n']}")
