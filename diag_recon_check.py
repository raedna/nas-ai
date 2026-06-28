"""
diag_recon_check.py — is recon using the corrected schema after re-ingest? Read-only.
Run:  python3 diag_recon_check.py
"""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

s = fetchall("SELECT schema_json FROM schemas WHERE collection_name='recon_assist_file'")
sch = s[0]["schema_json"] if s else None
if isinstance(sch, str):
    sch = json.loads(sch)
print("saved schema identifier  :", (sch or {}).get("identifier") if sch else "NO SCHEMA ROW")
print("saved schema primary_name:", (sch or {}).get("primary_name") if sch else None)

g = fetchall("""SELECT payload->>'identifier' AS i, payload->>'description_fields' AS df
                FROM chunks WHERE collection_name='recon_assist_file'
                AND payload->>'identifier' ILIKE 'gsact.txt' LIMIT 1""")
print("\ngsact.txt found by identifier:", bool(g))
if g:
    print("  description_fields:", g[0]["df"])

print("\nsample recon identifiers:",
      [r["i"] for r in fetchall("""SELECT DISTINCT payload->>'identifier' AS i
            FROM chunks WHERE collection_name='recon_assist_file'
            AND payload->>'identifier' IS NOT NULL ORDER BY i LIMIT 8""")])
print("distinct identifier count:",
      fetchall("""SELECT COUNT(DISTINCT payload->>'identifier') AS n
            FROM chunks WHERE collection_name='recon_assist_file'""")[0]["n"])
