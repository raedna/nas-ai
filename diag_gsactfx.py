"""diag_gsactfx.py — dump gsact_fx.txt's fields so we can fix RECON-09/10. Read-only."""
import sys, json
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.db import fetchall

g = fetchall("""SELECT payload->>'identifier' AS i, payload->>'primary_name' AS n,
                       payload->>'description_fields' AS df
                FROM chunks WHERE collection_name='recon_assist_file'
                AND payload->>'identifier' ILIKE %s LIMIT 1""", ('gsact_fx.txt',))
if not g:
    print("gsact_fx.txt NOT FOUND. Similar ids:",
          [r['i'] for r in fetchall("""SELECT DISTINCT payload->>'identifier' AS i
                FROM chunks WHERE collection_name='recon_assist_file'
                AND payload->>'identifier' ILIKE %s ORDER BY i""", ('%gsact%',))])
else:
    print("identifier:", g[0]['i'], "| primary_name:", g[0]['n'])
    df = g[0]['df']
    print("description_fields:", json.dumps(json.loads(df) if isinstance(df, str) else df, indent=2))
