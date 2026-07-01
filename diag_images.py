import sys, os
sys.path.insert(0, "/Users/raednasr/RaedsMacM1/nas-ai/claude")
from core.retrieval.router import run_query_with_method

r = run_query_with_method("obsidian", "how to check sFTP for gsact.txt")
p = r.get("answer_payload") or {}
paths = p.get("embedded_image_paths") or []
print("method            :", r.get("method"))
print("has answer_payload:", bool(r.get("answer_payload")))
print("n images          :", len(paths))
print("[image: in result :", "[image:" in str(r.get("result")))
if paths:
    print("first path exists :", os.path.exists(paths[0]))
    print("first path        :", paths[0])
