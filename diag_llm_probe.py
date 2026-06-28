"""
diag_llm_probe.py — checks the local LLM endpoint, prints the REAL error.
Run:  python3 diag_llm_probe.py
"""
import sys, json, requests
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.local_llm_client import get_local_llm_config

cfg = get_local_llm_config()
base = cfg["base_url"].rstrip("/")
print("config:", cfg)

# 1. what models does the server report?
try:
    r = requests.get(base + "/v1/models", timeout=10)
    print("\n/v1/models ->", r.status_code)
    print(json.dumps(r.json(), indent=2)[:1500])
except Exception as e:
    print("\n/v1/models FAILED:", repr(e))

# 2. tiny completion with the configured model — show raw response + errors
try:
    payload = {
        "model": cfg["model"],
        "messages": [{"role": "user", "content": "Reply with the single word: ok"}],
        "temperature": 0.0,
        "stream": False,
    }
    r = requests.post(base + "/v1/chat/completions", json=payload, timeout=cfg["timeout"])
    print("\n/v1/chat/completions ->", r.status_code)
    print("raw body (first 800 chars):")
    print(r.text[:800])
except Exception as e:
    print("\n/v1/chat/completions FAILED:", repr(e))
