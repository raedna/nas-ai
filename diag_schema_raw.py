"""
diag_schema_raw.py — shows RAW model output for the proposed tags-role schema
prompt, across models, so we see why JSON parsing failed and which model is reliable.
Read-only. Run:  python3 diag_schema_raw.py
"""
import sys, json, csv, requests
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.paths import CONFIG_DIR
from core.schema_inference import load_roles_config
from core.local_llm_client import get_local_llm_config, _parse_json_response

CSV_PATH = '/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv'
MODELS = ["meta-llama-3.1-8b-instruct", "qwen2.5-14b-instruct-1m"]


def build_prompt():
    with open(CSV_PATH, newline='', encoding='utf-8', errors='replace') as f:
        rows = list(csv.DictReader(f))
    cols = [k for k in rows[0].keys() if k != "source_file"]
    scored = sorted(rows, key=lambda r: sum(
        1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")), reverse=True)
    samples = [{k: str(v)[:80] for k, v in r.items() if k in cols} for r in scored[:5]]
    roles = list(load_roles_config(CONFIG_DIR / "structured_roles.json").keys()) + ["tags", "other"]
    system = (
        "You are a data schema classifier. Map each column to exactly one role:\n"
        "- identifier: primary unique key\n- primary_name: human-readable name/title\n"
        "- aliases: alternative names\n- description: longer descriptive text/notes\n"
        "- type: category or data type\n"
        "- tags: a column of comma/semicolon-separated keywords or category labels per row "
        "(e.g. 'email,Office365,VPN')\n"
        "- enum_value/enum_name/reference_identifier as usual\n- other: anything else\n"
        "Every column appears exactly once. Return ONLY JSON: {\"role\":[\"col\"]}")
    user = (f"Columns: {cols}\n\nSamples:\n{json.dumps(samples, indent=2)}\n\n"
            f"Map each column to one of: {roles}")
    return system, user


def main():
    cfg = get_local_llm_config()
    url = cfg["base_url"].rstrip("/") + "/v1/chat/completions"
    system, user = build_prompt()

    for model in MODELS:
        print("=" * 64)
        print("MODEL:", model)
        print("=" * 64)
        try:
            r = requests.post(url, json={"model": model, "temperature": 0.0, "stream": False,
                "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}]}, timeout=120)
            content = r.json()["choices"][0]["message"]["content"]
            print("--- RAW content ---")
            print(content[:1500])
            print("--- parsed ---")
            try:
                print(json.dumps(_parse_json_response(content), indent=2))
            except Exception as e:
                print("PARSE FAILED:", repr(e))
        except Exception as e:
            print("REQUEST FAILED:", repr(e))
        print()


if __name__ == "__main__":
    main()
