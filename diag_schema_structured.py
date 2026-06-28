"""
diag_schema_structured.py — tests schema inference with response_format (structured
output) to force valid JSON in the exact {role: [columns]} shape. Read-only.
Run:  python3 diag_schema_structured.py
"""
import sys, json, csv, requests
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')
from core.local_llm_client import get_local_llm_config

CSV_PATH = '/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv'
MODELS = ["meta-llama-3.1-8b-instruct", "qwen2.5-14b-instruct-1m"]
ROLES = ["identifier", "primary_name", "aliases", "description", "type", "tags",
         "enum_value", "enum_name", "reference_identifier", "other"]


def build_prompt():
    with open(CSV_PATH, newline='', encoding='utf-8', errors='replace') as f:
        rows = list(csv.DictReader(f))
    cols = [k for k in rows[0].keys() if k != "source_file"]
    scored = sorted(rows, key=lambda r: sum(
        1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")), reverse=True)
    samples = [{k: str(v)[:80] for k, v in r.items() if k in cols} for r in scored[:5]]
    system = (
        "You are a data schema classifier. Map each column to exactly one role.\n"
        "- identifier: primary unique key\n- primary_name: human-readable name/title\n"
        "- aliases: alternative names/secondary ids\n- description: longer descriptive text/notes\n"
        "- type: category or data type\n"
        "- tags: column of comma/semicolon-separated keywords or category labels per row\n"
        "- enum_value / enum_name / reference_identifier as usual\n- other: anything else\n"
        "Rules: exactly one identifier, exactly one primary_name, every column appears once.")
    user = f"Columns: {cols}\n\nSample rows:\n{json.dumps(samples, indent=2)}"
    return cols, system, user


def schema_response_format():
    props = {role: {"type": "array", "items": {"type": "string"}} for role in ROLES}
    return {"type": "json_schema", "json_schema": {
        "name": "column_roles", "strict": True,
        "schema": {"type": "object", "properties": props,
                   "required": ROLES, "additionalProperties": False}}}


def main():
    cfg = get_local_llm_config()
    url = cfg["base_url"].rstrip("/") + "/v1/chat/completions"
    cols, system, user = build_prompt()

    for model in MODELS:
        print("=" * 64); print("MODEL:", model); print("=" * 64)
        try:
            r = requests.post(url, timeout=120, json={
                "model": model, "temperature": 0.0, "stream": False,
                "response_format": schema_response_format(),
                "messages": [{"role": "system", "content": system},
                             {"role": "user", "content": user}]})
            content = r.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)              # should never fail with strict schema
            print(json.dumps(parsed, indent=2))
            # sanity: every column mapped exactly once?
            mapped = [c for cols_ in parsed.values() for c in cols_]
            dupes = {c for c in mapped if mapped.count(c) > 1}
            missing = [c for c in cols if c not in mapped]
            print(f"\n  tags -> {parsed.get('tags')}")
            print(f"  missing cols: {missing}   duplicate cols: {sorted(dupes)}")
        except Exception as e:
            print("FAILED:", repr(e))
        print()


if __name__ == "__main__":
    main()
