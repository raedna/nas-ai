"""
diag_schema_tags.py  — read-only schema probe, changes nothing.
Compares how the KB CSV schema is inferred 3 ways so we can decide on a `tags` role.
Run:  python3 diag_schema_tags.py
"""
import sys, json, csv
sys.path.insert(0, '/Users/raednasr/RaedsMacM1/nas-ai/claude')

from core.paths import CONFIG_DIR
from core.schema_inference import infer_schema, load_roles_config

CSV_PATH = '/Users/raednasr/RaedsMacM1/nas-ai/table_test_input/KB_Articles RN_20260225.csv'


def load_rows():
    with open(CSV_PATH, newline='', encoding='utf-8', errors='replace') as f:
        return list(csv.DictReader(f))


def llm_infer_with_roles(rows, roles_config, extra_role_lines=""):
    """Copy of llm_infer_schema, but lets us inject extra role descriptions
    (e.g. a `tags` role) WITHOUT touching the real code — test only."""
    from core.local_llm_client import call_local_llm_json

    columns = []
    for row in rows:
        if isinstance(row, dict):
            for k in row.keys():
                if k != "source_file" and k not in columns:
                    columns.append(k)
    if not columns:
        return None

    scored = sorted(rows, key=lambda r: sum(
        1 for v in r.values() if str(v or "").strip() not in ("", "None", "nan")),
        reverse=True)
    samples = [{k: str(v)[:80] for k, v in r.items() if k in columns} for r in scored[:5]]
    available_roles = list(roles_config.keys()) + ["other"]

    system_prompt = (
        "You are a data schema classifier. Given column names and sample values from a CSV file, "
        "map each column to exactly one of these roles:\n\n"
        "- identifier: the primary unique key (e.g. ID, code, tag number, catalog number)\n"
        "- primary_name: the human-readable name or title\n"
        "- aliases: alternative names or secondary IDs\n"
        "- description: longer descriptive text, notes, or definitions\n"
        "- type: category, classification, or data type\n"
        + extra_role_lines +
        "- enum_value: allowed values or codes for a field\n"
        "- enum_name: labels for enum values\n"
        "- reference_identifier: foreign key referencing another table\n"
        "- other: dates, boolean flags, or anything else\n\n"
        "Rules:\n"
        "- Only ONE column should be identifier\n"
        "- Only ONE column should be primary_name\n"
        "- Every column must appear exactly once\n"
        "- Return only JSON: {\"role_name\": [\"col1\"], ...}"
    )
    user_prompt = (
        f"Columns: {columns}\n\n"
        f"Sample values (5 most populated rows):\n{json.dumps(samples, indent=2)}\n\n"
        f"Map each column to one of: {available_roles}\n"
    )
    return call_local_llm_json(system_prompt, user_prompt, temperature=0.0)


def main():
    rows = load_rows()
    roles = load_roles_config(CONFIG_DIR / "structured_roles.json")
    print(f"rows={len(rows)}  columns={list(rows[0].keys())}\n")

    print("=" * 60)
    print("1. HEURISTIC schema (current default for kb_docs)")
    print("=" * 60)
    print(json.dumps(infer_schema(rows, roles), indent=2))

    print("\n" + "=" * 60)
    print("2. LLM schema — CURRENT roles (no tags role)")
    print("   -> shows where 'kbtags' lands today")
    print("=" * 60)
    print(json.dumps(llm_infer_with_roles(rows, roles), indent=2))

    print("\n" + "=" * 60)
    print("3. LLM schema — PROPOSED, with generic 'tags' role")
    print("=" * 60)
    roles_with_tags = dict(roles)
    roles_with_tags["tags"] = []  # name only; description supplied to the prompt
    tags_line = ("- tags: a column of comma/semicolon-separated keywords or "
                 "category labels describing each row (e.g. 'email,Office365,VPN')\n")
    print(json.dumps(llm_infer_with_roles(rows, roles_with_tags, tags_line), indent=2))


if __name__ == "__main__":
    main()
