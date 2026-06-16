"""
Test what the LLM produces when asked to also extract role and target,
replacing field_maps.json keyword matching.
Run: python tests/test_llm_intent.py
"""
import sys, json
sys.path.insert(0, ".")
from core.local_llm_client import call_local_llm_json

SYSTEM_PROMPT = (
    "You are a query intent classifier for a knowledge retrieval system. "
    "Classify the user query and extract structured search intent.\n\n"
    "Return only JSON with these fields:\n"
    "- mode: one of 'answer', 'discovery_list', 'discovery_count', 'comparison'\n"
    "- reason: brief reason for the mode\n"
    "- role: the payload field to search — one of 'primary_name', 'description', "
    "'identifier', 'type', 'enum_value', 'aliases', or null if not applicable\n"
    "- target: the specific value or substring to search for within that role field, "
    "or null if not applicable\n\n"
    "Intent modes:\n"
    "- 'answer': single record lookup or specific question (e.g. 'what is tag 22', "
    "'sftp folder for gsact.txt', 'how to troubleshoot X')\n"
    "- 'discovery_list': expects MULTIPLE records (e.g. 'what tags contain price', "
    "'which tags have order in their name', 'what fields are in category airlines', "
    "'show me all notes about Moore', 'what are the Moore notes')\n"
    "- 'discovery_count': counting query (e.g. 'how many tags contain price', "
    "'how many notes are in Moore')\n"
    "- 'comparison': comparing two or more items\n\n"
    "Role/target extraction examples:\n"
    "- 'which tags have order in their name' -> role: primary_name, target: order\n"
    "- 'which tags have ID in their name' -> role: primary_name, target: ID\n"
    "- 'what tags contain price' -> role: primary_name, target: price\n"
    "- 'what fields are in category airlines' -> role: type, target: airlines\n"
    "- 'what string fields are available' -> role: type, target: string\n"
    "- 'what is tag 22' -> role: null, target: null\n"
    "- 'show me all notes about Moore' -> role: null, target: null\n"
    "- 'how many Moore notes are there' -> role: null, target: null\n\n"
    "Return only JSON, no other text."
)

QUESTIONS = [
    # FIX discovery — existing working
    "what tags contain security",
    "what tags contain broker",
    "what string fields are available",
    # FIX discovery — currently broken
    "which tags have order in their name",
    "which tags have ID in their name",
    # BBG discovery — existing working
    "what fields contain ask price",
    "what fields are in category airlines",
    # Single answer — must stay as answer
    "what is tag 22",
    "what values can tag 22 have",
    "sftp folder for gsact.txt",
    # Obsidian list
    "what are the Moore notes",
    "show me all notes about Moore",
    "how many Moore notes are there",
    # Procedural — must stay as answer
    "how to troubleshoot the lock file error",
    "steps for manual file loading in recon",
]

print(f"{'Question':<50} {'Mode':<18} {'Role':<15} {'Target':<20} Reason")
print("-" * 120)

for q in QUESTIONS:
    try:
        result = call_local_llm_json(SYSTEM_PROMPT, q, temperature=0.0)
        mode   = result.get("mode", "?")
        reason = result.get("reason", "")[:40]
        role   = result.get("role") or "-"
        target = result.get("target") or "-"
        print(f"{q:<50} {mode:<18} {role:<15} {target:<20} {reason}")
    except Exception as e:
        print(f"{q:<50} ERROR: {e}")
