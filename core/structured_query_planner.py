import json
from typing import Any, Dict, List, Optional
from core.local_llm_client import call_local_llm_json
from core.query_helpers import normalize_simple_text


def empty_plan(question: str, reason: str = "") -> Dict[str, Any]:
    return {
        "enabled": False,
        "confidence": 0.0,
        "reason": reason,
        "question": question,
        "intent": None,
        "target_type": None,
        "return_fields": [],
        "match": None,
        "filters": [],
        "limit": 10,
    }


def normalize_query_value(value: str) -> str:
    value = str(value or "").strip()
    value = value.replace("_", " ")
    value = normalize_simple_text(value)
    return value


def _strip_question_noise(question: str) -> str:
    q = normalize_query_value(question)

    noise_phrases = [
        "what is",
        "what are",
        "show me",
        "list",
        "find",
        "give me",
        "tell me",
        "the",
        "a",
        "an",
    ]

    for phrase in noise_phrases:
        if q.startswith(phrase + " "):
            q = q[len(phrase):].strip()

    return q


def _looks_structured_collection(collection_profile: Optional[Dict[str, Any]]) -> bool:
    if not collection_profile:
        return True

    doc_types = set(collection_profile.get("doc_types") or [])
    identifier_kinds = set(collection_profile.get("identifier_kinds") or [])

    if "structured" in doc_types:
        return True

    if "canonical" in identifier_kinds:
        return True

    return False


def plan_structured_query(
    question: str,
    collection_profile: Optional[Dict[str, Any]] = None,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Build a normalized retrieval plan for structured/canonical payloads.

    Uses local LLM for language interpretation.
    Still dry-run by default.
    """

    if not _looks_structured_collection(collection_profile):
        return empty_plan(question, "collection does not look structured")

    q = normalize_query_value(question)

    if not q:
        return empty_plan(question, "empty question")

    system_prompt = """
You are a retrieval query planner.

Convert the user's question into JSON only.
Do not answer the user's question.
Do not invent any data.
Only describe how retrieval should search existing structured payloads.

Available payload roles:
- identifier: numeric/string ID such as FIX tag number or Bloomberg field ID
- identifier_field: label for identifier, such as Tag, Id, Field
- identifier_namespace: namespace for identifier, such as tag, field, component, image_file
- primary_name: canonical name or mnemonic
- description: human description
- aliases: alternate names
- enum_values: allowed values/enumerations
- doc_type
- source_type
- source_file

Allowed intents:
- lookup: find the best matching structured record for a concept
- list: return multiple records matching a concept
- direct_lookup: lookup a specific identifier in a namespace

Output JSON object fields:
- enabled: boolean
- confidence: number between 0.0 and 1.0
- reason: short explanation of the retrieval interpretation
- question: original user question
- intent: one of lookup, list, direct_lookup
- target_type: structured
- search_concept: the business concept to search for, or empty string for direct identifier lookup
- search_roles: payload roles to search, usually ["primary_name", "description", "aliases"]
- return_fields: payload roles to return, such as ["identifier", "primary_name", "description"] or ["enum_values", "primary_name", "description"]
- preferred_identifier_namespace: requested identifier namespace such as "tag", "field", "component", or null
- direct_identifier: specific identifier value if directly requested, otherwise null
- limit: number of results to retrieve
- dry_run: true

Rules:
- Return a JSON object only.
- Do not copy a schema template.
- Do not answer the question.
- Do not invent retrieval results.
- confidence must be a meaningful number between 0.0 and 1.0.
- Use confidence >= 0.8 when the question clearly maps to structured retrieval.
- If the user asks "what is X", search_concept should be X and return_fields should include primary_name, identifier, description.
- If the user asks for the tag, field id, identifier, id, or component for X, search_concept should be X, preferred_identifier_namespace should be that requested word, and return_fields should include identifier, primary_name, description.
- If the user asks "what tag is X", this means: find the record matching X and return its identifier/tag. Do not include the word "tag" in search_concept.
- If the user asks "what is tag 22", "tag 22", "field 40", or similar, use intent direct_lookup, preferred_identifier_namespace tag/field/etc, direct_identifier as the number/string identifier, and search_concept should be empty.
- If the user asks what values/enums a tag/field/identifier can have, use intent direct_lookup when a direct identifier is present, set direct_identifier, and include enum_values in return_fields.
- If the user asks which records contain/include/with X, or asks plural forms like "what mnemonics contain price", use intent list, search_concept X, and limit 20.
- Search roles should usually include primary_name, description, aliases.
- Even if the user asks for a mnemonic/name/tag, the search should still look across primary_name, description, and aliases unless the question provides an exact code.
- Never place the full question in search_concept. Extract only the business concept.

Examples:

Question: what tag is exec broker
JSON:
{
  "enabled": true,
  "confidence": 0.9,
  "reason": "User wants the identifier/tag for the concept exec broker.",
  "question": "what tag is exec broker",
  "intent": "lookup",
  "target_type": "structured",
  "search_concept": "exec broker",
  "search_roles": ["primary_name", "description", "aliases"],
  "return_fields": ["identifier", "primary_name", "description"],
  "preferred_identifier_namespace": "tag",
  "direct_identifier": null,
  "limit": 5,
  "dry_run": true
}

Question: what values can tag 22 have
JSON:
{
  "enabled": true,
  "confidence": 0.95,
  "reason": "User wants enum values for direct tag identifier 22.",
  "question": "what values can tag 22 have",
  "intent": "direct_lookup",
  "target_type": "structured",
  "search_concept": "",
  "search_roles": ["identifier"],
  "return_fields": ["enum_values", "primary_name", "description"],
  "preferred_identifier_namespace": "tag",
  "direct_identifier": "22",
  "limit": 5,
  "dry_run": true
}

Question: what mnemonics contain price
JSON:
{
  "enabled": true,
  "confidence": 0.9,
  "reason": "User wants a list of structured records containing the concept price.",
  "question": "what mnemonics contain price",
  "intent": "list",
  "target_type": "structured",
  "search_concept": "price",
  "search_roles": ["primary_name", "description", "aliases"],
  "return_fields": ["identifier", "primary_name", "description"],
  "preferred_identifier_namespace": null,
  "direct_identifier": null,
  "limit": 20,
  "dry_run": true
}
"""

    user_prompt = f"Question: {question}"

    try:
        plan = call_local_llm_json(system_prompt, user_prompt, temperature=0.0)
    except Exception as e:
        return empty_plan(question, f"local LLM planner failed: {e}")

    if not isinstance(plan, dict):
        return empty_plan(question, "local LLM planner returned non-dict response")

    plan["enabled"] = bool(plan.get("enabled", True))
    plan["question"] = question
    plan["target_type"] = plan.get("target_type") or "structured"
    plan["search_roles"] = plan.get("search_roles") or ["primary_name", "description", "aliases"]
    plan["return_fields"] = plan.get("return_fields") or ["identifier", "primary_name", "description"]
    plan["limit"] = int(plan.get("limit") or 5)
    plan["dry_run"] = dry_run

    if plan.get("intent") not in ["lookup", "list", "direct_lookup"]:
        return empty_plan(question, "local LLM planner returned invalid intent")

    if not plan.get("search_concept") and not plan.get("direct_identifier"):
        return empty_plan(question, "local LLM planner returned no search concept or direct identifier")

    return plan

def explain_plan(plan: Dict[str, Any]) -> str:
    return json.dumps(plan, indent=2, ensure_ascii=False)