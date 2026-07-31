"""core/glossary.py — declared domain vocabulary (config system.json
`glossary`, journaled edits via core.config_store).

The site's terms mean things the models cannot know: CTM is not Charles
River, Moore is MCM, 'bad dates' are recon files' incorrect dates. Each
entry: term -> {means, same_as: [...]}. Declared once, consumed at three
layers, each deterministic about WHEN it applies (the term or a synonym
appears in the question):

  * prompt injection  — glossary_for_question() -> lines appended to LLM
    prompts (reranker first) so wrong beliefs get corrected in-context
  * discovery filters — synonyms_for() widens a contains-filter to an OR
    over declared synonyms ('bad dates' also matches 'incorrect dates')
  * future: routing expansion, "remember that X means Y" capture (M3)
"""
import re
from typing import Dict, List


def load_glossary() -> Dict:
    try:
        import json
        from core.paths import SYSTEM_CONFIG_PATH
        with open(SYSTEM_CONFIG_PATH, "r", encoding="utf-8") as f:
            g = json.load(f).get("glossary") or {}
        return {str(k).lower(): v for k, v in g.items() if isinstance(v, dict)}
    except Exception:
        return {}


def _mentioned(term: str, entry: Dict, text_low: str) -> bool:
    """Word-boundary match of the term or any synonym in the text."""
    for cand in [term] + [str(s).lower() for s in entry.get("same_as") or []]:
        if cand and re.search(r"\b" + re.escape(cand) + r"\b", text_low):
            return True
    return False


def glossary_for_question(question: str) -> List[str]:
    """Lines like 'ctm: Central Trade Manager ...' for every entry whose
    term or synonym appears in the question. Empty list = inject nothing."""
    q = str(question or "").lower()
    out = []
    for term, entry in load_glossary().items():
        if _mentioned(term, entry, q):
            means = str(entry.get("means") or "").strip()
            if means:
                syns = [str(s) for s in entry.get("same_as") or []]
                syn_txt = f" (also written: {', '.join(syns)})" if syns else ""
                out.append(f"- {term}: {means}{syn_txt}")
    return out


def synonyms_for(phrase: str) -> List[str]:
    """Declared synonyms for a term matching this exact phrase (lowercased),
    NOT including the phrase itself. Empty when undeclared."""
    entry = load_glossary().get(str(phrase or "").lower())
    if not entry:
        return []
    return [str(s) for s in entry.get("same_as") or []
            if str(s).lower() != str(phrase).lower()]
