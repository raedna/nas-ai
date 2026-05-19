from collections import Counter
import re

DEBUG = True


# =========================================================
# HELPERS
# =========================================================
def _norm(text):
    return str(text or "").strip().lower()


def _count_block_types(blocks):
    counts = Counter()

    for block in blocks or []:
        block_type = block.get("block_type", "unknown")
        counts[block_type] += 1

    return counts


def _contains_any(text, keywords):
    t = _norm(text)
    return any(k in t for k in keywords)

import json
from core.paths import CONFIG_DIR

def load_doc_detection_hints():
    path = CONFIG_DIR / "doc_detection_hints.json"

    defaults = {
        "procedural_keywords": [],
        "reference_keywords": [],
        "scoring": {}
    }

    if not path.exists():
        return defaults

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {**defaults, **data}
    except Exception:
        return defaults


# =========================================================
# MAIN DETECTOR
# =========================================================
def detect_doc_type(parsed):
    blocks = parsed.get("blocks", [])
    full_text = _norm(parsed.get("text", ""))

    if not blocks and not full_text:
        return "reference"

    counts = _count_block_types(blocks)

    heading_count = counts.get("heading", 0)
    bullet_count = counts.get("bullet", 0)
    paragraph_count = counts.get("paragraph", 0)

    hints = load_doc_detection_hints()

    procedural_keywords = hints.get("procedural_keywords", [])
    reference_keywords = hints.get("reference_keywords", [])
    scoring = hints.get("scoring", {})

    procedural_score = 0
    reference_score = 0

    # block-pattern signals
    if bullet_count >= scoring.get("bullet_count_procedural_min", 3):
        procedural_score += scoring.get("bullet_count_procedural_score", 3)

    if heading_count >= scoring.get("heading_reference_min", 2):
        reference_score += scoring.get("heading_reference_score", 1)
    if (
        heading_count >= scoring.get("heading_bullet_heading_min", 2)
        and bullet_count >= scoring.get("heading_bullet_bullet_min", 2)
    ):

        procedural_score += scoring.get("heading_bullet_procedural_score", 1)

    # text signals
    if _contains_any(full_text, procedural_keywords):
        procedural_score += scoring.get("procedural_keyword_score", 3)

    if _contains_any(full_text, reference_keywords):
        reference_score += scoring.get("reference_keyword_score", 2)

    # numbered step signals
    if re.search(scoring.get("numbered_step_pattern", r"\bstep\s+\d+\b"), full_text):
        procedural_score += scoring.get("numbered_step_score", 2)

    # body-heavy narrative/reference
    if paragraph_count >= 4 and bullet_count <= 1:
        reference_score += 2

    # decision
    if procedural_score >= 4 and reference_score >= 3:
        doc_type = "mixed"
    elif procedural_score > reference_score:
        doc_type = "procedural"
    elif reference_score > procedural_score:
        doc_type = "reference"
    else:
        doc_type = "narrative"

    if DEBUG:
        print("[DOC DETECTOR] heading_count:", heading_count)
        print("[DOC DETECTOR] bullet_count:", bullet_count)
        print("[DOC DETECTOR] paragraph_count:", paragraph_count)
        print("[DOC DETECTOR] procedural_score:", procedural_score)
        print("[DOC DETECTOR] reference_score:", reference_score)
        print("[DOC DETECTOR] doc_type:", doc_type)

    return doc_type