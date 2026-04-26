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

    procedural_keywords = [
        "step", "steps", "procedure", "instructions", "checklist",
        "how to", "runbook", "follow these steps", "initial status check"
    ]

    reference_keywords = [
        "overview", "summary", "introduction", "benefits",
        "background", "reference", "white paper", "case study"
    ]

    procedural_score = 0
    reference_score = 0

    # block-pattern signals
    if bullet_count >= 3:
        procedural_score += 3

    if heading_count >= 2:
        reference_score += 1

    if heading_count >= 2 and bullet_count >= 2:
        procedural_score += 1

    # text signals
    if _contains_any(full_text, procedural_keywords):
        procedural_score += 3

    if _contains_any(full_text, reference_keywords):
        reference_score += 2

    # numbered step signals
    if re.search(r"\bstep\s+\d+\b", full_text):
        procedural_score += 2

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