import re
import html as _html

DEBUG = False

# =========================================================
# GENERIC HELPERS
# =========================================================
def _row_norm(row):
    return {str(k).lower(): v for k, v in row.items()}


def _strip_html(text):
    """P2: convert HTML/markup to clean text instead of discarding it.
    Unescapes entities, removes tags, and collapses whitespace."""
    t = _html.unescape(str(text or ""))
    t = re.sub(r"<[^>]+>", " ", t)
    t = t.replace("\xa0", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _norm_for_dedup(text):
    """Normalize for redundancy comparison: lowercase, alphanumerics only."""
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def _is_near_duplicate(a, b):
    """True if normalized a and b are effectively the same content (e.g. a markdown
    column mirroring its plain-text counterpart)."""
    if not a or not b:
        return False
    if a == b:
        return True
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    return len(short) >= 20 and short in long and len(short) / len(long) >= 0.8


def clean_dedup_text(values):
    """HTML-strip each value and drop near-duplicates, joining with blank lines.
    Used for the stored 'description' field so answers render clean, deduped text."""
    out, norms = [], []
    for v in values or []:
        s = _strip_html(v)
        if not s:
            continue
        n = _norm_for_dedup(s)
        if any(_is_near_duplicate(n, e) for e in norms):
            continue
        out.append(s)
        norms.append(n)
    return "\n\n".join(out)


def _first_value(row_norm, fields):
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            return str(val).strip()
    return ""


def _all_values(row_norm, fields):
    values = []
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            values.append(str(val).strip())
    return values

if DEBUG:
    print("[NLP_GENERATOR LOADED FROM]", __file__)

# =========================================================
# STRUCTURED NLP TEXT
# =========================================================
def build_structured_nlp_text(row, schema):
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    alias_fields = schema.get("aliases", [])
    type_fields = schema.get("type", [])
    other_fields = schema.get("other", [])

    identifier = _first_value(row_n, id_fields)
    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    description = "\n".join(str(v) for v in description_values if v)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    other_fields = schema.get("other", [])
    alias_fields = schema.get("aliases", [])

    other_values = _all_values(row_n, other_fields)
    alias_values = _all_values(row_n, alias_fields)

    parts = []

    if identifier:
        parts.append(str(identifier))

    if primary_name:
        parts.append(primary_name)

    if description:
        parts.append(description)

    if other_values:
        parts.append("\n".join(c for c in (_strip_html(v) for v in other_values if v) if c))

    if type_value:
        parts.append(f"Type: {type_value}")

    if alias_values:
        parts.append("Also known as: " + ", ".join(alias_values))

    # Reference identifiers must appear in nlp_text or BM25/trigram can never
    # find the record by them (field not in nlp_text = invisible to lexical).
    ref_values = _all_values(row_n, schema.get("reference_identifier", []))
    if ref_values:
        parts.append("References: " + ", ".join(ref_values))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# ENTITY-ROW NLP TEXT
# =========================================================
def build_entity_row_nlp_text(row, schema):
    row_n = _row_norm(row)
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    other_fields = schema.get("other", [])
    alias_fields = schema.get("aliases", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)
    other_values = _all_values(row_n, other_fields)
    alias_values = _all_values(row_n, alias_fields)

    
    if DEBUG:
        if '21R2 Weekend' in str(row_n.get('abstract', '')):
            print(f"[NLP DEBUG] row keys={list(row_n.keys())[:5]} other_values={_all_values(row_n, schema.get('other', []))}")

    # P2 (lossless) + dedup: include every field's text HTML-stripped, but skip values
    # that are near-duplicates of one already added (e.g. *Markdown columns that mirror
    # description/resolution). Long content is handled downstream by chunking (P0).
    parts = []
    norms = []

    def _add(text):
        text = _strip_html(text)
        if not text:
            return
        n = _norm_for_dedup(text)
        if any(_is_near_duplicate(n, e) for e in norms):
            return
        parts.append(text)
        norms.append(n)

    if primary_name:
        _add(primary_name)
    for d in description_values:
        _add(d)
    for o in other_values:
        _add(o)

    if alias_values:
        parts.append("Also known as: " + ", ".join(alias_values))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# PROCEDURAL NLP TEXT
# =========================================================
def build_procedural_nlp_text(row, schema):
    row_n = _row_norm(row)

    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    if not parts:
        fallback = []
        for k, v in row.items():
            if v not in [None, ""]:
                fallback.append(f"{k}: {str(v).strip()}")
        if fallback:
            parts.append("\n".join(fallback))

    return "\n\n".join([p for p in parts if p]).strip()