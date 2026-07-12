"""
core/ner_cross_linker.py
========================
CL-03: identifier-mention cross-linking.

Scans a source collection's text for *known, distinctive* identifiers from other
collections (e.g. RECON filenames like gspos.txt, code-like mnemonics) and creates
cross-links (e.g. obsidian -> recon_assist_file). This is gazetteer matching against
the known identifier set — more precise and deterministic than free-form LLM NER,
since the target entities are already enumerated in the database.

File-agnostic: any source collection -> any target collection. Precision guards mirror
CL-01/CL-02 (whole-word match, generic-term filter, meaningful context for non-files).
"""
import re
from core.db import fetchall

_FILENAME_RE = re.compile(r'.+\.[A-Za-z0-9]{1,5}$')


def _is_filename(term):
    return bool(_FILENAME_RE.match((term or '').strip()))


def _is_distinctive(term):
    """Keep only identifiers unlikely to collide with ordinary prose."""
    t = (term or '').strip()
    if len(t) < 6 or t.isdigit():
        return False
    if _is_filename(t):                                  # gspos.txt, eq_act.csv
        return True
    if re.search(r'\d', t) and ('_' in t or t.upper() == t):  # 020_W_RECON_GOLDMAN_PB_PULL
        return True
    if '_' in t and t.upper() == t and len(t) >= 8:      # ARD_OPERATING_EXP_PER_ASM_ASK
        return True
    return False


def build_gazetteer(target_collections, generic):
    """term_lower -> {term, targets:[(collection, identifier, is_filename)]}."""
    gaz = {}
    import json as _json
    for col in target_collections:
        # COLUMNS first, payload as fallback — payload->>'identifier' is not
        # reliably populated (recon rows carry it only in the column, which
        # left gsact.txt out of the gazetteer entirely).
        rows = fetchall("""
            SELECT DISTINCT COALESCE(identifier, payload->>'identifier') AS identifier,
                   COALESCE(primary_name, payload->>'primary_name') AS primary_name,
                   payload->>'aliases' AS aliases
            FROM chunks WHERE collection_name = %s
        """, (col,))
        for r in rows:
            ident = r.get('identifier')
            _terms = [ident, r.get('primary_name')]
            try:  # aliases ("Also known as" — e.g. the PB/broker filename)
                _al = _json.loads(r.get('aliases') or '[]')
                if isinstance(_al, list):
                    _terms += [str(a) for a in _al]
            except Exception:
                pass
            for field in _terms:
                term = (field or '').strip()
                if not term or term.lower() in generic or not _is_distinctive(term):
                    continue
                key = term.lower()
                entry = gaz.setdefault(key, {'term': term, 'targets': []})
                tgt = (col, ident or term, _is_filename(term))
                if tgt not in entry['targets']:
                    entry['targets'].append(tgt)
    return gaz


def discover_identifier_mentions(source_collection, target_collections=None):
    """Return cross-link candidate dicts (not yet saved)."""
    from core.cross_link_discoverer import _meaningful_context
    from core.query_helpers import load_doc_query_hints

    if target_collections is None:
        rows = fetchall(
            "SELECT DISTINCT collection_name AS c FROM chunks WHERE collection_name != %s",
            (source_collection,))
        target_collections = [r['c'] for r in rows]

    generic = {t.lower() for t in load_doc_query_hints().get('generic_terms', [])}
    gaz = build_gazetteer(target_collections, generic)
    if not gaz:
        return []

    # one compiled alternation, longest terms first, whole-word bounded
    terms_sorted = sorted((g['term'] for g in gaz.values()), key=len, reverse=True)
    pattern = re.compile(
        r'(?<![A-Za-z0-9])(' + '|'.join(re.escape(t) for t in terms_sorted) + r')(?![A-Za-z0-9])',
        re.IGNORECASE)

    # Chunked collections (e.g. obsidian: note_chunk_N) anchor on source_file; others
    # (entity_row/structured like kb_docs, where every article shares one export file)
    # anchor on the row identifier, so a mention is credited to the specific
    # article/record — not the whole file (which mislabels the link).
    _sample = fetchall("""SELECT payload->>'identifier' AS i FROM chunks
                          WHERE collection_name = %s AND payload->>'identifier' IS NOT NULL
                          LIMIT 5""", (source_collection,))
    _chunked = any("_chunk_" in (r.get("i") or "") for r in _sample)
    _id_expr = ("payload->>'source_file'" if _chunked
                else "COALESCE(payload->>'identifier', payload->>'source_file')")
    src_rows = fetchall(f"""
        SELECT {_id_expr} AS sid,
               COALESCE(payload->>'text', payload->>'description', '') AS text
        FROM chunks
        WHERE collection_name = %s AND {_id_expr} IS NOT NULL
    """, (source_collection,))

    candidates, seen = [], set()
    for r in src_rows:
        sf = (r.get('sid') or '').strip()
        text = r.get('text') or ''
        if not sf or not text:
            continue
        for m in pattern.finditer(text):
            matched = m.group(1)
            entry = gaz.get(matched.lower())
            if not entry:
                continue
            ctx_ok = _meaningful_context(text, matched)
            for (tcol, tid, is_file) in entry['targets']:
                if tcol == source_collection:
                    continue
                # filenames are unambiguous; non-file code terms need real context
                if not is_file and not ctx_ok:
                    continue
                key = (sf, tcol, tid)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append({
                    "source_collection": source_collection,
                    "source_identifier": sf,
                    "target_collection": tcol,
                    "target_identifier": tid,
                    "match_type": "ner",
                    "confidence": 0.85 if is_file else 0.6,
                })
    return candidates


def run_identifier_ner(source_collection, target_collections=None):
    """Discover + save identifier-mention cross-links. Returns the candidate list."""
    from core.cross_link_store import ensure_cross_links_table, save_cross_link_candidates
    ensure_cross_links_table()
    cands = discover_identifier_mentions(source_collection, target_collections)
    if cands:
        save_cross_link_candidates(cands)
    print(f"[NER] {source_collection}: {len(cands)} identifier-mention candidates")
    return cands
