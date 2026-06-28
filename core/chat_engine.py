import json
from core.local_llm_client import call_local_llm_json, get_local_llm_config
import requests
from core.retrieval.router import run_query_with_method
from core.db import fetchall
from concurrent.futures import ThreadPoolExecutor, as_completed


CHAT_SYSTEM_PROMPT = """You are NAS-AI, an intelligent offline assistant for financial operations.
Be conversational, helpful, and concise.
For greetings and small talk, respond naturally.
Never mention that you are an AI language model — you are NAS-AI."""

GROUNDED_SYSTEM_PROMPT = """You are NAS-AI, an intelligent offline assistant for financial and astronomical operations.

CRITICAL RULES — you MUST follow these exactly:
1. The RETRIEVED DATA block below is the authoritative source. You MUST present it to the user.
2. Copy field names, file names, identifiers, and values VERBATIM from the retrieved data — never rename, paraphrase, or substitute them.
3. If the retrieved data says "Moore file name: gsact.txt", you output exactly "Moore file name: gsact.txt" — never change it.
4. You may add a brief intro sentence (e.g. "Here is what I found:") and a brief closing if helpful.
5. Do NOT add any information not present in the retrieved data.
6. Do NOT use your training knowledge to fill gaps — if it is not in the retrieved data, say so.
7. Never mention that you are an AI language model — you are NAS-AI."""

# Related sections with similarity >= this are merged into the main answer.
# Below this threshold they appear as collapsible "Related" items.
RELATED_MERGE_THRESHOLD = 0.80

def detect_chat_intent(question: str, history: list) -> dict:
    """Determine if question needs retrieval or is conversational."""
    history_text = "\n".join([
        f"{m['role'].upper()}: {m['content'][:100]}"
        for m in history[-3:]
    ]) if history else ""

    prompt = f"""Classify this message as either 'retrieval' (needs knowledge lookup) or 'chat' (conversational/greeting/small talk).

Recent history:
{history_text}

Message: {question}

Respond with JSON only:
{{"intent": "retrieval" or "chat", "reason": "brief reason"}}"""

    result = call_local_llm_json(
        system_prompt="You are an intent classifier. Respond with JSON only.",
        user_prompt=prompt,
        temperature=0.0
    )
    if result and result.get("intent") in ("retrieval", "chat"):
        return result
    # Default to retrieval for operational messages
    return {"intent": "retrieval", "reason": "default fallback"}

DEBUG = False

def select_collections(question: str, history: list, available_collections: list) -> list:
    """
    3-tier collection routing:
    Tier 1 — identifier/filename direct DB matchc
    Tier 2 — concept vector cluster LLM routing
    Tier 3 — fallback to first available collection
    """
    import re
    from core.db import fetchall as _fetchall

    selected = []
    seen = set()

    # Skip identifier lookup for procedural questions — route by concept instead
    _procedural = re.search(
        r'\b(how|steps|procedure|where|verify|check|login|connect|access)\b',
        question, re.IGNORECASE
    )
    if _procedural:
        _identifiers = []  # skip Tier 1

    # --- Tier 1: direct identifier/filename match across collections ---
    _filenames = re.findall(r'\b[a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5}\b', question)
    #_tags = re.findall(r'\btag\s+(\d+)\b', question, re.IGNORECASE)
    _identifiers = _filenames #+ _tags

    _tier1_hits = []
    for _id in _identifiers:
        for _col in available_collections:
            if _col in _tier1_hits:
                continue
            _hit = _fetchall(
                """SELECT 1 FROM chunks
                   WHERE collection_name = %s
                   AND payload->>'identifier' ILIKE %s
                   LIMIT 1""",
                (_col, _id)
            )
            if _hit:
                _tier1_hits.append(_col)

    # Also add collections linked via confirmed cross-links
    for _id in _identifiers:
        linked = _fetchall(
            """SELECT DISTINCT target_collection FROM cross_links
               WHERE source_identifier ILIKE %s
               AND status = 'confirmed'""",
            (_id,)
        )
        for row in linked:
            col = row["target_collection"]
            if col in available_collections and col not in _tier1_hits:
                _tier1_hits.append(col)

    # Don't return early — let Tier 2 LLM determine ordering
    # Tier 1 hits will be merged after Tier 2

    # --- Tier 2: concept vector cluster LLM routing ---
    history_text = "\n".join([
        f"{m['role'].upper()}: {m['content'][:100]}"
        for m in history[-3:]
    ]) if history else ""

    from core.paths import COLLECTIONS_PATH
    import json as _json
    try:
        with open(COLLECTIONS_PATH, 'r') as _f:
            _coll_cfg = _json.load(_f)
    except Exception:
        _coll_cfg = {}

    collection_lines = []
    for c in available_collections:
        _rdesc = _coll_cfg.get(c, {}).get("routing_description", "")
        _filetypes = _coll_cfg.get(c, {}).get("allowed_filetypes", [])
        _is_doc = any(ft in _filetypes for ft in ["doc", "pdf", "docx"])

        if _rdesc and _is_doc:
            # Doc collections: static description + concept vector topics
            clusters = _fetchall(
                "SELECT DISTINCT group_value FROM concept_vectors WHERE collection = %s ORDER BY group_value",
                (c,)
            )
            if clusters:
                topics = ", ".join(r["group_value"] for r in clusters)
                collection_lines.append(f"- {c}: {_rdesc} [topics: {topics}]")
            else:
                collection_lines.append(f"- {c}: {_rdesc}")
        elif _rdesc:
            # Structured collections: static description only
            collection_lines.append(f"- {c}: {_rdesc}")
        else:
            # No description: concept vectors only
            clusters = _fetchall(
                "SELECT DISTINCT group_value FROM concept_vectors WHERE collection = %s ORDER BY group_value",
                (c,)
            )
            if clusters:
                topics = ", ".join(r["group_value"] for r in clusters)
                collection_lines.append(f"- {c}: covers [{topics}]")
            else:
                collection_lines.append(f"- {c}")
    collections_str = "\n".join(collection_lines)

    _procedural_hint = "\nNote: this question is procedural (how-to/steps) — prefer doc/note collections over structured data collections." if _procedural else ""
    prompt = f"""Given this question and conversation history, which collections are relevant?
Pick 1 to 3 collections. Return the most relevant first.{_procedural_hint}

Collections:
{collections_str}

Recent history:
{history_text}

Question: {question}

Respond with JSON only:
{{"collections": ["<name1>", "<name2>"], "reason": "brief reason"}}"""

    result = call_local_llm_json(
        system_prompt="You are a collection router. Respond with JSON only.",
        user_prompt=prompt,
        temperature=0.0
    )

    if result and isinstance(result.get("collections"), list):
        for c in result["collections"]:
            if c in available_collections and c not in seen:
                selected.append(c)
                seen.add(c)

    # Merge Tier 1 hits that Tier 2 missed (preserve Tier 2 ordering)
    for _col in _tier1_hits:
        if _col not in seen:
            selected.append(_col)
            seen.add(_col)

    if selected:
        return selected[:3]

    # --- Tier 3: fallback ---
    return [available_collections[0]] if available_collections else []

def run_parallel_queries(collections: list, question: str) -> dict:
    """
    Fan out run_query_with_method across 1–3 collections in parallel.
    Returns the best result: highest-scoring single answer, plus all related_sections merged.
    """
    if not collections:
        return {"result": "No collections available.", "related_sections": [], "collection": None}

    if len(collections) == 1:
        result = run_query_with_method(
            collections[0], question, limit=10,
            show_exact_links=True, show_related_topics=True,
            skip_planner=True, force_answer=True
        )
        result["collection"] = collections[0]
        return result

    results = {}
    with ThreadPoolExecutor(max_workers=len(collections)) as executor:
        futures = {
            executor.submit(
                run_query_with_method,
                col, question, "best", 10, True, True, True, True
            ): col
            for col in collections
        }
        for future in as_completed(futures):
            col = futures[future]
            try:
                results[col] = future.result()
            except Exception as e:
                results[col] = {"result": f"Error querying {col}: {e}", "related_sections": []}

    # Pick best result: prefer non-"No answer found" results, then first collection wins
    best_col = None
    best_result = None
    for col in collections:  # respects priority order from select_collections
        r = results.get(col, {})
        answer = r.get("result", "")
        if answer and "No answer found" not in answer and "No record found" not in answer and "No exact match found" not in answer:
            best_col = col
            best_result = r
            break

    if not best_result:
        best_col = collections[0]
        best_result = results.get(best_col, {"result": "No answer found.", "related_sections": []})

    # Merge related_sections from all collections
    merged_related = list(best_result.get("related_sections") or [])
    seen = {(s["collection"], s["title"]) for s in merged_related}
    for col in collections:
        if col == best_col:
            continue
        for sec in results.get(col, {}).get("related_sections") or []:
            key = (sec["collection"], sec["title"])
            if key not in seen:
                seen.add(key)
                merged_related.append(sec)

    best_result["collection"] = best_col
    best_result["collections_queried"] = collections
    best_result["related_sections"] = merged_related
    return best_result

def extract_focus_terms(history, last_n=3):
    """Extract focus terms from recent conversation history."""
    import re
    terms = []
    
    # Patterns to extract
    patterns = [
        r'\b([a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5})\b',  # filenames
        r'\btag\s+(\d+)\b',                              # FIX tags
        r'\b([A-Z][A-Z0-9_]{3,})\b',                    # ALL_CAPS identifiers
        r'\b(Goldman|JPMorgan|Citi|CSFB|Barclays|UBS|Morgan|Chase|BONY)\b',  # broker names
    ]
    
    for msg in history[-last_n:]:
        text = msg.get("content", "")
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for m in matches:
                if m not in terms:
                    terms.append(m)
    
    return terms


def augment_query_with_focus(question: str, history: list) -> str:
    """Only inject history terms if the current question has no identifiers of its own."""
    import re
    # If the question already contains its own identifiers, don't augment
    _own_files = re.findall(r'\b[a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5}\b', question)
    _own_tags = re.findall(r'\btag\s+\d+\b', question, re.IGNORECASE)
    _own_identifiers = _own_files + _own_tags
    if _own_identifiers:
        return question  # question is self-contained

    focus = extract_focus_terms(history)
    q_lower = question.lower()
    missing = [t for t in focus if t.lower() not in q_lower]
    if missing:
        return f"{question} [{' '.join(missing)}]"
    return question

def _strip_ocr_markers(text: str) -> str:
    """
    Remove [Embedded image OCR from: ...] markers and all OCR content
    that follows until the next real prose line.
    """
    import re
    # Remove from OCR marker to the next line starting with a digit+dot+space+capital,
    # or a capital word (prose), or a heading, or end of string
    text = re.sub(
        r'\[Embedded image OCR from:[^\]]*\].*?(?=\n\d+\.\s+[A-Z]|\n[A-Z][a-z]{3,}|\n#\s|\Z)',
        '',
        text,
        flags=re.DOTALL
    )
    return text.strip()

def _extract_key_terms(text: str) -> list:
    """Extract identifiers, filenames, and capitalised tokens from retrieved text."""
    import re
    terms = []
    terms += re.findall(r'\b[a-zA-Z0-9_\-]+\.[a-zA-Z0-9]{2,5}\b', text)  # filenames
    terms += re.findall(r'\b\d{2,6}\b', text)                               # numeric IDs
    terms += re.findall(r'\b[A-Z][A-Z0-9_]{3,}\b', text)                   # ALL_CAPS tokens
    return list(set(t.lower() for t in terms))


def _response_is_faithful(response: str, retrieved_answer: str) -> bool:
    """
    Verify >=70% of key terms from retrieved_answer appear in the LLM response.
    If not, the LLM likely renamed or dropped factual content.
    """

    key_terms = _extract_key_terms(retrieved_answer)
    if not key_terms:
        return True
    resp_lower = response.lower()
    matched = sum(1 for t in key_terms if t in resp_lower)
    print("DEBUG faithful check:", _faithful, "baseline:", _baseline[:50])
    return matched >= max(1, int(len(key_terms) * 0.7))


def generate_conversational_response(question: str, history: list, retrieved_answer: str = None, primary_answer: str = None) -> str:
    """
    Generate a response.
    - With retrieved_answer: uses GROUNDED_SYSTEM_PROMPT + low temperature + faithfulness guard.
    - Without retrieved_answer: uses CHAT_SYSTEM_PROMPT for free-form conversation.
    """
    system_prompt = GROUNDED_SYSTEM_PROMPT if retrieved_answer else CHAT_SYSTEM_PROMPT

    messages = []
    for m in history[-5:]:
        messages.append({"role": m["role"], "content": m["content"]})

    if retrieved_answer:
        user_content = (
            f"{question}\n\n"
            f"RETRIEVED DATA (present verbatim — do not rename or paraphrase field values):\n"
            f"{retrieved_answer}"
        )
    else:
        user_content = question

    messages.append({"role": "user", "content": user_content})

    try:
        cfg = get_local_llm_config()
        url = cfg["base_url"].rstrip("/") + "/v1/chat/completions"
        payload = {
            "model": cfg.get("model", "local-model"),
            "messages": [{"role": "system", "content": system_prompt}] + messages,
            "temperature": 0.2 if retrieved_answer else 0.7,
            "max_tokens": 2048,
        }
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        llm_response = resp.json()["choices"][0]["message"]["content"].strip()
        # Faithfulness guard: check against primary answer only, not appended context
        _baseline = primary_answer or retrieved_answer
        _faithful = _response_is_faithful(llm_response, _baseline) if _baseline else True
        if DEBUG:
            print("DEBUG faithful:", _faithful)
            print("DEBUG baseline terms:", _extract_key_terms(_baseline or ""))
            print("DEBUG llm_response snippet:", llm_response[:200])
        if _baseline and not _faithful:
            return _baseline
        if DEBUG:
            print("DEBUG final content snippet:", response[:300])
        return llm_response

    except Exception:
        _baseline = primary_answer or retrieved_answer
        if _baseline:
            return _baseline

        return "I'm not sure how to answer that. Please try again."


def chat_turn(question: str, history: list, available_collections: list) -> dict:
    """
    Process one chat turn. Returns:
    {
        role: assistant,
        content: response text,
        method: retrieval or chat,
        collection: which collection was queried (if retrieval),
        related_sections: cross-link enrichment (if any)
    }
    """
    # Step 1: detect intent
    intent = detect_chat_intent(question, history)
    print("DEBUG available_collections:", available_collections)
    if intent["intent"] == "chat":
        response = generate_conversational_response(question, history)
        return {
            "role": "assistant",
            "content": response,
            "method": "chat",
            "collection": None,
            "related_sections": []
        }

    # Step 2: select collections (1–3, ranked by relevance)
    collections = select_collections(question, history, available_collections)
    if not collections:
        return {
            "role": "assistant",
            "content": "I don't have any collections available to search.",
            "method": "retrieval",
            "collection": None,
            "related_sections": []
        }

    # Step 3: retrieve answer (parallel across selected collections)
    augmented_question = augment_query_with_focus(question, history)
    query_run = run_parallel_queries(collections, augmented_question)
    if DEBUG:
        print("DEBUG augmented_question:", augmented_question)
        print("DEBUG query_run related:", [(s.get('collection'), s.get('confidence'), bool(s.get('anchor_chunk_ids'))) for s in query_run.get('related_sections', [])])
    primary_answer = query_run.get("result", "No answer found.")
    retrieved = primary_answer
    all_related = query_run.get("related_sections", [])
    if DEBUG:
        print("DEBUG all_related:", [(s.get('collection'), s.get('confidence')) for s in all_related])
        print("DEBUG collections selected:", collections)
    collection = query_run.get("collection")

    # Split related sections: high-confidence → merge into answer, low-confidence → show as related
    import json as _json
    try:
        with open(COLLECTIONS_PATH, 'r') as _f:
            _coll_cfg_chat = _json.load(_f)
    except Exception:
        _coll_cfg_chat = {}

    def _is_structured_collection(col):
        from core.db import fetchall as _fа
        rows = _fа(
            "SELECT DISTINCT payload->>'doc_type' AS dt FROM chunks WHERE collection_name = %s LIMIT 5",
            (col,)
        )
        doc_types = {r['dt'] for r in rows if r['dt']}
        return doc_types == {'structured'} or doc_types == {'structured', None}

    high_confidence = [
        s for s in all_related
        if s.get("confidence", 0) >= RELATED_MERGE_THRESHOLD
        and not (s.get("match_type") == "concept" and _is_structured_collection(s.get("collection", "")))
    ]
    related_sections = [
        s for s in all_related
        if s.get("confidence", 0) < RELATED_MERGE_THRESHOLD
        and not (s.get("match_type") == "concept" and _is_structured_collection(s.get("collection", "")))
    ]

    merged_image_payload = None
    # Append high-confidence previews to the retrieved answer before sending to LLM
    if high_confidence:
        extra_parts = []
        for s in high_confidence:
            preview = _strip_ocr_markers(s.get("preview") or "")
            if DEBUG:
                print("DEBUG preview len:", len(preview), "for", s.get('collection'))
            if preview:
                extra_parts.append(
                    f"[Additional context from {s['collection']} — {s['title']}]:\n{preview}"
                )
            # Find first anchor chunk that has image data
            if not merged_image_payload:
                for chunk_id in (s.get("anchor_chunk_ids") or []):
                    try:
                        rows = fetchall(
                            "SELECT payload FROM chunks WHERE id = %s LIMIT 1",
                            (chunk_id,)
                        )
                        if rows:
                            p = rows[0]["payload"]
                            if isinstance(p, str):
                                import json
                                p = json.loads(p)
                            if p.get("embedded_image_paths"):
                                merged_image_payload = p
                                break
                    except Exception:
                        continue
        if DEBUG:
            print("DEBUG extra_parts count:", len(extra_parts))
            print("DEBUG retrieved len before append:", len(retrieved))
        if extra_parts:
            retrieved = primary_answer  # LLM only sees primary; context appended post-LLM

    if DEBUG:
        print("DEBUG retrieved snippet:", retrieved[:200])
    # Step 4: wrap in conversational response
    if primary_answer and "No answer found" in primary_answer:
        response = "I couldn't find specific information about that in my knowledge base. Could you provide more details or rephrase the question?"
    else:
        response = generate_conversational_response(question, history, retrieved_answer=retrieved, primary_answer=primary_answer)
    if high_confidence and extra_parts:
        formatted_parts = []
        for part in extra_parts:
            # Remove the [Additional context from X — Y]: label, keep only the content
            lines = part.split("\n", 1)
            content = lines[1].strip() if len(lines) > 1 else lines[0]
            formatted_parts.append(content)
        response = response + "\n\n---\n" + "\n\n---\n".join(formatted_parts)

    print("DEBUG merged_image_payload:", merged_image_payload is not None)
    print("DEBUG query_run answer_payload:", bool(query_run.get("answer_payload")))

    return {
        "role": "assistant",
        "content": response,
        "method": "retrieval",
        "collection": collection,
        "collections_queried": query_run.get("collections_queried", [collection]),
        "related_sections": related_sections,
        "raw_answer": primary_answer,
        "answer_payload": query_run.get("answer_payload") if query_run.get("answer_payload") and (query_run.get("answer_payload") or {}).get("embedded_image_paths") else (merged_image_payload or None)
    }