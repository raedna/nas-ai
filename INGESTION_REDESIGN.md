# NAS-AI Ingestion Re-evaluation & Redesign Plan

*Scope: how ingestion limits retrieval, and what to change. Grounded in the current
code path: `parse → infer_schema → detect_table_type → serialize (NLP text + payload)
→ merge_collection_docs → embed_texts → upsert`.*

## Core thesis

Whatever ingestion embeds and stores is the hard ceiling on retrieval. Two upstream
problems silently cap recall for **every** query; the concept-vector (CV-01..04) and
cross-link (CL-01/02/04) tickets optimize *under* that ceiling. Fix the ceiling first,
then the CV/CL work compounds on top.

---

## Findings, ranked by retrieval impact

### P0 — Silent embedding truncation (corpus-wide recall loss) — MEASURED, CONFIRMED
- `core/embedder.py` posts full text to the embeddings endpoint with **no length
  handling**. Truncation point measured directly (`diag_truncation_proof.py`): the
  embedding vector is **identical from ~2,500 chars onward** (cosine 1.0000; only 0.80
  at 2,000 chars). So the real cap is **≈2,500 chars / ~512 tokens** — everything past
  that is silently ignored.
- `kb_docs` is **one vector per article** (178 chunks = 178 articles). Measured:
  **60 of 178 articles (33.7%) exceed the cap**; the longest is 11,400 chars and loses
  **78% of its content** from the embedding (only the first ~2,500 chars are seen).
- Also affects `obsidian` (26 chunks, max 5,286 chars), `pdf_test` (max 6,041),
  `image_test`. Structured collections (astro, bbg, xml, recon) are fine — short rows.
- **Fix:** token-aware chunking of long content (entity rows + doc/PDF blocks) so no
  single vector exceeds ~2,000 chars (~400 tokens) with overlap to preserve context.

### P1 — Schema mapping fidelity controls what gets embedded
- `build_entity_row_nlp_text` **drops** any `other`-role field that is ≥200 chars or
  contains `<`. So a long field mis-mapped to `other` (qwen put kb `resolution`→`other`
  in testing) is silently deleted from the embedding.
- This is why robust schema inference matters beyond tags: wrong roles = lost content.
- **Fix:** (a) the schema guardrails already discussed — cross-role dedup, tightened
  `tags` role, wide-file/error fallback to heuristic; (b) make the NLP builder include
  *all* substantive fields regardless of role, rather than discarding `other`.

### P2 — Lossy NLP-text construction rules
- The "skip values ≥200 chars or containing `<`" rule throws away real content (HTML
  descriptions, markdown fields) instead of cleaning it.
- **Fix:** strip HTML to text and include it; stop length-gating substantive fields.
  Keep a sane overall cap, but cap by *chunking*, not by *dropping*.

### P3 — Doc/PDF chunking is block-count based, not token/heading aware
- `max_blocks_per_chunk = 4` can merge four long paragraphs into a >512-token chunk
  (truncated again) or split a coherent section across chunks.
- **Fix:** heading-aware + token-bounded chunking. Improves recall *and* the
  `section_heading` grouping that CV-01 relies on.

### P4 — Metadata surfacing (tags, clean headings)
- `kbtags` is parsed by the source but never surfaced into the payload → lost signal
  for filtering, concept grouping (CV-02), and cross-links.
- Obsidian `section_heading` contains junk (`###`, `-`, `---`) → weak titles/grouping.
- **Fix:** surface `kb_tags` via the `tags` role (file-agnostic, LLM-detected);
  normalize/clean headings at ingest.

### P5 — Verify embed-model consistency (ingest vs query)
- Ingest uses `system.json → embeddings_model`; queries use `embed_question`
  (`core/retrieval/semantic.py`). Both `bge-large` and `nomic` are loaded on the server.
  If these ever differ, vector space mismatch silently destroys retrieval.
- **Fix:** assert a single source of truth for the embed model across ingest + query.

---

## Recommended sequence

1. **Measure** — run `diag_truncation.py`; confirm the P0 magnitude. (no code change)
2. **P0 + P3: chunking** — introduce token-aware chunking for entity rows and
   doc/PDF blocks. Biggest single recall win.
3. **P1 + P2: schema guardrails + non-lossy NLP text** — protects embedded-text
   completeness. This is where the existing schema-inference work lands.
4. **P4: metadata** — `kb_tags` via the `tags` role; clean headings. Unblocks CV-02.
5. **CV-01..04 + CL-01/02/04** — the original tickets, now operating above a raised
   ceiling so their gains actually show up.
6. **P5: embed-model consistency check** — quick assertion / config audit.

## How the original tickets fold in
- **CV-01** benefits directly from P3 (clean, heading-aligned chunks).
- **CV-02** depends on P4 (`kb_tags` surfaced).
- **CV-03/04** unchanged (TF-IDF + LLM labels), but cluster quality improves once
  chunks aren't truncated (P0).
- **CL-01/02** unchanged (precision tweaks on mention matching).
- **CL-04** benefits from P4 (cleaner identifiers/headings → better wikilink targets).

## Decisions made
- **Sequencing:** ingestion redesign first — P0 → P1/P2 → P4 → then CV/CL tickets.
- **Entity-row chunking scheme (resolved):** keep `identifier` = the article's id on
  every chunk (NO `_chunk_N` suffix, NO new article_id). Storage id is
  `collection:filehash:seq`, so chunks sharing an identifier don't collide. Downstream
  lookups (`get_by_identifier`, cross-link exact match, `link_keys`) work unchanged and
  simply return all of an article's chunks. Add `chunk_index`/`chunk_total`; repeat the
  article title in each chunk for self-contained embedding.
- **Chunk sizing:** ~2,000 chars / ~400 tokens target (under the measured 2,500 cap),
  ~150-char overlap, split on paragraph/sentence boundaries.

## Open decisions for Raed
- Re-ingest scope: chunking + schema changes require re-ingesting affected collections
  (kb_docs at minimum; obsidian/pdf if we change doc chunking).
- Schema inference trigger: always-run LLM + guardrails vs entity_row-only (pending the
  multi-file test results).
- Whether to apply the char cap to doc/PDF block-chunks in P0 (closes obsidian/pdf
  truncation, ~3–5% of those chunks) or defer to a later phase.
