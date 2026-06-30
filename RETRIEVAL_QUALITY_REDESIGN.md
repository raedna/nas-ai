# Retrieval Quality Redesign

**Status:** agreed design, June 2026. Priority work — UI tabs and chat memory/learning
are deferred until this lands, unless they directly affect retrieval quality.

## Problem (observed)

Across several recon/FIX questions, answers were wrong, hedged, or dumped whole
documents. Root causes, from the example set:

1. **Whole-document return.** For doc collections the pipeline reassembles the full
   source file (`build_fuller_doc_payload`) and the grounded prompt says "present the
   retrieved data verbatim" — so a long article is dumped instead of answering the
   question. ("how to check if FIX server is running" returned several full articles.)
2. **Enrichment noise.** Low-precision concept-similarity "related" sections are
   appended (in chat, even merged into the answer body), cluttering the result.
3. **Cross-linking low value.** Mention + trigram link discovery produces mostly
   rejected, noisy links.
4. **Identifier hijack.** A detected filename short-circuits the real question — any
   question containing `gsact.txt` returns the gsact.txt record regardless of what was
   asked (sFTP, ask price…).
5. **Answer drops fields.** The structured renderer omits `aliases` (the PB filename).

## Target design

### 1. Noise removal (Step 1 — no rebuild)
- Chat: stop appending related previews into the answer body.
- Don't surface related sections by default; Ask toggles default off; concept-related
  threshold raised. Related, if shown, is a short collapsible list — never inline.

### 2. Doc-type-aware synthesis + aliases (Step 2)
- **Structured** (FIX/BBG/RECON): render fields verbatim **plus `aliases`** (PB filename).
- **Doc / procedural / entity_row**: lead with a concise answer to the question, then
  show the full entry with the relevant lines **highlighted** (bold), collapsible if
  long. "Return the whole entry but highlight the relevant parts."
- Stop concatenating multiple documents into one answer body.

### 3. Cross-linking confidence model (Step 3 — clear + rebuild `cross_links`)
- **Exact identifier** → auto-confirm, **1.0** — same *normalized* ID appears in the
  target's `identifier` **or `aliases`**.
- **Structured field reference** → auto-confirm, **0.9–1.0** — a source field
  (`reference_identifier` role) explicitly references a target ID/name. *(New; uses the
  schema role, so it stays file-agnostic.)*
- **Name / trigram** → **never auto-confirm alone**; pending only if corroborated by a
  shared signal (same `doc_type`, `category`, vendor/PB-type field, or alias overlap).
- **Mention in text** → never auto-confirm, never a cross-link; at most a weak,
  clearly-labeled "mentions" hint, off by default.

### 4. Identifier-hijack fix (Step 4)
- A detected filename must not short-circuit a question whose real target isn't in that
  record; let intent / retrieval drive when the asked field is absent.

## Principles (unchanged)
- No hardcoding — roles/signals inferred from content + schema, not entity lists.
- Fully local. Smoke tests pass before each commit. One step at a time.
