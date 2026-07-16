# Retrieval Quality Redesign

**Status: DELIVERED — July 2026.** Every problem in this paper is fixed and verified
by the automated 50-question eval (tests/eval_runner.py; three baselines, latest
best-on-record). The design below was largely followed, then extended by a second
generation of mechanisms it did not foresee. See "Outcome" at the end, the Technical
Manual v8 (§2.7), and the Redesign Tracker for the full delivery record.

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

---

## Outcome (July 2026)

**Each numbered problem, as delivered:**

1. **Whole-document return** → doc answers get a concise LLM synthesis with verbatim
   quotes; the full entry lives behind "Show full entry" (Ask) and "Open full article ↗"
   (`/entry/{chunk_id}` pages, sibling chunks merged, images inline).
2. **Enrichment noise** → related sections ranked confirmed-first
   (exact/ner/wikilink → hop → similarity → concept), capped at 5, collapsible, never
   inline in the answer body.
3. **Cross-linking low value** → the confidence model landed as designed (mentions
   removed from the discoverer), then gazetteer NER made the links real: 95 confirmed
   mention-links (the first recon↔notes bridge) + 43 wikilinks, bidirectional lookups,
   CL-04 one-hop traversal live in answers.
4. **Identifier hijack** → `_record_covers_question` guard: a detected filename answers
   only when the question's focus terms exist in the record (values AND field labels,
   compact-matched) — "sftp for gsact.txt" routes to the procedure, "PB filename for
   gsact.txt" returns the record.
5. **Answer drops fields** → aliases rendered ("Also known as: …"), reference
   identifiers serialized + searchable, the lead sentence labels the name field
   ("gsact.txt — Tidal Job Name: …"), and every record is findable by ANY of its exact
   names (identifier, alias, reference).

**What the design didn't foresee — the second generation** (Manual v8 §2.7):
metadata SQL for aggregates with a dozen deterministic guards; the chat routing anchor
family (identifiers, code tokens, collection names, unique schema column names with
proportionality rules) + concept-centroid Tier 1.5 that skips the routing LLM on clear
margins; the answer-arbitration ladder (exact-key > name-hit > graded groundedness >
doc-title hits > routing order); "compose, don't blend" cross-collection record blocks;
typo-proof BM25 (vocabulary token filtering) and camelCase word-splitting; the
low-coverage honesty banner; schema inference rebuilt LLM-primary with structural
constraints and no junk persistence.

**The operating principle that emerged:** LLM for perception, math for constraints —
deterministic guards are no-ops when the model is right, and the model's variance
stops mattering when every choice it makes is grounded, gated, or repaired.

**Still open (tracked):** VOCAB-01 spell-correction (PP-03), PP-01 as the next
chapter's acceptance bar, heterogeneous multi-item splitting (MI-04/XC-03), Phase 1b
fix_version (AG-10), astro workflow profiles (DESIGN-01), SPEED-01.

---

# Addendum (July 2026): Beyond Retrieval --- Speed, Memory, and the Connector Era

The redesign above ended with a system that answered correctly. Three
arcs since have made it fast, personal, and self-feeding --- each
governed by the same doctrine the redesign established: **LLMs perceive;
deterministic structure decides.**

## Speed without regression

Latency work never touched answer quality machinery. The wins were
call-count and token-count: one fast-model front call replacing two
serialized 14B calls; identical per-collection intent calls memoized;
chatty per-word database lookups batched; schemas cached in-process;
throwaway "reason" tokens capped. Chat went from 137s average (1013s
worst) to ~24s (68s worst) with the eval suite byte-stable on every
structured answer. The enduring lesson from Apple-Silicon profiling:
concurrent LLM requests time-slice one GPU --- the only real levers are
fewer calls and fewer tokens.

The fast model earned a constitution of its own. Caught fabricating four
different ways --- mangling an echo, leaving a pronoun unresolved,
shrinking a rewrite to a fragment, and declaring a question standalone
while citing its missing subject --- it is now permitted to CLASSIFY but
never to ASSERT: every string it produces either passes a deterministic
gate (token-preservation, subset-detection, marker vetting) or is
escalated to the larger model. Nothing a small model says is taken on
faith, and nothing enters permanent state without a gate.

## Memory as a collection

The memory feature added no new answer path. A remembered fact --- typed
with a trigger phrase, stated bare (behind interrogative vetoes), or
saved from an answer --- becomes an ordinary chunk in an ordinary
collection, inheriting routing, retrieval, arbitration, vocabulary and
cross-links. Provenance is baked into the text itself, so no renderer
can present recollection as record. Notes ride answers as verbatim
"From memory" blocks --- anchored to what the QUESTION names, filtered
for relevance --- and when a note disputes a field of a structured
record, a red source-level alert names the field and the file to fix:
data stays the single source of truth; memory is the annotation layer
that pressures it to stay true.

Feedback closes the loop conservatively: thumbs verdicts form a
per-question prior that enters arbitration one rung above routing order
--- settling only the ties that were previously arbitrary, never
overruling a grounded answer. The system's first live act of learning
was a thumbs-down flipping the winner of a repeated question ---
observed in the arbitration debug as `feedback prior: {'kb_docs': -1}`.

## Tickets as data

The Halo ITSM connector proved the architecture's central bet: a wholly
new data source --- support tickets over OAuth2, with expiring-JWT
image links, status lookup tables, and email boilerplate --- ingested
through the EXISTING pipeline with zero changes to retrieval,
arbitration, or delivery. Tickets are parsed per-item (header + human
actions), each chunk carrying both faces: text for "how was this
fixed?", payload for "who resolved it and how many are open?". The
serializer DECLARES its schema --- the first collection where schema is
a fact rather than an inference --- and site vocabulary ('resolved'
means Closed) is declared in config, injected deterministically, and
subject to a retreat rule when a heuristic anchor contradicts a
grounded claim.

The loop this closes is the system's purpose in miniature: a ticket
arrives; triage asks whether the knowledge base already answers it; the
resolved ticket is normalized, ingested, cross-linked to the records it
mentions; and the next similar question is answered partly by the
support system's own history --- knowledge the organization generated
by doing its work, made retrievable the same afternoon.

## What the measurements refused to permit

Two honest negatives are part of this record. Embedding-based relevance
could not separate junk concept links from genuine ones (0.495--0.532
vs 0.506 --- no gap), so no threshold was invented; the measurement
harness now serves as the acceptance test for any future embedding
model (EMBED-01). And a proposed hardcoded synonym ('resolved' =
'closed') was redirected into declared configuration --- because the
system's guards had just demonstrated, via a Barclays-to-BOA
fabrication they correctly blocked, that undeclared semantic mappings
are indistinguishable from hallucinations.

