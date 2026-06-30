Table of Contents {#table-of-contents .TOC-Heading}
=================

NAS-AI
======

**Offline AI Knowledge Retrieval System** --- *Technical Architecture,
Methodology & Operations Manual*

  Field                    Value
  ------------------------ ----------------------------------------------------------------------------------------------------------------------------------
  **Version**              Phase 3+ / Ingestion Hardening + NiceGUI & Analytics --- June 2026 (v6)
  **Platform**             Mac M1 Pro + TerraMaster NAS (TNAS-RN)
  **LLM model**            qwen2.5-14b-instruct-1m (default) --- replaced meta-llama-3.1-8b-instruct
  **Embeddings**           text-embedding-bge-large-en-v1.5 (1024 dims, \~512-token / \~2,500-char window)
  **Active Collections**   xml\_test, bbg\_fields, kb\_docs, recon\_assist\_file, obsidian, pdf\_test, docx\_test, image\_test, astro\_test, astro\_catalog
  **Tailscale IP**         100.123.16.57 (NAS)
  **Classification**       Confidential --- Internal Use Only

*Change log (v6): NiceGUI front-end migration (coexists with Streamlit;
reliable inline images via direct DOM control); guarded local
text-to-SQL analytics engine for metadata/aggregate questions + SQL
Inspector tab + router analytics intent; Chat now implemented with an
LLM contextualizer (follow-up detection + standalone-query rewrite
replacing bracket injection) and a fixed faithfulness guard;
discovery/list results rendered as a table instead of raw JSON;
collection lists now include configured-but-not-yet-ingested collections.
All UI prompts/logic kept domain-agnostic (no hardcoding).*

*Change log (v5): embedding-truncation fix via token-aware chunking; LLM
schema inference with structured output, generic tags role, cross-role
dedup, signal-based column pruning and prose-promotion; content-priority
table-type detection; non-lossy NLP text; concept-vector grouping tiers
+ LLM topic/cluster labels; cross-link precision (min length 8,
meaningful-context gate) and one-hop wikilink traversal; default model
switched to qwen-14b.*

1. System Overview
==================

NAS-AI is a fully offline, local AI knowledge retrieval system. It
answers natural language questions about ingested documents --- FIX
protocol dictionaries, Bloomberg field definitions, KB articles, RECON
file mappings, Obsidian notes, PDFs, images, DOCX files, and
astrophotography metadata --- without any connection to external AI
services.

**Standing constraint: NAS-AI is fully local. No data leaves the local
network. No external AI or cloud model connections, ever.**

1.1 Infrastructure Components
-----------------------------

  Component                   Details
  --------------------------- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------
  Mac M1 Pro                  Runs LM Studio (qwen2.5-14b + BGE embeddings), Streamlit UI, Python ingestion pipeline
  TerraMaster NAS (TNAS-RN)   Runs ai-pgvector container (PostgreSQL + pgvector), pgAdmin, Tailscale, Portainer
  LM Studio                   Serves local embeddings (text-embedding-bge-large-en-v1.5, 1024 dims) and LLM (qwen2.5-14b-instruct-1m). Honors response\_format json\_schema for structured output.
  PostgreSQL / pgvector       Primary store for all chunks, embeddings, cross-links, concept vectors. Port 5433, db=nasai.
  Tailscale                   Remote access to NAS via IP 100.123.16.57
  Streamlit                   Web UI for ingestion, querying, validation, cross-link review, collection management

1.2 Design Principles
---------------------

-   Fully offline --- no data leaves the local network, no external AI
    or cloud connections
-   Domain-agnostic --- new collections add rows, not tables or code
-   No hardcoding --- domain logic lives in config or the database, not
    in Python; column roles are inferred from content, not column names
-   LLM-driven --- natural language routing and schema classification
    handled by the local LLM (qwen-14b), not regex patterns; structured
    output guarantees valid JSON
-   Four-stage retrieval pipeline --- intent classification, retrieval,
    reranking, synthesis are separate concerns
-   Token-aware ingestion --- no chunk exceeds the embedding window, so
    no content is silently dropped at embed time
-   Shared payload schema --- all serializers produce category,
    folder\_path, primary\_name\_field, type\_field enabling
    cross-collection linking
-   Every change covered by smoke tests before and after

2. Retrieval Methodology: The Four-Stage Pipeline
=================================================

  Stage     Description
  --------- ---------------------------------------------------------------------------------------------------------
  Stage 0   LLM Intent Classification --- qwen-14b classifies intent AND extracts role/target for discovery queries
  Stage 1   Hybrid Retrieval (RRF) --- PostgreSQL BM25 + pgvector + trigram → top-25 via Reciprocal Rank Fusion
  Stage 2   Semantic Reranking (BGE) --- BAAI/bge-reranker-large cross-encoder → top-5
  Stage 3   Answer Synthesis --- structured formatter (answer.py) renders labeled key-value pairs

2.1 Stage 0: LLM Intent Classification
--------------------------------------

qwen-14b classifies intent mode AND extracts role and target for
structured discovery routing. Replaced field\_maps.json keyword matching
and query\_terms.json intent\_routing (both retired).

  Intent             Description                                            Action
  ------------------ ------------------------------------------------------ ---------------------
  answer             Single record or procedural. Singular 'tag'/'field'.   Stage 1 RRF
  discovery\_list    Plural subject, 'all', 'show me', 'list'.              Discovery query
  discovery\_count   Counting query: 'how many fields...'                   Discovery / analytics
  comparison         Comparing two items                                    Comparison pipeline

*Discovery\_count / discovery\_list questions are first offered to the
analytics engine (§2.6): genuine metadata/aggregate questions ("how many
files", "how many fits files", "tickets closed in December") are answered
by guarded text-to-SQL; content-based discovery falls through to the RRF
discovery path.*

2.2 Stage 1: Hybrid Retrieval via RRF
-------------------------------------

  Signal              Technology                           Strength
  ------------------- ------------------------------------ ------------------------------------------------
  BM25 (keyword)      PostgreSQL tsvector + ts\_rank\_cd   Exact token matching + synonym expansion
  Vector (semantic)   pgvector HNSW cosine, BGE 1024-dim   Concept similarity without exact token overlap
  Trigram (fuzzy)     pg\_trgm GIN on primary\_name        Character-level name similarity

2.3 Stage 2: BGE Cross-Encoder Reranking
----------------------------------------

  Setting                Value
  ---------------------- ------------------------------------------------------------------------------
  Primary reranker       BAAI/bge-reranker-large (1.3GB)
  Applied to             entity\_row, procedural, reference, mixed --- NOT structured (FIX/BBG/RECON)
  Top-K input / output   Top-25 from Stage 1 → Top-5 to Stage 3

2.4 Stage 3: Answer Synthesis
-----------------------------

  Doc Type                            Answer Format
  ----------------------------------- -------------------------------------------------------------------------------------------------------
  structured (FIX/BBG/RECON)          Labeled key-value pairs using original schema field names (description\_fields)
  entity\_row (KB)                    Source label + title + description + resolution steps (resolution guaranteed in the description role)
  chunked doc (PDF, DOCX, Obsidian)   Source label + section heading + content text + inline images with expandable OCR
  image                               Source label + image filename + caption / OCR text
  astro                               Source label + target + RA/Dec + camera + exposure + metadata

2.5 Cross-Collection Enrichment (Phase 3.5)
-------------------------------------------

When a query returns a result, NAS-AI enriches the answer with related
content from other collections using three mechanisms: confirmed exact
cross-links, one-hop wikilink traversal from those links, and
concept-vector similarity.

### 2.5.1 Exact Cross-Links

Pre-built confirmed relationships in the cross\_links table, built via
exact-identifier, trigram name-similarity, and mention matching with
confidence-tiered storage (≥0.9 auto-confirmed; 0.3--0.9 pending review;
\<0.3 skipped).

### 2.5.2 Wikilink One-Hop Traversal (CL-04, NEW)

-   From each confirmed first-hop cross-link target, the router follows
    one additional hop of confirmed links in the cross\_links table.
-   Enables short chains such as gsact.txt → 4.2 → 4.3.1 across linked
    notes.
-   Second-hop sections are deduped, never loop back to the source, are
    tagged match\_type=wikilink\_hop, and have confidence dampened ×0.9.
-   Payoff scales with how many confirmed wikilinks exist (populated by
    the related-titles linker / planned NER pass).

### 2.5.3 Concept Vector Similarity

Semantic cross-collection discovery using HDBSCAN clustering over BGE
embeddings. The matched answer chunk's cluster centroid is compared
against all other collections' centroids; related clusters surface as
expandable "Related Topics" sections. See §5 for how clusters are
grouped and labeled.

### 2.5.4 Ask Tab Controls

-   Show exact cross-links --- surfaces confirmed exact/name
    relationships (and their one-hop wikilink targets).
-   Show related topics --- surfaces concept-vector similarity matches.

2.6 Analytics: Guarded Local Text-to-SQL (NEW)
----------------------------------------------

Semantic retrieval answers "what does X say" but cannot answer
"how many / which / count where" --- those are aggregate/metadata
questions over the database itself. core/retrieval/analytics.py adds a
fully local text-to-SQL path for them, dispatched from the router when a
count/list intent is a genuine metadata question.

-   **Live schema introspection** --- tables + columns are read from
    information\_schema, and per-collection payload keys, doc/source
    types and filetypes are sampled from the data, then handed to the
    LLM. No column/entity names are hardcoded, so it adapts as
    collections change.
-   **Structured generation** --- qwen returns
    {is\_analytics, sql, explanation} via response\_format json\_schema.
    is\_analytics=false (e.g. a single-record lookup or field-content
    search) makes the router fall back to the normal path.
-   **Guardrails** --- generated SQL must be a single SELECT/WITH
    statement against a fixed table whitelist (chunks, files,
    collections, enum\_values, concept\_vectors, cross\_links,
    background\_tasks); forbidden keywords/functions (INSERT/UPDATE/
    DELETE/DDL, pg\_sleep, COPY, dblink, …) are rejected; a LIMIT is
    auto-added to non-aggregate queries.
-   **Read-only execution** --- runs in a READ ONLY transaction with a
    statement timeout and a row cap, then rolls back (never commits).
-   **Scoping** --- defaults to the selected collection unless the
    question says "all collections / in total".
-   **Surfaced** in Ask/Chat (router-dispatched) and in the SQL
    Inspector tab (NL→editable SQL→run, plus a raw read-only SQL box).

2.7 Chat: Multi-Turn Contextualization (NEW)
--------------------------------------------

The Chat tab (core/chat\_engine.py) adds conversational, multi-turn
retrieval over the same pipeline. Each turn: classify chat-vs-retrieval
intent → contextualize → route to 1--3 collections → retrieve in
parallel → synthesize a grounded answer with optional related sections.

-   **LLM contextualizer** (replaces bracket injection) ---
    contextualize\_query() returns {is\_followup, standalone\_query}. A
    new/standalone question passes through UNCHANGED; a genuine follow-up
    ("what about the other one?") is rewritten into a self-contained
    query by pulling only the missing subject from recent history. The
    classifier defaults to standalone to avoid false-positive follow-ups,
    and never dumps prior answer text into the query --- which was the
    root cause of "senseless second answers".
-   **History gating** --- prior turns are passed to collection routing
    and answer generation ONLY when the turn is an actual follow-up, so a
    previous topic can't pollute a new question.
-   **Grounded synthesis + faithfulness guard** --- answers are built
    from retrieved data verbatim; a key-term faithfulness check falls
    back to the raw retrieved answer if the LLM drifts. (A v5-era
    NameError that silently disabled this guard is fixed in v6.)
-   **Fast rewrite model (optional)** --- local\_llm.rewrite\_model /
    fast\_model points the rewrite step at a smaller model; defaults to
    the main model.
-   **Foundation for memory/learning (planned)** --- the standalone
    query + is\_followup flag are the hooks persistent sessions, rolling
    summaries and a rated-Q&A "fast lane" will build on (§10.3).

3. Database Schema
==================

PostgreSQL with pgvector is the sole primary store. The schema is
generic --- domain knowledge lives in the payload JSONB column and the
schema JSON stored in PostgreSQL.

3.1 Core Tables
---------------

  Table              Purpose
  ------------------ ------------------------------------------------------------------------------------------------
  chunks             All ingested content --- identifier, primary\_name, description, nlp\_text, embedding, payload
  enum\_values       Normalized enum values from structured records (FIX allowed values)
  files              Ingestion state per file --- path, hash, mtime, chunks\_created
  schemas            Schema role mappings per collection/source\_file (replaces disk JSON)
  cross\_links       Confirmed and pending cross-collection relationships with confidence scores
  concept\_vectors   HDBSCAN cluster centroids + anchor chunks per collection/group for semantic routing

3.2 Key Columns in chunks
-------------------------

  Column                             Description
  ---------------------------------- ---------------------------------------------------------------------------------------------------------------------------------------------
  collection\_name                   Which collection this chunk belongs to
  identifier                         Primary key value. For chunked entity rows, all chunks of one record SHARE the identifier (storage id is seq-based, so they don't collide).
  primary\_name / \*\_field          Human-readable name + original column name
  type / type\_field                 Type value + original column name
  category / folder\_path            Top-level folder + full relative folder path
  kb\_tags                           JSON list of tags surfaced from a tags-role column (e.g. KB kbtags). Drives concept grouping. **(NEW)**
  chunk\_index / chunk\_total        Position within a split entity-row record **(NEW)**
  chunk\_part / chunk\_part\_total   Position within a split doc/PDF block-chunk **(NEW)**
  doc\_type                          structured, entity\_row, procedural, reference, mixed, image, astro
  section\_heading                   Heading for chunked docs (docx/pdf) --- used as a concept grouping field
  nlp\_text / nlp\_text\_tsv         Full text for embedding + BM25. HTML-stripped, deduped, capped under the embed window.
  embedding                          1024-dim pgvector embedding from BGE
  payload                            JSONB: all original fields plus the above

3.3 cross\_links Table
----------------------

  Column                                   Description
  ---------------------------------------- -----------------------------------------------------------------------------------------------
  source/target\_collection + identifier   Source and target records (target may be identifier or source\_file for doc collections)
  match\_type                              exact\_identifier, name\_similarity, mention; wikilink\_hop is produced at query time (CL-04)
  confidence                               exact 1.0; similarity = trigram score; mention length-based 0.45/0.55/0.70
  status                                   confirmed, pending\_review, or rejected

3.4 concept\_vectors Table
--------------------------

  Column                                     Description
  ------------------------------------------ ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  collection / group\_field / group\_value   Grouping field (folder\_path, section\_heading, type, identifier\_namespace, source\_file, or kb\_tags) + the group value (a folder, heading, tag, or LLM-generated topic label)
  cluster\_id                                HDBSCAN cluster number; LLM-relabeled clusters use a high running id (≥900000) to avoid collisions
  centroid                                   pgvector(1024) --- average embedding of cluster members
  anchor\_chunk\_ids / anchor\_texts         Top-5 chunk IDs + text previews closest to the centroid

4. Ingestion Pipeline
=====================

4.1 Pipeline Stages
-------------------

  Stage                  Description
  ---------------------- ------------------------------------------------------------------------------------------------------------------------
  1\. File discovery     Scan collection path for allowed\_filetypes (recursive)
  2\. Change detection   Compare SHA256 hash against files table --- skip unchanged
  3\. Parsing            Convert raw file to content blocks; parser selected by filetype
  4\. Field filtering    Apply collection-level field filters (e.g. KBinactive=TRUE → exclude). Runs BEFORE schema inference and serialization.
  5\. Schema mapping     Load saved schema from PostgreSQL, else auto-infer (§4.2)
  6\. Serialization      Build chunk dicts; shared payload enrichment; token-aware chunking (§4.3); non-lossy text (§4.4)
  7\. Embedding          LM Studio embeddings API → 1024-dim BGE vectors (batched). Every chunk fits the window, so nothing is truncated.
  8\. PostgreSQL write   pg\_client.py upserts chunks, enum\_values, updates files table
  9\. Concept vectors    build\_concept\_vectors() clears stale rows, regroups, clusters, and labels (§5)

4.2 Schema Inference (Updated)
------------------------------

Schema inference maps each source column to a role (identifier,
primary\_name, aliases, description, type, tags, enum\_value,
enum\_name, reference\_identifier, other). It is heuristic-first with
LLM escalation, and now file-agnostic --- roles are decided by content,
not hardcoded column names.

-   **Cache-first**: a saved schema in the PostgreSQL schemas table is
    reused (delete it to force re-inference).
-   **Heuristic pass** (config/structured\_roles.json keyword patterns)
    runs first and is fast.
-   **LLM escalation** (qwen-14b) runs when the heuristic misses
    identifier/primary\_name OR when the table is detected as
    entity\_row (article-style) --- where tag detection and free-text
    roles benefit from the LLM.
-   **Structured output**: the LLM call uses response\_format
    json\_schema, guaranteeing valid JSON in the exact {role:
    \[columns\]} shape (no more parse failures).
-   **Generic tags role**: a content-based 'tags' role lets the model
    identify multi-value keyword columns (e.g. KB kbtags) in any file,
    surfaced into the kb\_tags payload field.
-   **Cross-role de-duplication**: structured output guarantees shape
    but not one-column-one-role, so each column is assigned to exactly
    one role by priority.
-   **Signal-based column pruning**: near-empty columns (e.g. blank
    date-matrix columns) are dropped before the LLM call so
    wide-but-real tables still get inference; pruned columns route to
    'other'.
-   **Wide-file safety net**: if a sheet is still very wide after
    pruning, inference falls back to the heuristic instead of risking a
    bad/oversized LLM call.
-   **Prose-promotion**: any 'other' column whose values are long
    free-text is promoted into 'description', so substantive content
    (e.g. a resolution column) lands in the role answers are built from
    --- regardless of heuristic/LLM quirks.
-   **Manual override** remains available via the Validation/Schema tab
    for edge cases.

*Table-type detection (entity\_row vs structured) is content-first: a
table whose rows carry long free-text (description median ≥ 500 or max ≥
1,500 chars) is entity\_row, even when it also has a reference/enum
column --- this check runs BEFORE the reference/enum rule. This is why
kb\_docs (long resolutions) is entity\_row while bbg\_fields (one-line
field labels) is structured, though both are tables.*

4.3 Chunking & the Embedding Window (NEW)
-----------------------------------------

The embedding model (bge-large) silently ignores input beyond \~2,500
chars (\~512 tokens) --- measured directly: a vector stops changing once
a long document's prefix passes \~2,500 chars. Before this fix, roughly
one KB article in three exceeded the cap, the worst losing \~78% of its
text from the embedding. Ingestion now chunks long content so every
vector fits the window.

-   **Shared splitter** (core/chunking.py): targets \~1,800 chars/chunk
    with \~150-char overlap, breaking on paragraph then sentence
    boundaries.
-   **Entity rows (P0)**: long records split into multiple chunks that
    SHARE identifier/primary\_name/source\_file/link\_keys/kb\_tags;
    each adds chunk\_index/chunk\_total and repeats the title so it
    embeds self-contained.
-   **Doc/PDF (P0b)**: oversized block-chunks are capped via the same
    splitter at the doc and pdf serializer outputs (covers txt, md, rtf,
    docx, pdf).
-   **Result**: 0 chunks over the cap across all collections (kb\_docs
    33.7% truncated → 0%).
-   **Not yet capped**: structured tables and standalone images ---
    their rows/text are short by nature, so no truncation occurs today;
    revisit if a huge single field ever appears.

4.4 Non-Lossy NLP Text (NEW)
----------------------------

-   All field text is HTML-stripped (entities unescaped, tags removed)
    and included rather than dropped on length or '\<' --- content is
    preserved, and length is handled by chunking, not by discarding.
-   **Redundant-field dedup**: a value that is a near-duplicate of one
    already included (e.g. a \*Markdown column mirroring its plain-text
    counterpart) is skipped, keeping the index lean.

4.5 FIX Version Consolidation (Phase 1b --- COMPLETE)
-----------------------------------------------------

-   merge\_rows\_by\_version() groups rows by Tag (fields) or (Tag,
    Value) (enums) across version files at finalize.
-   Latest-version-wins for canonical Name/Type/Description/SymbolicName
    on conflicts; full \_version\_history retained.
-   Verified: Tag 22 returns all 19 enum values (9 from FIX42 + 10 from
    FIX44).

4.6 Supported File Types
------------------------

  Filetype   Extensions                       Key Features
  ---------- -------------------------------- -----------------------------------------------------------------------------------------
  xml        .xml                             FIX batch processing, cross-file link index, FIX version merge, CamelCase tokenization
  tables     .csv, .xlsx, .xls                Content-based schema role mapping, tags detection, entity-row chunking, sheet selection
  docs       .txt, .md, .rtf, .docx           Obsidian embed resolution, front matter, embedded-image OCR, oversized-chunk cap
  pdf        .pdf                             Auto readable vs scanned; pytesseract OCR per page; oversized-chunk cap
  image      .png, .jpg, .jpeg, .webp, .tif   Auto doc\_type detection, OCR via pytesseract (enable\_ocr: true)
  astro      .fit, .fits, .fts, .xisf         FITS header extraction (RA, Dec, OBJECT, camera, exposure); XISF geometry only

5. Cross-Collection System (Phase 3.5)
======================================

5.1 Architecture Overview
-------------------------

### Layer 1: Exact Cross-Links (cross\_links table)

Built by core/cross\_link\_discoverer.py with three strategies:

-   **Exact identifier match** --- confidence 1.0 (skips plain short
    numeric IDs to avoid false positives).
-   **Name/primary\_name trigram similarity** --- confidence = trigram
    score (\>0.3).
-   **Mention matching** --- source identifier/type appears in target
    text. Precision hardened: minimum term length raised from 4 to 8
    (CL-01), and a meaningful-context gate (CL-02) requires the term to
    appear as a whole word with enough surrounding words --- not a bare
    list/reference entry. Confidence length-scaled 0.45/0.55/0.70;
    filtered by generic\_terms.

Manual review via Collections tab: Confirm / Reject / Reject+Ignore Term
(auto-adds to generic\_terms).

### Layer 2: Concept Vector Similarity (concept\_vectors table)

Built by core/concept\_vector\_builder.py. Grouping is data-driven and
tiered, then HDBSCAN clusters within each group, and generic labels are
upgraded via the LLM:

-   **Grouping field (non-entity\_row)**: folder\_path →
    section\_heading (docx/pdf, CV-01) → identifier\_namespace → type →
    category → source\_file, chosen by what the data actually has.
-   **Entity\_row grouping (CV-02/03)**: Tier 1 --- score kb\_tags by
    inverse document frequency and pick the most distinctive *shared*
    tag as the group value (no LLM). Tier 2 --- for chunks with
    no/generic tags, compute TF-IDF, take the top terms, and batch-call
    the LLM for a shared topic label.
-   **HDBSCAN clustering** with min\_cluster\_size adaptive to group
    size; multi-label assignment via cosine ≥ 0.75.
-   **CV-04** --- after clustering, any group value that is still a
    filename or generic value is relabeled by the LLM from the cluster's
    anchor texts, using a high running cluster\_id so relabeled clusters
    can't collide.
-   **Stale rows** for a collection are cleared before each rebuild, so
    old grouping fields / pre-relabel filenames don't linger.
-   **Anchor chunks**: top-5 most representative per cluster stored for
    reference.

### Layer 3: NER-Based Entity Extraction (PLANNED)

Future: LLM-based NER to extract domain entities (filenames, broker
names, job names) from text for precise obsidian→recon\_assist\_file
links (CL-03/CL-05). This is what the CL-04 traversal is waiting on to
show full effect.

5.2 Query-Time Enrichment Flow
------------------------------

-   identifier\_lookup and fallback paths both fetch confirmed exact
    cross-links first.
-   CL-04: follow one hop of confirmed wikilinks from each first-hop
    target.
-   Both paths: run find\_concept\_links() for semantic concept matches.
-   Results merged, deduplicated, sorted by confidence/similarity,
    rendered as expandable "Related from other collections" sections.

6. Collections
==============

  Name                  Type                 Chunks   Notes
  --------------------- -------------------- -------- ---------------------------------------------------------------------
  xml\_test             xml                  1164     FIX Protocol dictionary (4.2 + 4.4 merged)
  bbg\_fields           tables/structured    730      Bloomberg field definitions (test subset)
  kb\_docs              tables/entity\_row   349      HaloITSM KB. Chunked + kb\_tags surfaced (was 178, one-per-article)
  recon\_assist\_file   tables/structured    89       RECON Moore-PB mapping
  obsidian              docs                 904      Markdown vault; oversized chunks capped
  pdf\_test             pdf                  75       PortfolioOne whitepaper + OCR sample; chunks capped
  image\_test           image                3        Test images, OCR working
  docx\_test            docs                 11       OmniVista whitepaper; section headings
  astro\_test           astro                3        2 FITS + 1 XISF
  astro\_catalog        tables               13,336   OpenNGC + Messier merged

7. User Interface
=================

The UI is migrating from Streamlit to **NiceGUI** (core/ui\_app.py →
ui/app.py). Streamlit re-ran the whole script on every interaction and
issued N+1 DB calls over Tailscale, making it slow; NiceGUI controls the
DOM directly (which also fixes inline image rendering) and reuses the
same UI-agnostic data layer.

7.1 NiceGUI App (primary)
-------------------------

Run with: `python ui/app.py` from the nas-ai/claude directory, then open
http://localhost:8080 . Shares core/ logic via core/ui\_data.py (a
framework-agnostic data layer used by both UIs and any future API).

  Tab               Status        Description
  ----------------- ------------- -----------------------------------------------------------------------------------------------------------
  Collections       Done          Create/edit/delete collections; build cross-links + concept vectors; review pending cross-links
  Ask               Done          Natural language query; enrichment toggles; inline images; expandable related sections
  Chat              Done          Multi-turn conversational retrieval with LLM contextualization (§2.7)
  Ingestion         Done          Path check, scan-by-extension counts, run/force ingest with per-file results, live task status + stop
  Knowledge Graph   Done          ECharts cross-link graph; cross-link review; concept-vector cluster inspector
  SQL Inspector     Done          Analytics: NL→editable SQL→run (§2.6) + raw read-only SQL box
  Debug             Done          Query Debug (merged/reranked candidates + returned answer) + diagnostics reports
  Validation        Pending       Inspect payloads by identifier, sample, or BM25 search
  Preview           Pending       Browse collection contents
  System Config     Pending       Edit system.json: confidence threshold, embeddings, reranker
  Data Prep         Pending       CSV merge/clean/join with key normalization + fuzzy matching

Notes: tabs live inside a sticky header so they stay visible while
scrolling; discovery/list answers render as a table (not raw JSON);
collection dropdowns include configured-but-not-yet-ingested collections
(union of collections.json + DB + stored data).

7.2 Streamlit App (legacy, still runnable)
------------------------------------------

Run with: `streamlit run core/ui_app.py`. Retains the full tab set
(Collections, Ingestion, Validation, Ask, Preview, SQL Inspector, System
Config, Data Prep). Kept until the NiceGUI tabs reach parity, then
retired.

8. Configuration Files
======================

  File                            Key Settings
  ------------------------------- ------------------------------------------------------------------------------------------------
  config/system.json              pgvector connection, embeddings URL/model, bge\_reranker, retrieval\_confidence\_threshold
  config/nlp\_config.json         base\_url http://localhost:1234, model: qwen2.5-14b-instruct-1m, timeout; optional rewrite\_model / fast\_model for the chat contextualizer
  config/structured\_roles.json   Heuristic role→keyword patterns, including the generic 'tags' role (fallback to LLM detection)
  config/synonyms.json            qty↔quantity, exec↔execution, px↔price, pb↔prime broker, 1mad↔one madison
  config/doc\_query\_hints.json   stopwords, relationship/enum query terms, generic\_terms (cross-link noise filter)
  config/filetypes.json           enable\_ocr, asset\_search\_roots, batch\_finalize; serializer per extension
  config/collections.json         Per-collection path, allowed\_filetypes, field\_filters, sheet\_name

9. Smoke Tests & Verification
=============================

Smoke suites must pass before any commit. Ingestion changes are
additionally validated by the diagnostic scripts below (read-only unless
noted).

  Script                                             Checks
  -------------------------------------------------- ----------------------------------------------------------------------------
  diag\_truncation.py / diag\_truncation\_proof.py   Per-collection chunk lengths; measured embed-window cap
  diag\_schema\_guardrails.py                        LLM schema inference: dedup, pruning, wide-file fallback
  diag\_verify\_ingestion.py                         Post-ingest: truncation, kb\_docs schema/tags role, kb\_tags coverage
  diag\_verify\_concepts.py                          Rebuild + inspect concept vectors (CV-01..04); cross-link discovery sample
  diag\_analytics.py                                  Text-to-SQL end-to-end: schema context, generation, guardrails, scalar/group results
  diag\_chat\_scenarios.py                            Chat follow-up vs new-question: shows standalone-query rewrite, routing, and answer per turn
  smoke\_fix / bbg / kb / recon / obsidian           Domain ground-truth retrieval suites

10. Roadmap
===========

10.1b Completed (NiceGUI & Analytics --- v6)
--------------------------------------------

-   NiceGUI front-end (Collections, Ask, Chat, Ingestion, Knowledge
    Graph, SQL Inspector, Debug) with shared core/ui\_data.py layer and
    reliable inline images
-   Guarded local text-to-SQL analytics engine + SQL Inspector tab +
    router analytics intent
-   Chat tab with LLM contextualizer (follow-up detection + standalone
    rewrite) and history gating; faithfulness-guard NameError fixed
-   Discovery/list results rendered as a table; collection lists include
    configured-but-not-yet-ingested collections
-   Domain-specific examples removed from chat prompts (fully
    file-agnostic)

10.1 Completed (Ingestion Hardening --- v5)
-------------------------------------------

-   Token-aware chunking for entity rows + doc/PDF oversized-chunk cap
    (P0/P0b)
-   LLM schema inference: structured output, generic tags role,
    cross-role dedup, column pruning, prose-promotion, wide-file
    fallback (P1)
-   Non-lossy NLP text with redundant-field dedup (P2)
-   kb\_tags surfaced into payload; content-priority table-type
    detection
-   Concept vectors CV-01..04 (section\_heading grouping, kb\_tags IDF,
    TF-IDF+LLM topics, LLM cluster relabel, stale-row clearing)
-   Cross-link precision CL-01/CL-02; one-hop wikilink traversal CL-04
-   Default LLM switched to qwen2.5-14b-instruct-1m

10.2 Pending / Next
-------------------

-   Remaining NiceGUI tabs: Validation, Preview, System Config, Data
    Prep --- then retire Streamlit
-   NER-based obsidian→recon links (CL-03 / CL-05) --- unlocks full
    CL-04 payoff
-   Validation tab: show ALL source columns
-   Background-thread cross-link trigger after ingest
-   Full BBG dataset ingest (currently test subset)
-   Date normalization for date-typed fields (lands with ticket
    ingestion --- enables analytics date-range counts)

10.3 Phase 4+: MCP, Chat, Advanced
----------------------------------

-   MCP as ingestion source (Halo / Confluence / Snowflake) and as query
    interface
-   Chat memory + learning layer: persistent sessions (raw + rewritten +
    response), rolling summaries (keep last 3--5 turns raw), and a
    rated-Q&A "fast lane" (cosine ≥ ~0.92) with 👎 suppression and
    user-approved synonym learning --- built on the §2.7 contextualizer
-   Model routing (small for structured, larger for reasoning, vision
    for images)
-   Whisper transcription pipeline; astro catalog at scale with RA/Dec
    cross-linking
-   Deployment to Azure Virtual Desktop --- fully local, no external AI,
    ever

11. Key Learnings & Debugging Notes
===================================

11.1 Ingestion & Embedding
--------------------------

-   The embedding model silently truncates past \~2,500 chars --- long
    content must be chunked or it's invisible to retrieval. Measure the
    cap directly; don't trust token-usage reporting.
-   What gets embedded is the ceiling on recall: a field mis-mapped to a
    dropped role is lost. Non-lossy text + prose-promotion protect
    against this.
-   Entity-row chunks can share an identifier because the storage id is
    sequence-based --- no need for \_chunk\_N suffixes or a separate
    article\_id.

11.2 Schema Generation
----------------------

-   Structured output (response\_format json\_schema) guarantees JSON
    shape but NOT one-column-one-role --- always de-duplicate across
    roles afterward.
-   qwen-14b is reliable for schema classification with structured
    output; llama-3.1-8b dropped columns and over-assigned roles.
-   Prioritize by signal, not by column name: prune empty columns,
    promote long-prose columns --- keeps it file-agnostic.
-   Table-type detection must check the long-description signal BEFORE
    the reference/enum rule, or article tables with a creator-id column
    get mis-routed to structured.

11.3 Cross-Linking & Concept Vectors
------------------------------------

-   Short/generic mention terms produce false positives --- raise the
    minimum length and require meaningful context.
-   Concept vectors give far better precision than substring matching;
    HDBSCAN finds topics without labels; multi-label assignment handles
    hybrid chunks.
-   Rebuilds must clear stale concept vectors first, or old group values
    accumulate.
-   LLM cluster/topic labels need collision-safe cluster ids when
    multiple groups map to the same label.

11.4 Chat & Analytics (v6)
--------------------------

-   Injecting prior-turn text into the next query (bracket injection)
    pollutes retrieval and produces "senseless" answers on new
    questions. An LLM rewrite into a standalone query is far better ---
    but it must default to standalone, or it invents follow-ups and
    attaches the wrong subject.
-   Only feed conversation history to routing/generation on actual
    follow-ups; otherwise a previous topic leaks into a fresh question.
-   A debug print referencing undefined variables silently disabled the
    faithfulness guard for months (it crashed into the except branch).
    Gate debug prints behind the DEBUG flag and keep them out of
    return-critical paths.
-   Text-to-SQL is safe only behind hard guards: SELECT-only, single
    statement, table whitelist, forbidden-keyword block, read-only
    transaction with a timeout and row cap. Show the generated SQL so
    interpretation is auditable.
-   Aggregate queries double as a data-quality lens --- "count by type"
    immediately surfaced typo'd/duplicate values (Holdngs vs Holdings)
    and concatenated cells in the source data.

11.5 psycopg2 Gotchas
---------------------

-   \% in ILIKE '%term%' → IndexError; use %% in fetchall() calls.
-   JSONB returned as Python lists/dicts --- don't json.loads() again.
-   pgvector centroid stored as string --- pass directly, don't
    round-trip through json.

*NAS-AI Technical Manual • v6 • NiceGUI & Analytics • June 2026 •
Confidential*
