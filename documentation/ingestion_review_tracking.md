# Ingestion Review Tracking

This document tracks ingestion weaknesses, suggested fixes, and implementation status.

Status values:

* Not Started
* In Review
* In Progress
* Done
* Deferred

---

## 1. Core ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| CORE-001 | core/ingest_collection.py | Core injects filetype-specific config for images and docs. | Move filetype defaults into config/filetypes.json and merge config generically. | High | Done |
| CORE-002 | core/ingest_collection.py | filetypes.json is loaded multiple times during task building. | Load filetypes once and pass through helper functions. | Medium | Done |
| CORE-003 | config/filetypes.json / registry_setup.py | Audio/video are configured but parsers/serializers are not registered. | Disable audio/video or skip unregistered filetypes until implemented. | High | Done |
| CORE-004 | core/orchestrator.py | merge_collection_docs runs for all filetypes. | Make merge behavior type-safe and eventually configurable per filetype. | High | Done |
| CORE-005 | core/collection_state.py | File state key uses filename only, which can collide across folders. | Use normalized source path or path hash as state key. | Medium | Done |
| CORE-006 | core/qdrant_client.py | Delete-before-upsert depends on inconsistent source_file metadata. | Standardize payload metadata and delete key. | High | Done |
| CORE-007 | core/collection_merger.py | Merge strategies are not configurable per filetype. | Add filetype-level merge_strategy later. | Medium | Not Started |
| CORE-008 | UI validation | Ingestion has no generic validation dashboard for schemas and Qdrant payloads. | Add validation tab for schema and payload checks across filetypes. | High | Done |
| CORE-009 | path handling | Some scripts use relative paths like config/ and schemas/ which depend on launch folder. | Centralize project-root path resolution and use it across config/schema/state files. | High | Done |
| CORE-010 | UI validation | Payload Inspector hides link_keys and related_link_keys in raw payload only. | Add link_keys and related_link_keys columns to the Payload Inspector table. | Low | Done |
| CORE-011 | Query router modularization | query_router.py is too large and mixes routing, synthesis, scoring, discovery fallbacks, and legacy helpers. | Split into structured_lookup, relationship_lookup, synthesis, and scoring modules after XML regression is stable. | Medium | Not Started |
| CORE-012 | Legacy test scripts | core/test_upload.py contains hardcoded local FIX paths and is not part of orchestrated ingestion. | Move to archive. | Low | Done |
| CORE-013 | collection_merger structured overwrite | collection_merger.py re-merges structured docs and destroys normalized payload fields, producing old identifiers like identifier:RQ472. | Disable structured/entity/procedural re-merge; keep only image relationship post-processing until merger is redesigned. | Critical | Done |
---

## 2. XML ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| XML-001 | XML/xml_serializer.py | Serializer stores batch state on function attributes. | Replace with explicit batch context or orchestrator-supported finalize flow. | Critical | Not Started |
| XML-002 | XML/xml_serializer.py | expected_files is not clearly set by core. | Core should pass expected file count or batch info explicitly. | Critical | Done |
| XML-003 | XML/xml_parser.py | Row tag is auto-detected only by most frequent tag. | Add optional row_tag override in template config. | Medium | Not Started |
| XML-004 | core/link_index.py | Enum linking depends heavily on schema inference. | Add regression checks for tag enum questions before changing logic. | Critical | Done |
| XML-005 | XML schema/linking | XML files with different row meanings share identifier values and collide. | Add namespace-aware identifiers using identifier_field and identifier_namespace. | Critical | Done |
| XML-006 | XML serializer/linking | Field definitions and enum values from separate XML files were stored as split payloads. | Use batch finalization to assemble same-entity XML records before embedding. | Critical | Done |
| XML-007 | XML relationship linking | Same-file and same-collection relationships, such as tag 48 referencing tag 22, are not namespace-aware. | Add link_keys and related_link_keys without merging separate entities. | High | Done |
| XML-008 | XML/xml_ingestion.py | Legacy standalone XML ingestion script bypasses orchestrator and contains hardcoded paths/Qdrant URL. | Confirm unused, then move to archive. | Medium | Done |
| XML-009 | XML relationship validation | related_link_keys are extracted from any matching identifier number in description text, which may create false positives. | Add relationship confidence / relation_source / relation_text metadata and review filters. | Medium | Not Started |
| XML-010 | XML relationship validation | related_link_keys are extracted from any matching identifier number in description text, which creates false positives. | Require explicit reference patterns such as “tag 22”, “field 22”, “(22)”, or known field-name references before creating related_link_keys. | High | Done |
| XML-011 | Structured enum normalization | enum_values preserve source column names, forcing retrieval to guess keys like Value/SymbolicName. | Normalize enum entries during ingestion using schema roles into enum_value / enum_name / description keys. | High | Done |

---

## 3. Table ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| TBL-001 | TABLES/table_detector.py | Detector hardcodes KB/Halo columns. | Replace with schema/template-driven detection. | Critical | Done |
| TBL-001b | TABLES/table_detector.py | Generic detector classifies BBG-style reference tables as entity_row because they have identifier/name/description but no enum/reference fields. | Add schema-driven text-density heuristic: long article-like descriptions imply entity_row; compact identifier/name/description records imply structured. | High | Not Started |
| TBL-002 | TABLES/table_parser.py | Header detection is heuristic only. | Add optional header_row override in template config. | Medium | Done |
| TBL-002b | TABLES/table_serializer.py | Entity-row table payloads do not emit identifier_field, identifier_namespace, link_keys, related_link_keys, and file_path. | Apply the same normalized identity fields to _build_entity_row_doc(). | High | Done |
| TBL-002c | TABLES/table_serializer.py / retrieval | KB-style entity rows use source IDs that are not natural user query targets. | Ensure entity-row retrieval prioritizes primary_name, aliases, and description/text over identifier lookup. | High | Done |
| TBL-002d | TABLES/table_serializer.py | Table payloads do not distinguish canonical/user-facing identifiers from source/system IDs or generated row IDs. | Add identifier_kind: structured=canonical, entity_row=source, procedural=generated. | High | Done |
| TBL-003 | TABLES/table_serializer.py / link logic | Tables do not yet build same-file related_link_keys. | Reuse core.link_index for canonical structured table docs only; skip entity_row/source IDs and procedural/generated IDs. | High | In Progress |
| TBL-004 | TABLES/table_serializer.py | Table behavior depends on detector classification quality. | Improve detector before serializer changes. | High | Superceded |
| TBL-005 | TABLES/table_parser.py | Header-row detection is heuristic and may choose the wrong row for messy spreadsheets. | Add optional template override for header_row_index. | Medium | Not Started |
| TBL-006 | TABLES/table_parser.py | CSV/XLS parsing handles basic files but not multi-sheet workbook selection. | Add optional sheet selection / all-sheets mode for Excel. | Medium | Not Started |
| TBL-007 | Entity-row relationship model | Entity-row records use source IDs, so identifier-based related_link_keys are not useful for user-facing relationships. | Define entity-row relationship logic based on primary_name, aliases, tags, explicit article references, and optional source-system related article fields. | Medium | Not Started |
| TBL-008 | Entity-row related article extraction | KB-style rows may mention other articles or steps by title, but current table linking skips entity_row documents. | Build related_link_keys for entity_row only from safe signals such as explicit title references, shared workflow prefixes, or configured relationship columns. | Medium | Not Started |

---

## 4. Document ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| DOC-001 | DOCS/doc_parser.py / doc_serializer.py | Docs default to narrative because doc_type is not clearly detected. | Add DOCS/doc_detector.py or equivalent doc_type classification. | High | Not Started |
| DOC-002 | DOCS/doc_parser.py | Parser mixes text parsing, block detection, image resolution, and OCR enrichment. | Later split responsibilities if needed. | Medium | Not Started |
| DOC-003 | DOCS/doc_parser.py | Asset search roots are passed by core special case. | Move to generic config merge. | High | Not Started |

---

## 5. Image ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| IMG-001 | core/ingest_collection.py / IMAGES/image_parser.py | OCR is enabled through core special case. | Move enable_ocr to filetype config. | High | Not Started |
| IMG-002 | IMAGES/image_detector.py | Detector uses fixed heuristic thresholds and keyword lists. | Keep for now; make configurable later only if needed. | Low | Deferred |

---

## 6. PDF ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| PDF-001 | PDF/pdf_detector.py | Readable vs scanned threshold is hardcoded. | Keep for now; expose config later only if needed. | Low | Deferred |
| PDF-002 | PDF/pdf_serializer.py | Chunking behavior is local to serializer. | Accept for now; align later with standard chunk config if needed. | Low | Deferred |

---

## 7. Astro ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| ASTRO-001 | ASTRO/astro_parser.py | Astro metadata keys and filename parsing are hardcoded. | Accept as specialized plugin for now; move mappings to config later. | Medium | Deferred |
| ASTRO-002 | ASTRO/astro_serializer.py | Structured astro docs do not have stable identifiers. | Consider file path or metadata-based identifier later. | Medium | Not Started |

---

## 8. Regression checks needed

| ID | Area | Check | Status |
|---|---|---|---|
| REG-XML-001 | XML | What values can tag 22 have? | Not Started |
| REG-XML-002 | XML | What is tag 55? | Not Started |
| REG-XML-003 | XML | What does SecurityIDSource mean? | Not Started |
| REG-TBL-001 | Tables | Bloomberg field lookup works. | Not Started |
| REG-TBL-002 | Tables | KB article lookup works. | Not Started |
| REG-TBL-003 | Tables | KB procedural resolution lookup works. | Not Started |
| REG-DOC-001 | Docs | Procedural markdown note is classified correctly. | Not Started |
| REG-DOC-002 | Docs | Reference markdown note is classified correctly. | Not Started |
| REG-IMG-001 | Images | Screenshot OCR works. | Not Started |
| REG-PDF-001 | PDF | Readable PDF ingestion works. | Not Started |
| REG-PDF-002 | PDF | Scanned PDF OCR ingestion works. | Not Started |

---

## 9. Post-ingestion linking

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| LINK-001 | Cross-filetype linking | Payloads from docs, tables, XML, logs, and tickets are not linked after ingestion. | Add generic link_keys and related_link_keys metadata model. | High | Not Started |
| LINK-002 | Cross-collection linking | Related entities across collections cannot be discovered or expanded reliably. | Build post-ingestion crosslink index across collections using link_keys. | High | Not Started |
| LINK-003 | Retrieval expansion | Retrieval does not yet expand from one entity to linked chunks/docs/logs/tickets. | Add retrieval-time expansion using related_link_keys and crosslink index. | High | Not Started |

---

## 10. Retrieval / Ask routing

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| RET-001 | Structured namespace lookup | Structured asks like “what values can tag 22 have?” were routed to discovery and mixed entities by plain identifier. | Detect explicit namespace + identifier, fetch by identifier + identifier_namespace, and synthesize from payload. | Critical | Done |
| RET-002 | Reverse enum lookup | Asks like “what tag can have value CUSIP?” do not reliably search normalized enum_values. | Search enum_values by enum_value, enum_name, and description, then return the owning structured payload. | High | Done |
| RET-002b | Reverse enum candidate cleanup | Reverse enum questions like “what tag contains principal” left trigger/namespace words in the candidate, causing fallback to semantic/discovery. | Use config-driven cleanup via discovery_noise_words, enum_lookup_query_terms, and structured_namespace_terms. | High | Done |
| RET-003 | Relationship lookup | Asks like “what is related to tag 22?” do not use link_keys / related_link_keys. | Fetch base payload, then return forward and reverse related payloads using link_keys and related_link_keys. | High | Done |
| RET-004 | Structured answer formatting | Structured answers previously used collection-level schema labels, causing wrong labels like FIXMLFileName / CategoryID. | Prefer payload fields such as identifier_field, identifier, primary_name, description, and normalized enum_values. | High | Done |
| RET-005 | Structured primary-name lookup | Asks like “what values can tag SecurityIDSource have?” do not resolve the structured payload by primary_name. | Detect structured value/name questions, search primary_name/aliases using n-gram spans from the question, and synthesize from the matched payload. | High | Done |
| RET-006 | Fuzzy structured name lookup | Misspelled names like “seucityIdSource” do not resolve to SecurityIDSource. | Add conservative fuzzy matching over primary_name and aliases. | Medium | Not Started |
| RET-007 | Entity/procedural reranking | Entity-row/procedural KB articles should be retrieved by topic/action/title, but final reranker can promote a neighboring article with stronger body keyword overlap over the semantically best action-specific article. | Adjust reranker for entity_row/procedural docs to preserve strong semantic rank and boost primary_name/action-term overlap. | High | Not Started |
| RET-008 | Structured primary-name token matching | BBG-style structured rows are found by description terms like “ask price” but not by compact primary_name queries like “px ask” matching PX_ASK. | Normalize separators/underscores and boost token-overlap matches against primary_name/aliases for structured/canonical rows. | High | Not Started |

---

## 11. Discovery harcoding fix

| ID | Area | Issue | Suggested Fix | Priority | Status |
| DISC-001 | Discovery intent terms | detect_ask_intent() hardcodes count/list/comparison/distinct-value query terms. | Move terms to config/doc_query_hints.json and read them dynamically. | High | Done |
| DISC-002 | Discovery role field resolution | discovery_engine.py hardcodes role_to_payload_fields for exposure/rotation/filter/object/date. | Add schema-driven resolve_payload_fields_for_role(collection, requested_role), then replace hardcoded dictionaries in match and distinct-value discovery. | Critical | Done |
| DISC-002b | Role-specific query cleanup | parse_structured_filter_query() has special handling for requested_role == "exposure". | Move role-specific cleanup terms to config or remove once schema-driven role parsing is stable. | Medium | Not Started |
| DISC-003 | Discovery operator parsing | parse_structured_filter_query() hardcodes query operators like greater than, less than, after, before. | Move operator phrases to config/query_operators.json after DISC-002 is stable. | Medium | Done |
| DISC-003b | Symbol operator matching | Operator parser may not match symbol-only terms like >=, <=, >, < because the regex uses word boundaries. | Adjust pattern generation based on whether the operator term is alphanumeric or symbolic. | Low | Not Started |
| DISC-004 | LLM query parser fallback | Discovery filters currently rely on configured operator terms only. | Add optional LLM fallback that outputs role/operator/value JSON, validated against schema roles and allowed operators. | Medium | Not Started |


---

## 12. UI

| ID | Area | Issue | Suggested Fix | Priority | Status |
| UI-002 | Validation payload inspector | Inspector only searches by identifier, which is weak for entity_row/source-ID records. | Allow lookup by identifier, primary_name, and link_keys. | Medium | Not Started |
| UI-003 | Related articles panel | Ask tab no longer shows related articles after disabling legacy collection_merger entity-row merge. | Rebuild related articles using normalized payload fields and a proper entity_row relationship model instead of old fuzzy merger. | Medium | Not Started |

---

## 13. Ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
| ING-001 | Force re-ingest cleanup | Force re-ingest reprocesses files but can leave stale vectors when chunk IDs or payload shape changes. | When force_reingest=True, delete/recreate the target Qdrant collection before ingesting configured files. Normal ingestion should skip unchanged files and add/update only needed files. | Critical | In Progress |
| ING-002 | Normal ingestion skip check | collection_state.py recorded file state but orchestrator did not use it, so normal ingestion reprocessed unchanged files. | Add should_skip_file() and skip unchanged files when force_reingest=False. | Critical | Done |





