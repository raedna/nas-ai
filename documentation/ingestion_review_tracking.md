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

---

## 2. XML ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| XML-001 | XML/xml_serializer.py | Serializer stores batch state on function attributes. | Replace with explicit batch context or orchestrator-supported finalize flow. | Critical | Not Started |
| XML-002 | XML/xml_serializer.py | expected_files is not clearly set by core. | Core should pass expected file count or batch info explicitly. | Critical | Not Started |
| XML-003 | XML/xml_parser.py | Row tag is auto-detected only by most frequent tag. | Add optional row_tag override in template config. | Medium | Not Started |
| XML-004 | core/link_index.py | Enum linking depends heavily on schema inference. | Add regression checks for tag enum questions before changing logic. | Critical | Not Started |

---

## 3. Table ingestion

| ID | Area | Issue | Suggested Fix | Priority | Status |
|---|---|---|---|---|---|
| TBL-001 | TABLES/table_detector.py | Detector hardcodes KB/Halo columns. | Replace with schema/template-driven detection. | Critical | Not Started |
| TBL-002 | TABLES/table_parser.py | Header detection is heuristic only. | Add optional header_row override in template config. | Medium | Not Started |
| TBL-003 | TABLES/schema_inference_table.py | Global roles may overfit one dataset. | Support collection/template role overrides later. | High | Not Started |
| TBL-004 | TABLES/table_serializer.py | Table behavior depends on detector classification quality. | Improve detector before serializer changes. | High | Not Started |

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