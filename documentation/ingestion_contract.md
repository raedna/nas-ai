# Ingestion Contract

This document defines the expected responsibilities and data contracts for the ingestion pipeline.

The goal is to prevent filetype-specific fixes from breaking other filetypes.

---

## 1. High-level ingestion flow

ingest_collection.py
  -> discover files
  -> match filetype from config/filetypes.json
  -> build FileTask
  -> orchestrator.py
      -> parser
      -> detector / classification
      -> serializer
      -> optional merger
      -> embedder
      -> qdrant upsert

---

## 2. Core responsibility

Core files must remain generic.

Core may:

* discover files
* match file extensions to configured filetypes
* load config
* create ingestion tasks
* call registered parsers and serializers
* call embedding
* upsert vectors
* update collection state

Core must not:

* contain filetype-specific if filetype_name == ... logic
* know that images need OCR
* know that docs need asset search roots
* know table-specific or KB-specific column names
* know XML/FIX-specific field names
* modify behavior for one filetype in a way that affects all others

Filetype-specific behavior belongs in:

* config/filetypes.json
* collection config
* template config
* the relevant parser/detector/serializer module