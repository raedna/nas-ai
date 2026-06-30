# Analysis Engine Design
**Project:** NAS-AI

---

# Vision

The Analysis Engine transforms NAS-AI from a knowledge retrieval platform into an intelligent analysis platform.

Where the Retrieval Engine answers:

> "What do I know about this?"

the Analysis Engine answers:

> "What is this, what happened, why did it happen, and what does it mean?"

The Analysis Engine is designed to analyze business and technical artifacts using the existing NAS-AI ingestion, knowledge graph, and retrieval infrastructure.

It does **not** replace the RAG.

It consumes it.

---

# Objectives

The Analysis Engine shall:

- Analyze structured and semi-structured business artifacts.
- Reuse existing OCR, parser, serializer, merger and enrichment pipelines.
- Reuse the Knowledge Graph and Retrieval Engine.
- Never duplicate parser or serializer logic.
- Never hardcode domain knowledge that already exists in the Knowledge Base.
- Produce deterministic business objects before invoking an LLM.
- Support multiple analyzer types through a pluggable registry.

---

# Guiding Principles

## Reuse Before Rebuild

Every analyzer must reuse the existing:

- OCR layer
- Parser Registry
- Serializer Registry
- Merge pipeline
- Knowledge Graph
- Retrieval Engine

The Analysis Engine exists **after** serialization.

---

## Domain Knowledge Lives in RAG

The Analysis Engine must never hardcode:

- FIX tag names
- FIX enum values
- Bloomberg field definitions
- XML schemas
- Halo ticket fields

These are retrieved from the Knowledge Base.

The Knowledge Base remains the single source of truth.

---

## Structured Before LLM

The LLM should never receive raw technical input.

Instead:

Input

↓

Business Object

↓

LLM Explanation

This guarantees deterministic interpretation while allowing natural-language explanations.

---

# High-Level Architecture

```
                    User Input
                         │
         ┌───────────────┼────────────────┐
         │               │                │
      Text          Screenshot          PDF
         │               │                │
         └───────────────┼────────────────┘
                         ▼
                     OCR (existing)
                         ▼
                Parser Registry (existing)
                         ▼
             Serializer Registry (existing)
                         ▼
                 Standard NAS-AI Payload
                         ▼
                 Knowledge Graph Lookup
                         ▼
                  Retrieval / Enrichment
                         ▼
                 Analyzer Registry
                         ▼
                  Selected Analyzer
                         ▼
                 Business Object Model
                         ▼
                 LLM Explanation Layer
                         ▼
                 NiceGUI Presentation
```

---

# Proposed Folder Structure

```
core/

    analysis/

        registry.py

        base_analyzer.py

        analysis_result.py

        analyzers/

            fix/

            xml/

            sql/

            halo/

            json/

            logs/

            generic/
```

---

# Analyzer Registry

The registry is responsible for:

- registering analyzers
- determining supported input types
- selecting analyzers
- exposing available analyzers to the UI

Example:

```python
AnalyzerRegistry.register(
    name="FIX",
    analyzer=FixAnalyzer,
    supported_inputs=[
        "text",
        "image",
        "pdf"
    ]
)
```

---

# Base Analyzer

Every analyzer implements the same lifecycle.

```python
class BaseAnalyzer:

    detect()

    preprocess()

    parse()

    serialize()

    enrich()

    analyze()

    explain()
```

Most analyzers will simply delegate:

- parse()
- serialize()

to the existing NAS-AI registries.

---

# Analysis Result Contract

Every analyzer returns the same object.

```python
AnalysisResult

title

summary

confidence

findings

warnings

entities

related_documents

related_concepts

timeline

business_object

raw_payload
```

The UI only understands this object.

The analyzer type is irrelevant to the presentation layer.

---

# Business Object

The Business Object is the canonical representation of an artifact.

Example (FIX):

```json
{
  "message_type": "ExecutionReport",
  "trade": {
    "side": "Sell",
    "instrument": "IBM",
    "quantity": 1000,
    "price": 183.25
  },
  "broker": "...",
  "custodian": "...",
  "status": "Filled"
}
```

The LLM receives this object rather than raw FIX tags.

---

# Analyzer Lifecycle

```
Artifact

↓

Detection

↓

Existing OCR

↓

Existing Parser

↓

Existing Serializer

↓

Knowledge Graph Lookup

↓

Analyzer

↓

Business Object

↓

LLM Explanation

↓

AnalysisResult

↓

NiceGUI
```

---

# Knowledge Graph Integration

Analyzers should leverage:

- Cross-links
- Concept vectors
- Retrieval Router
- Structured Retrieval
- Related documents
- Related entities

The Knowledge Graph provides context.

The Analyzer provides understanding.

---

# Planned Analyzers

## FIX

Business interpretation of FIX protocol messages.

Supported inputs:

- Raw FIX
- Pasted tables
- Screenshots
- PDFs

---

## Halo

Analyze tickets.

Explain:

- issue
- workflow
- SLA
- probable cause
- historical context

---

## SQL

Analyze SQL statements.

Explain:

- intent
- affected tables
- joins
- risks
- optimizations

---

## XML

Explain XML messages.

Map schemas.

Describe business meaning.

---

## Logs

Analyze:

- application logs
- system logs
- FIX logs
- API traces

Identify failures and probable root causes.

---

## Generic

Fallback analyzer.

Uses retrieval and LLM reasoning.

---

# NiceGUI

Instead of a dedicated FIX page, NAS-AI will expose:

Analysis

Users choose:

- FIX
- XML
- SQL
- Halo
- Generic

The UI remains identical regardless of analyzer.

---

# Future MCP Integration

The Analysis Engine is intentionally designed to become an MCP capability.

Example:

```
analyze(
    analyzer="fix",
    input=...
)
```

This allows:

- NiceGUI
- Open WebUI
- Claude Desktop
- VS Code
- External AI clients

to reuse the same analysis engine.

---

# Design Goals

The Analysis Engine should eventually answer questions such as:

"What happened?"

"What does this message mean?"

"Why was this rejected?"

"What changed?"

"What should happen next?"

"What documents explain this?"

"What systems are affected?"

"What tickets are related?"

without requiring users to understand the underlying protocol.

---

# Roadmap

## ANL-001

Analysis Engine framework

---

## ANL-002

Analyzer Registry

---

## ANL-003

Analysis Result contract

---

## ANL-004

FIX Analyzer

---

## ANL-005

Halo Analyzer

---

## ANL-006

SQL Analyzer

---

## ANL-007

XML Analyzer

---

## ANL-008

Log Analyzer

---

## ANL-009

Multi-artifact reasoning

Correlate multiple inputs into a single business explanation.

---

# Long-Term Vision

The Analysis Engine represents the next major evolution of NAS-AI.

The Ingestion Engine gave NAS-AI memory.

The Retrieval Engine gave NAS-AI recall.

The Analysis Engine gives NAS-AI understanding.

Together they form an intelligent platform capable of not only retrieving knowledge, but reasoning over real-world business artifacts in a deterministic, explainable, and extensible manner.


Non-Goals

The Analysis Engine will not:

    * Duplicate parser logic.
    * Duplicate serializer logic.
    * Store business knowledge outside the Knowledge Base.
    * Hardcode protocol definitions.
    * Bypass the Retrieval Engine.
    * Produce analyzer-specific UI components when a generic Analysis UI can be used.
    * Perform protocol parsing inside the LLM.
    * Become responsible for ingestion.


DO NOT TOUCH:
    existing parsers
    existing serializers
    existing OCR
    existing retrieval/router
    existing Ask/Chat
    existing KG logic
    existing ingestion flow

If new functionality is required:

    * Create a new parser.
    * Create a new serializer.
    * Create a new analyzer.
    * Create a new adapter.

    Do not modify existing implementations.