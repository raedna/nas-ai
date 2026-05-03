# XML Regression Checks

These checks must pass before and after changing XML ingestion, XML serialization, schema inference, or link-index logic.

The purpose is to protect FIX field lookup and enum lookup behavior.

---

## 1. Required XML regression questions

| ID | Question | Expected Behavior | Status |
|---|---|---|---|
| REG-XML-001 | XML | Ask can answer “what values can tag 22 have?” using namespace-aware structured lookup and normalized enum_values. | Done |
| REG-XML-002 | XML | Ask can answer “what is tag 55?” using namespace-aware structured lookup. | Done |
| REG-XML-003 | XML | Ask can answer “what does SecurityIDSource mean?” by matching primary_name or namespace lookup. | In Progress |
| REG-XML-004 | What tag is Symbol? | Must identify Symbol as tag 55. | Not Started |
| REG-XML-005 | XML | Ask can answer “what values can Rule80A have?” using structured primary_name lookup. | Done |
| REG-XML-006 | XML | Ask can answer “what is related to tag 22?” using link_keys and related_link_keys. | Done |
| REG-XML-007 | XML | Ask can answer “what tag contains principal” using reverse enum lookup. | Done |
| XML-006 | XML/schema/linking | Identifier values collide across different identifier fields such as Tag, ComponentID, SectionID, and MsgType. | Add identifier_namespace derived from schema identifier field name and use namespace + identifier for cross-linking. | Critical | Not Started |
| XML-007 | UI validation | No UI view to validate schema inference and payload quality before retrieval. | Add Streamlit schema/payload validation panel showing schema roles, inferred subtype, identifier collisions, and payload completeness. | High | Not Started |

---

## 2. Expected source behavior

XML ingestion should support multiple related XML files, such as:

* FIX fields file
* FIX enum values file

The system should link enum values to the correct field using schema roles, not hardcoded FIX logic.

Required schema roles:

| Role | Purpose |
|---|---|
| identifier | Field/tag identifier, for example tag 22 |
| primary_name | Main field name, for example SecurityIDSource |
| description | Field description |
| enum_value | Enum value/code |
| enum_name | Enum symbolic name/label |
| aliases | Alternative names where available |
| type | Field type where available |

---

## 3. Pass criteria

### REG-XML-001

Question:

```text
What values can tag 22 have?