# XML Regression Checks

These checks must pass before and after changing XML ingestion, XML serialization, schema inference, or link-index logic.

The purpose is to protect FIX field lookup and enum lookup behavior.

---

## 1. Required XML regression questions

| ID | Question | Expected Behavior | Status |
|---|---|---|---|
| REG-XML-001 | What values can tag 22 have? | Must return enum values for tag 22 / SecurityIDSource. It must not return unrelated fields. | Not Started |
| REG-XML-002 | What is tag 55? | Must identify tag 55 as Symbol / field name and explain its meaning. | Not Started |
| REG-XML-003 | What does SecurityIDSource mean? | Must identify SecurityIDSource as tag 22 and explain it. | Not Started |
| REG-XML-004 | What tag is Symbol? | Must identify Symbol as tag 55. | Not Started |
| REG-XML-005 | What values can SecurityIDSource have? | Must return enum values for tag 22. | Not Started |

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