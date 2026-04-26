from XML.xml_parser import parse_xml_rows
from core.schema_inference import infer_schema, load_roles_config
from core.link_index import build_link_index
from core.nlp_generator import build_nlp_text
from core.embedder import embed_text
from core.qdrant_client import upsert_points

# --- INPUTS ---
fields_path = "/Users/raednasr/Documents/OmniVista/FIX Dict/Fields_FIX44.xml"
enums_path = "/Users/raednasr/Documents/OmniVista/FIX Dict/Enums_FIX44.xml"
roles_path = "config/structured_roles.json"

# --- PARSE ---
fields_rows = parse_xml_rows(fields_path, "Field")
enums_rows = parse_xml_rows(enums_path, "Enum")

# --- LOAD ROLES ---
roles = load_roles_config(roles_path)

# --- SCHEMA ---
fields_schema = infer_schema(fields_rows, roles)
enums_schema = infer_schema(enums_rows, roles)

# --- COMBINE ---
all_rows = {
    "Fields_FIX44.xml": fields_rows,
    "Enums_FIX44.xml": enums_rows
}

schema_map = {
    "Fields_FIX44.xml": fields_schema,
    "Enums_FIX44.xml": enums_schema
}

# --- LINK ---
link_index = build_link_index(all_rows, schema_map)

# --- BUILD POINTS ---
points = []

for identifier, entry in link_index["identifier"].items():

    text = build_nlp_text(identifier, entry)
    vector = embed_text(text)

    payload = {
        "identifier": identifier,
        "primary_name": entry.get("primary_name"),
        "aliases": entry.get("aliases"),
        "description": entry.get("description"),
        "enum_values": entry.get("enum_values"),
        "source_files": entry.get("source_files"),
        "text": text
    }

    points.append({
        "id": int(identifier) if identifier.isdigit() else abs(hash(identifier)),
        "vector": vector,
        "payload": payload
    })

print("POINTS:", len(points))

# --- UPLOAD ---
upsert_points("fix_collection", points)

print("✅ Uploaded to Qdrant")