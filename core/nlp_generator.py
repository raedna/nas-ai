import json
import os
from pathlib import Path
from embedder import generate_embedding

# =========================================================
# CONFIGURATION LOADER
# =========================================================
def load_nlp_config():
    config_path = Path("/Users/raednasr/RaedsMacM1/nas-ai/claude/config/nlp_config.json")
    
    if not config_path.exists():
        raise FileNotFoundError(f"nlp_config.json not found at {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)




# =========================================================
# GENERIC HELPERS
# =========================================================
def _row_norm(row):
    """Normalize row keys to lowercase for case-insensitive lookup"""
    return {str(k).lower(): v for k, v in row.items()}


def _first_value(row_norm, fields):
    """Extract first non-empty value from a list of field names"""
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            return str(val).strip()
    return ""


def _all_values(row_norm, fields):
    """Extract all non-empty values from a list of field names"""
    values = []
    for f in fields:
        val = row_norm.get(str(f).lower())
        if val not in [None, ""]:
            values.append(str(val).strip())
    return values


# =========================================================
# STRUCTURED NLP TEXT
# =========================================================
def build_structured_nlp_text(row, schema):
    """
    Build richly structured NLP text from row using schema field mappings.
    Includes identifier, name, description, type, aliases, and other fields.
    """
    row_n = _row_norm(row)

    id_fields = schema.get("identifier", [])
    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])
    alias_fields = schema.get("aliases", [])
    type_fields = schema.get("type", [])
    other_fields = schema.get("other", [])

    identifier = _first_value(row_n, id_fields)
    primary_name = _first_value(row_n, name_fields)
    description = _first_value(row_n, desc_fields)
    aliases = _all_values(row_n, alias_fields)
    type_value = _first_value(row_n, type_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)
    elif identifier:
        parts.append(identifier)

    if description:
        parts.append(description)

    if type_value:
        parts.append(f"Type: {type_value}")

    # Optional "other" fields (e.g., category, status)
    other_lines = []
    for f in other_fields:
        val = row_n.get(str(f).lower())
        if val not in [None, ""]:
            other_lines.append(f"{f}: {str(val).strip()}")

    if other_lines:
        parts.append("\n".join(other_lines))

    if aliases:
        parts.append(f"Also known as: {', '.join(aliases)}")

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# ENTITY-ROW NLP TEXT
# =========================================================
def build_entity_row_nlp_text(row, schema):
    """
    Build simple entity NLP text from row using schema field mappings.
    Includes primary name and description only.
    """
    row_n = _row_norm(row)

    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# PROCEDURAL NLP TEXT
# =========================================================
def build_procedural_nlp_text(row, schema):
    """
    Build procedural NLP text from row using schema field mappings.
    Falls back to all row values if schema mapping yields empty result.
    """
    row_n = _row_norm(row)

    name_fields = schema.get("primary_name", [])
    desc_fields = schema.get("description", [])

    primary_name = _first_value(row_n, name_fields)
    description_values = _all_values(row_n, desc_fields)

    parts = []

    if primary_name:
        parts.append(primary_name)

    if description_values:
        parts.append("\n\n".join(description_values))

    # Fallback: if no schema mapping found, use all row values
    if not parts:
        fallback = []
        for k, v in row.items():
            if v not in [None, ""]:
                fallback.append(f"{k}: {str(v).strip()}")
        if fallback:
            parts.append("\n".join(fallback))

    return "\n\n".join([p for p in parts if p]).strip()


# =========================================================
# EMBEDDING INTEGRATION
# =========================================================
def generate_text_embedding(nlp_text):
    """
    Generate embedding for NLP text using configured embedder.
    Delegates to embedder.py; handles errors gracefully.
    
    Args:
        nlp_text (str): Text to embed
        
    Returns:
        list[float]: Embedding vector, or empty list on failure
    """
    if not nlp_text or not nlp_text.strip():
        return []
    
    try:
        embedding = generate_embedding(nlp_text)
        return embedding
    except Exception as e:
        print(f"⚠️  Embedding generation failed: {e}")
        return []


# =========================================================
# FULL PIPELINE
# =========================================================
def process_row(row, content_type, schema):
    """
    Process a single row: generate NLP text, then embedding.
    
    Args:
        row (dict): Data row
        content_type (str): One of "structured", "entity", "procedural"
        schema (dict): Field mapping schema from nlp_config.json
        
    Returns:
        dict: {"nlp_text": str, "embedding": list[float]}
    """
    # Select builder based on content type
    builders = {
        "structured": build_structured_nlp_text,
        "entity": build_entity_row_nlp_text,
        "procedural": build_procedural_nlp_text,
    }
    
    builder = builders.get(content_type.lower())
    if not builder:
        raise ValueError(f"Unknown content_type: {content_type}")
    
    # Generate NLP text
    nlp_text = builder(row, schema)
    
    # Generate embedding
    embedding = generate_text_embedding(nlp_text)
    
    return {
        "nlp_text": nlp_text,
        "embedding": embedding,
    }
