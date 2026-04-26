from TABLES.table_detector import detect_table_type
from TABLES.table_serializer import (
    process_structured_table,
    process_entity_row_table,
    process_procedural_table,
)

DEBUG = True

def process_table(all_rows, schema_map):
    """
    all_rows: {filename: rows}
    schema_map: {filename: schema}
    """

    # temporary: single-table assumption
    source_file = list(all_rows.keys())[0]
    rows = all_rows[source_file]
    schema = schema_map[source_file]

    table_type = detect_table_type(rows, schema)

    if DEBUG:
        print(f"[TABLE ROUTER] Detected type: {table_type}")

    if table_type == "structured":
        return process_structured_table(rows, schema, source_file)

    elif table_type == "entity_row":
        return process_entity_row_table(rows, schema, source_file)

    elif table_type == "procedural":
        return process_procedural_table(rows, schema, source_file)

    else:
        raise ValueError(f"Unknown table type: {table_type}")