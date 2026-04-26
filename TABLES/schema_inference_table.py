from core.schema_inference import infer_schema, load_roles_config, save_schema

DEBUG = True


def infer_table_schema(rows, collection_name=None, source_file=None):
    roles = load_roles_config("config/structured_roles.json")
    schema = infer_schema(rows, roles)

    # make sure expected keys always exist
    for key in [
        "identifier",
        "primary_name",
        "aliases",
        "description",
        "type",
        "enum_value",
        "enum_name",
        "other",
    ]:
        schema.setdefault(key, [])

    if DEBUG:
        print("[TABLE SCHEMA] Inferred schema:")
        print(schema)

    if collection_name and source_file:
        save_schema(schema, source_file, "schemas", collection_name)

    return schema