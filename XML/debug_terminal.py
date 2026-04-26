from XML.xml_serializer import xml_serializer

all_rows = getattr(xml_serializer, "_all_rows", {})

print("\n=== DEBUG _all_rows STRUCTURE ===")

for fname, rows in all_rows.items():
    print(f"\nFILE: {fname}")
    print("TYPE:", type(rows))

    if isinstance(rows, list):
        print("LIST SAMPLE TYPE:", type(rows[0]) if rows else "EMPTY")
        if rows and isinstance(rows[0], dict):
            print("KEYS:", rows[0].keys())

    elif isinstance(rows, dict):
        print("DICT KEYS:", rows.keys())

print("\n=== END DEBUG ===")