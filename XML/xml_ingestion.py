from XML.xml_parser import parse_xml_rows
from core.schema_inference import infer_schema, load_roles_config, save_schema
from core.link_index import build_link_index
from core.embedder import embed_texts

from core.qdrant_client import upsert_points   # ✅ correct
from qdrant_client.models import PointStruct   # ✅ correct
from qdrant_client import QdrantClient

import uuid

client = QdrantClient(url="http://192.168.1.141:6333")

DEBUG = True



from qdrant_client.models import VectorParams, Distance

def ensure_collection_exists(collection_name, vector_size):

    existing = [c.name for c in client.get_collections().collections]

    if collection_name not in existing:
        print(f"🆕 Creating collection: {collection_name}")

        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE
            )
        )


def ingest_xml_collection(files, collection_name, force_reingest=False):

    print(f"\n📥 Processing XML collection ({len(files)} files)")

    all_rows = {}
    schemas = {}

    total_rows = 0
    file_row_counts = {}

    roles_config = load_roles_config("config/structured_roles.json")

    # ============================
    # PASS 1 — DISCOVERY
    # ============================
    for file_path in files:

        print(f"\n📄 Parsing: {file_path}")

        rows = parse_xml_rows(file_path)  # auto-detect now

        row_count = len(rows)
        file_row_counts[file_path.name] = row_count
        total_rows += row_count

        print(f"📊 Rows in {file_path.name}: {row_count}")

        schema = infer_schema(rows, roles_config)


        if DEBUG:
            print("🧪 Saving schema for:", file_path.name)

        save_schema(
            schema=schema,
            source_file=file_path,
            output_dir="schemas",
            collection_name=collection_name
        )

        if not rows:
            print(f"⚠️ No rows parsed: {file_path}")
            continue

        if DEBUG:
            print("DEBUG rows type:", type(rows))
            print("DEBUG first item type:", type(rows[0]) if rows else None)

        schema = infer_schema(rows, roles_config)

        all_rows[file_path.name] = rows
        schemas[file_path.name] = schema

    if not all_rows:
        print("❌ No data parsed")
        return

    # --- LINK INDEX (cross-file)
    link_index = build_link_index(all_rows, schemas)

    if DEBUG:
        print("DEBUG link_index keys:", list(link_index.keys()))
        print("DEBUG identifier count:", len(link_index.get("identifier", {})))

        if "22" in link_index.get("identifier", {}):
            print("\n🔎 DEBUG TAG 22:")
            print(link_index["identifier"]["22"])

    # ============================
    # PASS 2 — STRUCTURING
    # ============================
    documents = build_documents_from_link_index(link_index)

    if DEBUG:
        print("DEBUG documents:", len(documents))

    if not documents:
        print("⚠️ No documents generated")
        return

    texts = [doc["text"] for doc in documents]

    # ============================
    # PASS 3 — SEARCH PREP
    # ============================

    vectors = embed_texts(texts)

    points = []

    for i, doc in enumerate(documents):
        points.append(
            PointStruct(
                id=str(uuid.uuid4()),
                vector=vectors[i],
                payload=doc["metadata"] | {"text": doc["text"]}
            )
        )

    ensure_collection_exists(collection_name, len(vectors[0]))

    upsert_points(collection_name, points)

    print("\n📊 INGESTION SUMMARY")
    for fname, count in file_row_counts.items():
        print(f"{fname}: {count} rows")

    print(f"TOTAL ROWS: {total_rows}")
    print(f"TOTAL DOCUMENTS: {len(documents)}")

    print(f"\n✅ Completed XML ingestion ({len(documents)} documents)")


