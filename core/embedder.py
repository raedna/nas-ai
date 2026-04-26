import requests
from core.system_config import load_system_config


def _load_embedding_config():
    cfg = load_system_config()

    embed_url = cfg.get("embeddings_url")
    model = cfg.get("embeddings_model")

    if not embed_url:
        raise ValueError("Missing embeddings_url in config/system.json")

    if not model:
        raise ValueError("Missing embeddings_model in config/system.json")

    return embed_url, model


def embed_text(text):
    if text is None or not str(text).strip():
        raise ValueError("embed_text received empty text")

    embed_url, model = _load_embedding_config()

    response = requests.post(
        embed_url,
        json={
            "model": model,
            "input": text
        },
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"Embedding failed: {response.text}")

    return response.json()["data"][0]["embedding"]


def embed_texts(texts):
    return [embed_text(t) for t in texts]