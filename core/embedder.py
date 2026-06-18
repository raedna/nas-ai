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


def embed_texts(texts, batch_size=64):
    """
    Embed a list of texts using batch API calls.
    LM Studio supports list input to /v1/embeddings — send batch_size texts per call.
    Falls back to one-at-a-time if batch call fails.
    """
    if not texts:
        return []

    embed_url, model = _load_embedding_config()
    results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        try:
            response = requests.post(
                embed_url,
                json={
                    "model": model,
                    "input": batch
                },
                timeout=120
            )
            if response.status_code == 200:
                data = response.json()["data"]
                # Sort by index to ensure correct order
                data.sort(key=lambda x: x.get("index", 0))
                results.extend([d["embedding"] for d in data])
            else:
                # Fallback to one-at-a-time for this batch
                for text in batch:
                    results.append(embed_text(text))
        except Exception:
            # Fallback to one-at-a-time for this batch
            for text in batch:
                results.append(embed_text(text))

    return results