import json
import time
import logging
import requests
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def load_config():
    config_path = Path("/Users/raednasr/RaedsMacM1/nas-ai/claude/config/nlp_config.json")
    
    if not config_path.exists():
        raise FileNotFoundError(f"nlp_config.json not found at {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)



class EmbedderClient:
    """
    Configuration-driven client for generating text embeddings
    via a local LM Studio OpenAI-compatible API.
    """

    def __init__(self, config: Optional[dict] = None):
        self.config      = config or _load_config()
        self.api_cfg     = self.config.get("api", {})
        self.response_cfg = self.config.get("response", {})

        self.url          = self.api_cfg.get("url", "http://localhost:8000/v1/embeddings")
        self.model        = self.api_cfg.get("model", "text-embedding-bge-large-en-v1.5")
        self.timeout      = self.api_cfg.get("timeout", 30)

        retry_cfg         = self.api_cfg.get("retry", {})
        self.max_attempts = retry_cfg.get("max_attempts", 3)
        self.backoff      = retry_cfg.get("backoff_factor", 2)

        self.data_path    = self.response_cfg.get("data_path", ["data"])
        self.embed_key    = self.response_cfg.get("embedding_key", "embedding")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def embed(self, text: str) -> Optional[list[float]]:
        """
        Generate an embedding for a single text string.

        Args:
            text: Input text to embed.

        Returns:
            List of floats representing the embedding, or None on failure.
        """
        if not self._validate_input(text):
            return None

        results = self._embed_batch([text])
        return results[0] if results else None

    def embed_batch(self, texts: list[str]) -> list[Optional[list[float]]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of input strings.

        Returns:
            List of embeddings (None for any failed entry).
        """
        if not texts:
            logger.warning("embed_batch called with empty list.")
            return []

        valid_texts = [t if self._validate_input(t) else None for t in texts]
        to_embed    = [t for t in valid_texts if t is not None]

        if not to_embed:
            return [None] * len(texts)

        results_map = {}
        raw_results = self._embed_batch(to_embed)
        idx = 0
        for i, t in enumerate(valid_texts):
            if t is not None:
                results_map[i] = raw_results[idx] if raw_results else None
                idx += 1
            else:
                results_map[i] = None

        return [results_map[i] for i in range(len(texts))]

    # ------------------------------------------------------------------
    # Internal Methods
    # ------------------------------------------------------------------

    def _embed_batch(self, texts: list[str]) -> list[Optional[list[float]]]:
        """Send a batch request to the embedding API with retry logic."""
        payload = {"model": self.model, "input": texts}

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = requests.post(
                    self.url,
                    json=payload,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return self._parse_response(response.json(), len(texts))

            except requests.exceptions.Timeout:
                logger.warning(f"Attempt {attempt}/{self.max_attempts}: Request timed out.")
            except requests.exceptions.ConnectionError:
                logger.warning(f"Attempt {attempt}/{self.max_attempts}: Connection error — is LM Studio running?")
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error on attempt {attempt}: {e}")
                break  # No point retrying on 4xx errors
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt}: {e}")

            if attempt < self.max_attempts:
                wait = self.backoff ** (attempt - 1)
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)

        logger.error("All retry attempts exhausted. Returning None results.")
        return [None] * len(texts)

    def _parse_response(self, body: dict, expected_count: int) -> list[Optional[list[float]]]:
        """
        Extract embeddings from the API response body.

        Traverses data_path, then reads embedding_key from each item.
        """
        try:
            data = body
            for key in self.data_path:
                data = data[key]

            if not isinstance(data, list):
                logger.error(f"Expected list at data_path, got: {type(data)}")
                return [None] * expected_count

            results = []
            for item in data:
                embedding = item.get(self.embed_key)
                if not self._validate_embedding(embedding):
                    results.append(None)
                else:
                    results.append(embedding)

            if len(results) != expected_count:
                logger.warning(
                    f"Response count mismatch: expected {expected_count}, got {len(results)}"
                )

            return results

        except (KeyError, TypeError) as e:
            logger.error(f"Failed to parse embedding response: {e}")
            return [None] * expected_count

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_input(text: str) -> bool:
        """Check that input text is a non-empty string."""
        if not isinstance(text, str):
            logger.warning(f"Invalid input type: {type(text)}. Expected str.")
            return False
        if not text.strip():
            logger.warning("Empty or whitespace-only string provided.")
            return False
        return True

    @staticmethod
    def _validate_embedding(embedding) -> bool:
        """Check that the returned embedding is a non-empty list of floats."""
        if not isinstance(embedding, list) or not embedding:
            logger.warning("Embedding is missing or not a list.")
            return False
        if not all(isinstance(v, (int, float)) for v in embedding):
            logger.warning("Embedding contains non-numeric values.")
            return False
        return True


# ------------------------------------------------------------------
# Legacy-compatible wrapper
# ------------------------------------------------------------------

_default_client: Optional[EmbedderClient] = None

def get_embedding(text: str) -> Optional[list[float]]:
    """
    Legacy wrapper for single-text embedding.
    Instantiates a shared EmbedderClient on first call.
    """
    global _default_client
    if _default_client is None:
        _default_client = EmbedderClient()
    return _default_client.embed(text)
