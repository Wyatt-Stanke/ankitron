import hashlib
import json
import time
from pathlib import Path
from typing import Any

# TODO: Generate a cache dir that is best-practice crossplatform (Windows, MacOS, and Linux).
CACHE_DIR = Path.home() / ".cache" / "ankitron"
# TODO: The TTL should be per-cache item rather than per-cache object.
# The put() should accept a TTL, defaulting to DEFAULT_TTL.
DEFAULT_TTL = 7 * 24 * 3600  # 7 days in seconds


class Cache:
    """File-based response caching with TTL expiration."""

    def __init__(self, ttl: int = DEFAULT_TTL):
        self.ttl = ttl
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, params: dict[str, Any]) -> str:
        raw = json.dumps(params, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def _cache_path(self, key: str) -> Path:
        return CACHE_DIR / f"{key}.json"

    def get(self, params: dict[str, Any]) -> tuple[Any | None, float | None]:
        """
        Look up cached data. Returns (data, remaining_seconds) if fresh,
        or (None, None) if stale or missing.
        """
        key = self._cache_key(params)
        path = self._cache_path(key)
        if not path.exists():
            return None, None
        try:
            entry = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None, None
        expires_at = entry.get("expires_at", 0)
        remaining = expires_at - time.time()
        if remaining <= 0:
            return None, None
        return entry["data"], remaining

    def put(self, params: dict[str, Any], data: Any) -> None:
        """Store data in the cache with the configured TTL."""
        key = self._cache_key(params)
        path = self._cache_path(key)
        entry = {
            "expires_at": time.time() + self.ttl,
            "data": data,
        }
        path.write_text(json.dumps(entry, ensure_ascii=False), encoding="utf-8")
