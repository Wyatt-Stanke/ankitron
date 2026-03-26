"""HTTP client for the AnkiWeb sync protocol (v11 — zstd + direct POST)."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass

import requests
import zstandard

ANKIWEB_BASE = "https://sync.ankiweb.net/"
SYNC_VERSION = 11
CLIENT_VER_SHORT = "25.9.2,ankisync,linux"
CLIENT_VER_FULL = "anki,25.9.2 (ankisync),linux"


@dataclass
class SyncMeta:
    modified: int
    schema: int
    usn: int
    server_time: int
    server_message: str
    should_continue: bool
    host_number: int
    empty: bool
    media_usn: int


class AnkiWebClient:
    """Direct HTTP client for the AnkiWeb sync protocol."""

    def __init__(self, endpoint: str | None = None):
        self.base_url = endpoint or ANKIWEB_BASE
        if not self.base_url.endswith("/"):
            self.base_url += "/"
        self.hkey: str = ""
        self.session_key: str = secrets.token_hex(8)
        self._cctx = zstandard.ZstdCompressor()
        self._dctx = zstandard.ZstdDecompressor()
        self._session = requests.Session()

    def _make_header(self) -> str:
        return json.dumps({
            "v": SYNC_VERSION,
            "k": self.hkey,
            "c": CLIENT_VER_SHORT,
            "s": self.session_key,
        })

    def _post(self, path: str, data: bytes, *, is_json: bool = True) -> bytes:
        """Send a zstd-compressed POST and return the decompressed response.

        Handles 308 redirects by updating the base URL and retrying.
        """
        compressed = self._cctx.compress(data)
        headers = {
            "Content-Type": "application/octet-stream",
            "anki-sync": self._make_header(),
        }

        url = self.base_url + path
        resp = self._session.post(url, data=compressed, headers=headers, allow_redirects=False)

        # Handle shard redirect (308)
        if resp.status_code in (301, 302, 307, 308):
            new_base = resp.headers["Location"]
            if not new_base.endswith("/"):
                new_base += "/"
            self.base_url = new_base
            url = self.base_url + path
            resp = self._session.post(url, data=compressed, headers=headers)

        if resp.status_code != 200:
            raise SyncError(f"Server returned {resp.status_code}: {resp.text[:500]}")

        orig_size = resp.headers.get("anki-original-size")
        if orig_size:
            return self._dctx.decompress(resp.content, max_output_size=int(orig_size) + 4096)
        # Fallback: try decompressing without known size
        try:
            return self._dctx.decompress(resp.content, max_output_size=50 * 1024 * 1024)
        except zstandard.ZstdError:
            return resp.content

    def login(self, username: str, password: str) -> str:
        """Login to AnkiWeb. Returns the host key (session token)."""
        body = json.dumps({"u": username, "p": password}).encode()
        resp = self._post("sync/hostKey", body)
        result = json.loads(resp)
        self.hkey = result["key"]
        return self.hkey

    def meta(self) -> SyncMeta:
        """Fetch sync metadata from the server."""
        body = json.dumps({"v": SYNC_VERSION, "cv": CLIENT_VER_FULL}).encode()
        resp = self._post("sync/meta", body)
        data = json.loads(resp)
        return SyncMeta(
            modified=data["mod"],
            schema=data["scm"],
            usn=data["usn"],
            server_time=data["ts"],
            server_message=data.get("msg", ""),
            should_continue=data.get("cont", True),
            host_number=data.get("hostNum", 0),
            empty=data.get("empty", True),
            media_usn=data.get("media_usn", 0),
        )

    def upload(self, collection_data: bytes) -> str:
        """Upload a collection file (raw SQLite bytes). Returns 'OK' on success."""
        resp = self._post("sync/upload", collection_data, is_json=False)
        result = resp.decode("utf-8").strip()
        if result != "OK":
            raise SyncError(f"Upload failed: {result}")
        return result

    def download(self) -> bytes:
        """Download the collection file from the server. Returns raw SQLite bytes."""
        body = b"{}"
        return self._post("sync/download", body, is_json=False)


class SyncError(Exception):
    pass
