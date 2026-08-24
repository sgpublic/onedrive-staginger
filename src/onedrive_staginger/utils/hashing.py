"""Streaming local hash implementations compatible with OneDrive metadata."""

from __future__ import annotations

import base64
from collections.abc import Callable
import hashlib
from pathlib import Path

from quickxorhash import quickxorhash

CHUNK_SIZE = 1024 * 1024


def file_hash(
    path: Path, hash_type: str, on_progress: Callable[[int], None] | None = None
) -> str:
    """Return a OneDrive-compatible Base64 digest for ``path``."""
    if hash_type == "quickXorHash":
        hasher = quickxorhash()
    elif hash_type in {"sha1", "sha256"}:
        hasher = hashlib.new(hash_type)
    else:
        raise ValueError(f"Unsupported OneDrive hash type: {hash_type}")

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(CHUNK_SIZE), b""):
            hasher.update(chunk)
            if on_progress is not None:
                on_progress(file.tell())
    digest = hasher.digest()
    return base64.b64encode(digest).decode("ascii")
