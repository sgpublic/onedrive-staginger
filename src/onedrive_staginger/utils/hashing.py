"""Streaming local hash implementations compatible with OneDrive metadata."""

from __future__ import annotations

import base64
import hashlib
from pathlib import Path

CHUNK_SIZE = 1024 * 1024


def file_hash(path: Path, hash_type: str) -> str:
    """Return a OneDrive-compatible Base64 digest for ``path``."""
    if hash_type == "quickXorHash":
        digest = _quick_xor_hash(path)
    elif hash_type in {"sha1", "sha256"}:
        hasher = hashlib.new(hash_type)
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(CHUNK_SIZE), b""):
                hasher.update(chunk)
        digest = hasher.digest()
    else:
        raise ValueError(f"Unsupported OneDrive hash type: {hash_type}")
    return base64.b64encode(digest).decode("ascii")


def _quick_xor_hash(path: Path) -> bytes:
    """Implement Microsoft's 160-bit QuickXorHash stream algorithm."""
    result = bytearray(20)
    length = 0
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(CHUNK_SIZE), b""):
            for value in chunk:
                shift = (length * 11) % 160
                byte_index, bit_offset = divmod(shift, 8)
                result[byte_index] ^= (value << bit_offset) & 0xFF
                result[(byte_index + 1) % len(result)] ^= value >> (8 - bit_offset)
                length += 1
    for index, value in enumerate(length.to_bytes(8, "little")):
        result[len(result) - 8 + index] ^= value
    return bytes(result)
