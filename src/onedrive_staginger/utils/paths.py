"""Safe local path construction for migration files."""

from __future__ import annotations

from pathlib import Path, PurePosixPath

MAX_TEMP_FILE_BYTES = 240


def temp_path(temp_dir: Path, drive_item_id: str, file_name: str) -> Path:
    """Return the flat, UTF-8-safe temp path for one remote file."""
    if not drive_item_id or "/" in drive_item_id:
        raise ValueError("drive_item_id must be a non-empty path-safe identifier")
    if not file_name or "/" in file_name or file_name in {".", ".."}:
        raise ValueError("file_name must be a non-empty file name without path separators")
    prefix = f"{drive_item_id}-"
    available_bytes = MAX_TEMP_FILE_BYTES - len(prefix.encode("utf-8"))
    if available_bytes < 1:
        raise ValueError("drive_item_id is too long for a temporary file name")
    safe_file_name = file_name.encode("utf-8")[:available_bytes].decode("utf-8", "ignore")
    return temp_dir / f"{prefix}{safe_file_name}"


def dist_path(dist_dir: Path, relative_path: str) -> Path:
    """Return a final path while rejecting absolute and traversal paths."""
    path = PurePosixPath(relative_path)
    if not relative_path or path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ValueError("relative_path must be a non-empty relative path without traversal")
    return dist_dir.joinpath(*path.parts)


def partial_path(dist_dir: Path, final_path: Path) -> Path:
    """Return the destination-side partial path for a validated final path."""
    try:
        final_path.relative_to(dist_dir)
    except ValueError as error:
        raise ValueError("final_path must be inside dist_dir") from error
    return Path(f"{final_path}.partial")
