"""A single OneDrive-to-local migration task."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .utils.paths import dist_path, partial_path, temp_path


@dataclass(frozen=True, slots=True)
class MigrationTask:
    temp_dir: Path
    dist_dir: Path
    remote_root_path: str

    def __post_init__(self) -> None:
        temp_dir = self.temp_dir.expanduser().resolve(strict=False)
        dist_dir = self.dist_dir.expanduser().resolve(strict=False)
        if temp_dir == dist_dir:
            raise ValueError("temp_dir and dist_dir must be different directories")
        if not self.remote_root_path.startswith("/"):
            raise ValueError("remote_root_path must start with '/'")

        object.__setattr__(self, "temp_dir", temp_dir)
        object.__setattr__(self, "dist_dir", dist_dir)
        object.__setattr__(self, "remote_root_path", "/" + self.remote_root_path.strip("/"))

    def temp_path(self, drive_item_id: str, file_name: str) -> Path:
        """Return the flat temp path for one remote file."""
        return temp_path(self.temp_dir, drive_item_id, file_name)

    def dist_path(self, relative_path: str) -> Path:
        """Return the final path while rejecting absolute and traversal paths."""
        return dist_path(self.dist_dir, relative_path)

    def partial_path(self, final_path: Path) -> Path:
        """Return the destination-side partial path for a validated final path."""
        return partial_path(self.dist_dir, final_path)

    def download_path(self, manifest_path: str) -> Path:
        """Map a full-drive manifest path into this task's selected root."""
        root = self.remote_root_path.strip("/")
        if not root:
            return self.dist_path(manifest_path)
        prefix = f"{root}/"
        if not manifest_path.startswith(prefix):
            raise ValueError(f"Manifest path is outside remote_root_path: {manifest_path}")
        return self.dist_path(manifest_path.removeprefix(prefix))

    @property
    def manifest_root(self) -> str:
        """Return the selected root in the full-drive manifest's path format."""
        return self.remote_root_path.strip("/")
