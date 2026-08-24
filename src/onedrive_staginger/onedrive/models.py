"""Value objects returned by Microsoft Graph OneDrive endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class FileHashes:
    quick_xor_hash: str | None = None
    sha1_hash: str | None = None
    sha256_hash: str | None = None


@dataclass(frozen=True, slots=True)
class DriveItem:
    id: str
    name: str | None
    size: int | None
    e_tag: str | None
    last_modified_date_time: datetime | None
    parent_id: str | None
    is_file: bool
    is_folder: bool
    is_deleted: bool
    hashes: FileHashes | None = None

    @classmethod
    def from_graph(cls, value: dict[str, Any]) -> DriveItem:
        file_data = value.get("file")
        hashes_data = file_data.get("hashes", {}) if file_data else {}
        modified = value.get("lastModifiedDateTime")

        return cls(
            id=value["id"],
            name=value.get("name"),
            size=value.get("size"),
            e_tag=value.get("eTag"),
            last_modified_date_time=(
                datetime.fromisoformat(modified.replace("Z", "+00:00"))
                if modified
                else None
            ),
            parent_id=value.get("parentReference", {}).get("id"),
            is_file=file_data is not None,
            is_folder=value.get("folder") is not None,
            is_deleted=value.get("deleted") is not None,
            hashes=(
                FileHashes(
                    quick_xor_hash=hashes_data.get("quickXorHash"),
                    sha1_hash=hashes_data.get("sha1Hash"),
                    sha256_hash=hashes_data.get("sha256Hash"),
                )
                if file_data is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DeltaPage:
    items: tuple[DriveItem, ...]
    next_link: str | None
    delta_link: str | None


@dataclass(frozen=True, slots=True)
class AccessToken:
    token: str
    token_type: str
    expires_in: int
    refresh_token: str | None = None


@dataclass(frozen=True, slots=True)
class DeviceCode:
    device_code: str
    user_code: str
    verification_uri: str
    message: str
    expires_in: int
    interval: int


class GraphApiError(Exception):
    """A failed Microsoft Graph or Microsoft identity platform response."""

    def __init__(
        self,
        status: int,
        code: str | None,
        message: str,
        request_id: str | None,
    ) -> None:
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id
        super().__init__(f"Graph API request failed ({status}, {code}): {message}")


class DeltaCursorExpiredError(GraphApiError):
    """A delta cursor is no longer valid and a full enumeration is required."""
