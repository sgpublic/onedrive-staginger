"""Peewee models for the static OneDrive manifest and sync cursor."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

from peewee import (
    BigIntegerField,
    BooleanField,
    CharField,
    DateTimeField,
    EXCLUDED,
    Model,
    SqliteDatabase,
    TextField,
)


database = SqliteDatabase(
    None,
    pragmas={
        "busy_timeout": 5_000,
        "foreign_keys": 1,
        "journal_mode": "wal",
    },
)


DELTA_CURSOR_KEY = "onedrive.delta_cursor_url"
MANIFEST_STATE_KEY = "onedrive.manifest_state"
MANIFEST_ROOT_ID_KEY = "onedrive.manifest_root_id"


class ManifestState(StrEnum):
    ENUMERATING = "enumerating"
    COMPLETE = "complete"
    FAILED = "failed"


class BaseModel(Model):
    class Meta:
        database = database


class OneDriveItem(BaseModel):
    """One file or folder in the static OneDrive tree."""

    drive_item_id = CharField(unique=True)
    parent_drive_item_id = CharField(null=True, index=True)
    name = TextField(null=True)

    is_file = BooleanField(default=False)
    is_folder = BooleanField(default=False)
    is_deleted = BooleanField(default=False, index=True)

    size = BigIntegerField(null=True)
    hash_type = CharField(null=True)
    hash = TextField(null=True)
    etag = CharField(null=True)
    remote_mtime = DateTimeField(null=True)


class KeyValue(BaseModel):
    """Small persistent state values such as the complete Graph delta cursor URL."""

    key = CharField(primary_key=True)
    value = TextField()
    updated_at = DateTimeField(default=lambda: datetime.now(UTC))


def initialize_database(path: Path) -> SqliteDatabase:
    """Open a SQLite database and create the current schema when absent."""
    if not database.is_closed():
        database.close()
    database.init(str(path.expanduser().resolve(strict=False)))
    database.connect()
    database.create_tables([OneDriveItem, KeyValue], safe=True)
    columns = {column.name for column in database.get_columns(OneDriveItem._meta.table_name)}
    if "relative_path" in columns:
        database.execute_sql("UPDATE onedriveitem SET relative_path = NULL")
    database.execute_sql("DROP TABLE IF EXISTS transferrecord")
    return database


def persist_delta_page(
    items: Iterable[Mapping[str, Any]],
    cursor_url: str,
    *,
    manifest_state: ManifestState = ManifestState.ENUMERATING,
) -> None:
    """Persist one complete delta page and its next cursor as one transaction.

    ``cursor_url`` is always the unmodified Graph ``@odata.nextLink`` or
    ``@odata.deltaLink``. The caller must guarantee remote files remain static
    throughout the migration; this cursor only resumes interrupted enumeration.
    """
    with database.atomic():
        for item in items:
            _upsert_item(item)
        _set_value(DELTA_CURSOR_KEY, cursor_url)
        _set_value(MANIFEST_STATE_KEY, manifest_state.value)


def get_value(key: str) -> str | None:
    row = KeyValue.get_or_none(KeyValue.key == key)
    return row.value if row else None


def set_manifest_state(state: ManifestState) -> None:
    """Set the manifest lifecycle state outside a delta page transaction."""
    _set_value(MANIFEST_STATE_KEY, state.value)


def ensure_root_item(root_drive_item_id: str) -> None:
    """Seed and persist the drive root used to resolve download paths."""
    (
        OneDriveItem.insert(
            drive_item_id=root_drive_item_id,
            name="",
            is_folder=True,
        )
        .on_conflict(
            conflict_target=[OneDriveItem.drive_item_id],
            update={OneDriveItem.name: "", OneDriveItem.is_folder: True},
        )
        .execute()
    )
    _set_value(MANIFEST_ROOT_ID_KEY, root_drive_item_id)


def get_manifest_root_id() -> str:
    root_id = get_value(MANIFEST_ROOT_ID_KEY)
    if root_id is None:
        raise ValueError("Manifest has no saved root item")
    return root_id


def get_manifest_child(parent_drive_item_id: str, name: str) -> OneDriveItem | None:
    """Look up one live direct child while resolving a requested remote path."""
    return OneDriveItem.get_or_none(
        OneDriveItem.parent_drive_item_id == parent_drive_item_id,
        OneDriveItem.name == name,
        ~OneDriveItem.is_deleted,
    )


def iter_manifest_children(parent_drive_item_id: str) -> Iterator[OneDriveItem]:
    """Stream live direct children without materializing a manifest subtree."""
    query = OneDriveItem.select().where(
        OneDriveItem.parent_drive_item_id == parent_drive_item_id,
        ~OneDriveItem.is_deleted,
    )
    yield from query.iterator()


def _upsert_item(item: Mapping[str, Any]) -> None:
    if not isinstance(item.get("drive_item_id"), str) or not item["drive_item_id"]:
        raise ValueError("Each OneDrive item must include a non-empty drive_item_id")
    existing = OneDriveItem.get_or_none(OneDriveItem.drive_item_id == item["drive_item_id"])
    if existing is not None and item.get("is_deleted") and not existing.is_deleted:
        _mark_descendants_deleted(existing.drive_item_id)

    fields = (
        "parent_drive_item_id",
        "name",
        "is_file",
        "is_folder",
        "is_deleted",
        "size",
        "hash_type",
        "hash",
        "etag",
        "remote_mtime",
    )
    updates = {
        getattr(OneDriveItem, field): getattr(EXCLUDED, field)
        for field in fields
        if field in item
    }
    query = OneDriveItem.insert(**item)
    if updates:
        query = query.on_conflict(
            conflict_target=[OneDriveItem.drive_item_id],
            update=updates,
        )
    else:
        query = query.on_conflict_ignore()
    query.execute()


def _mark_descendants_deleted(drive_item_id: str) -> None:
    queue = [drive_item_id]
    while queue:
        parent_id = queue.pop()
        children = list(
            OneDriveItem.select(OneDriveItem.drive_item_id).where(
                OneDriveItem.parent_drive_item_id == parent_id
            )
        )
        queue.extend(child.drive_item_id for child in children)
        OneDriveItem.update(is_deleted=True).where(
            OneDriveItem.drive_item_id == parent_id
        ).execute()


def _set_value(key: str, value: str) -> None:
    now = datetime.now(UTC)
    (
        KeyValue.insert(key=key, value=value, updated_at=now)
        .on_conflict(
            conflict_target=[KeyValue.key],
            update={KeyValue.value: value, KeyValue.updated_at: now},
        )
        .execute()
    )
