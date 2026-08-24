"""Peewee models for the static OneDrive manifest and sync cursor."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Mapping

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


class ManifestState(StrEnum):
    ENUMERATING = "enumerating"
    COMPLETE = "complete"
    FAILED = "failed"


class TransferStatus(StrEnum):
    PENDING = "pending"
    CHECKING = "checking"
    DOWNLOADING = "downloading"
    STAGED = "staged"
    MOVING = "moving"
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
    relative_path = TextField(null=True, index=True)

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


class TransferRecord(BaseModel):
    """Local transfer state for one static OneDrive file."""

    drive_item_id = CharField(unique=True)
    status = CharField(
        choices=[(status.value, status.value) for status in TransferStatus],
        default=TransferStatus.PENDING.value,
        index=True,
    )
    aria2_gid = CharField(null=True)
    last_error = TextField(null=True)
    verified_size = BigIntegerField(null=True)
    verified_mtime = BigIntegerField(null=True)
    verified_hash = TextField(null=True)


def initialize_database(path: Path) -> SqliteDatabase:
    """Open a SQLite database and create the current schema when absent."""
    if not database.is_closed():
        database.close()
    database.init(str(path.expanduser().resolve(strict=False)))
    database.connect()
    database.create_tables([OneDriveItem, KeyValue, TransferRecord], safe=True)
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
            _create_transfer_if_needed(item)
        _set_value(DELTA_CURSOR_KEY, cursor_url)
        _set_value(MANIFEST_STATE_KEY, manifest_state.value)


def get_value(key: str) -> str | None:
    row = KeyValue.get_or_none(KeyValue.key == key)
    return row.value if row else None


def set_manifest_state(state: ManifestState) -> None:
    """Set the manifest lifecycle state outside a delta page transaction."""
    _set_value(MANIFEST_STATE_KEY, state.value)


def ensure_root_item(root_drive_item_id: str) -> None:
    """Seed the selected root so children can receive paths before root delta arrives."""
    (
        OneDriveItem.insert(
            drive_item_id=root_drive_item_id,
            name="",
            relative_path="",
            is_folder=True,
        )
        .on_conflict(
            conflict_target=[OneDriveItem.drive_item_id],
            update={OneDriveItem.relative_path: ""},
        )
        .execute()
    )


def claim_ready_transfers(limit: int) -> list[TransferRecord]:
    """Atomically claim path-ready files for local reconciliation."""
    with database.atomic():
        candidates = list(
            TransferRecord.select(TransferRecord.drive_item_id)
            .join(OneDriveItem, on=(TransferRecord.drive_item_id == OneDriveItem.drive_item_id))
            .where(
                TransferRecord.status == TransferStatus.PENDING.value,
                OneDriveItem.is_file,
                ~OneDriveItem.is_deleted,
                OneDriveItem.relative_path.is_null(False),
            )
            .limit(limit)
        )
        ids = [candidate.drive_item_id for candidate in candidates]
        if not ids:
            return []
        (
            TransferRecord.update(status=TransferStatus.CHECKING.value)
            .where(
                TransferRecord.drive_item_id.in_(ids),
                TransferRecord.status == TransferStatus.PENDING.value,
            )
            .execute()
        )
        return list(TransferRecord.select().where(TransferRecord.drive_item_id.in_(ids)))


def get_transfer_items(
    statuses: Iterable[TransferStatus] | None = None,
    relative_root: str = "",
) -> list[tuple[OneDriveItem, TransferRecord]]:
    """Return path-resolved files in one static manifest subtree and their transfer state."""
    query = (
        TransferRecord.select()
        .join(OneDriveItem, on=(TransferRecord.drive_item_id == OneDriveItem.drive_item_id))
        .where(
            OneDriveItem.is_file,
            ~OneDriveItem.is_deleted,
            OneDriveItem.relative_path.is_null(False),
        )
    )
    if statuses is not None:
        query = query.where(TransferRecord.status.in_([status.value for status in statuses]))
    if relative_root:
        query = query.where(OneDriveItem.relative_path.startswith(f"{relative_root}/"))
    records = list(query)
    items = {
        item.drive_item_id: item
        for item in OneDriveItem.select().where(
            OneDriveItem.drive_item_id.in_([record.drive_item_id for record in records])
        )
    }
    return [(items[record.drive_item_id], record) for record in records]


def resolve_relative_paths(root_drive_item_id: str) -> int:
    """Publish paths whose complete parent chain is now known.

    Graph delta pages do not guarantee parent-before-child ordering. The breadth-first
    walk only queries direct unresolved children, so it can run after every page.
    """
    resolved = 0
    with database.atomic():
        root = OneDriveItem.get_or_none(OneDriveItem.drive_item_id == root_drive_item_id)
        if root is None or root.is_deleted:
            return 0
        if root.relative_path != "":
            root.relative_path = ""
            root.save(only=[OneDriveItem.relative_path])
            resolved += 1

        queue = [root]
        while queue:
            parent = queue.pop()
            children = OneDriveItem.select().where(
                OneDriveItem.parent_drive_item_id == parent.drive_item_id,
                OneDriveItem.relative_path.is_null(True),
                ~OneDriveItem.is_deleted,
            )
            for child in children:
                if child.name is None:
                    continue
                child.relative_path = (
                    child.name if parent.relative_path == "" else f"{parent.relative_path}/{child.name}"
                )
                child.save(only=[OneDriveItem.relative_path])
                queue.append(child)
                resolved += 1
    return resolved


def _upsert_item(item: Mapping[str, Any]) -> None:
    if not isinstance(item.get("drive_item_id"), str) or not item["drive_item_id"]:
        raise ValueError("Each OneDrive item must include a non-empty drive_item_id")
    existing = OneDriveItem.get_or_none(OneDriveItem.drive_item_id == item["drive_item_id"])
    if existing is not None and _item_changed(existing, item):
        _clear_relative_paths(existing.drive_item_id)
        if item.get("is_deleted"):
            _mark_descendants_deleted(existing.drive_item_id)

    fields = (
        "parent_drive_item_id",
        "name",
        "relative_path",
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


def _item_changed(existing: OneDriveItem, item: Mapping[str, Any]) -> bool:
    return any(
        field in item and getattr(existing, field) != item[field]
        for field in (
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
    )


def _clear_relative_paths(drive_item_id: str) -> None:
    """Invalidate an item and descendants after a rename, move, or deletion."""
    queue = [drive_item_id]
    while queue:
        parent_id = queue.pop()
        children = list(
            OneDriveItem.select(OneDriveItem.drive_item_id).where(
                OneDriveItem.parent_drive_item_id == parent_id
            )
        )
        queue.extend(child.drive_item_id for child in children)
        OneDriveItem.update(relative_path=None).where(
            OneDriveItem.drive_item_id == parent_id
        ).execute()
        _reset_transfer(parent_id)


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


def _reset_transfer(drive_item_id: str) -> None:
    TransferRecord.update(
        status=TransferStatus.PENDING.value,
        aria2_gid=None,
        last_error=None,
        verified_size=None,
        verified_mtime=None,
        verified_hash=None,
    ).where(TransferRecord.drive_item_id == drive_item_id).execute()


def _create_transfer_if_needed(item: Mapping[str, Any]) -> None:
    if item.get("is_file") and not item.get("is_deleted"):
        (
            TransferRecord.insert(drive_item_id=item["drive_item_id"])
            .on_conflict_ignore()
            .execute()
        )


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
