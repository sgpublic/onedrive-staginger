"""Filesystem reconciliation, verification, and final-file moves."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from ..aria2 import Aria2DownloadManager
from ..database import OneDriveItem, TransferRecord, TransferStatus
from ..task import MigrationTask
from ..utils.hashing import file_hash


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Verification:
    size: int
    mtime_ns: int
    digest: str


class MigrationWorker:
    """Reconcile one manifest file against local staging and final paths."""

    def __init__(self, task: MigrationTask, downloads: Aria2DownloadManager) -> None:
        self._task = task
        self._downloads = downloads

    async def reconcile(self, item: OneDriveItem, transfer: TransferRecord) -> TransferStatus:
        """Reconcile stale persisted state with dist, partial, temp, and aria2 files."""
        metadata_error = _metadata_error(item)
        if metadata_error is not None:
            _set_transfer(item.drive_item_id, TransferStatus.FAILED, last_error=metadata_error)
            return TransferStatus.FAILED
        final_path = self._task.download_path(_required(item.relative_path, "relative path"))
        temp_path = self._task.temp_path(item.drive_item_id, _required(item.name, "file name"))
        partial_path = self._task.partial_path(final_path)

        if final_path.exists():
            verification = await self._verify(item, transfer, final_path)
            if verification is not None:
                _set_transfer(item.drive_item_id, TransferStatus.COMPLETE, verification=verification)
                return TransferStatus.COMPLETE
            await asyncio.to_thread(final_path.unlink)

        if partial_path.exists():
            verification = await self._verify(item, transfer, partial_path)
            if verification is not None:
                await asyncio.to_thread(os.replace, partial_path, final_path)
                if temp_path.exists():
                    await asyncio.to_thread(temp_path.unlink)
                _set_transfer(item.drive_item_id, TransferStatus.COMPLETE, verification=verification)
                return TransferStatus.COMPLETE
            await asyncio.to_thread(partial_path.unlink)

        control_path = Path(f"{temp_path}.aria2")
        if temp_path.exists() and control_path.exists():
            logger.info("Resuming interrupted download: %s", item.relative_path)
            await self._downloads.resume(item)
            return TransferStatus.DOWNLOADING

        if temp_path.exists():
            verification = await self._verify(item, transfer, temp_path)
            if verification is not None:
                _set_transfer(item.drive_item_id, TransferStatus.STAGED, verification=verification)
                logger.info("Verified staged download: %s", item.relative_path)
                return TransferStatus.STAGED
            await asyncio.to_thread(temp_path.unlink)
            if control_path.exists():
                await asyncio.to_thread(control_path.unlink)

        _set_transfer(item.drive_item_id, TransferStatus.PENDING, aria2_gid=None)
        return TransferStatus.PENDING

    async def check_download(self, item: OneDriveItem, transfer: TransferRecord) -> TransferStatus:
        """Validate an aria2-complete temp file before it can be moved."""
        metadata_error = _metadata_error(item)
        if metadata_error is not None:
            _set_transfer(item.drive_item_id, TransferStatus.FAILED, last_error=metadata_error)
            return TransferStatus.FAILED
        temp_path = self._task.temp_path(item.drive_item_id, _required(item.name, "file name"))
        verification = await self._verify(item, transfer, temp_path) if temp_path.exists() else None
        if verification is not None:
            _set_transfer(item.drive_item_id, TransferStatus.STAGED, verification=verification)
            logger.info("Verified downloaded file: %s", item.relative_path)
            return TransferStatus.STAGED

        if temp_path.exists():
            await asyncio.to_thread(temp_path.unlink)
        control_path = Path(f"{temp_path}.aria2")
        if control_path.exists():
            await asyncio.to_thread(control_path.unlink)
        _set_transfer(item.drive_item_id, TransferStatus.PENDING, aria2_gid=None)
        logger.warning("Downloaded file failed verification; retrying: %s", item.relative_path)
        return TransferStatus.PENDING

    async def move(self, item: OneDriveItem, transfer: TransferRecord) -> TransferStatus:
        """Copy a verified temp file through a verified partial file into dist."""
        temp_path = self._task.temp_path(item.drive_item_id, _required(item.name, "file name"))
        final_path = self._task.download_path(_required(item.relative_path, "relative path"))
        partial_path = self._task.partial_path(final_path)
        source_verification = await self._verify(item, transfer, temp_path) if temp_path.exists() else None
        if source_verification is None:
            return await self.reconcile(item, transfer)

        _set_transfer(item.drive_item_id, TransferStatus.MOVING)
        logger.info("Moving to final directory: %s", item.relative_path)
        await asyncio.to_thread(final_path.parent.mkdir, parents=True, exist_ok=True)
        if partial_path.exists():
            await asyncio.to_thread(partial_path.unlink)
        await asyncio.to_thread(shutil.copyfile, temp_path, partial_path)
        verification = await self._verify(item, transfer, partial_path)
        if verification is None:
            await asyncio.to_thread(partial_path.unlink)
            _set_transfer(item.drive_item_id, TransferStatus.STAGED, verification=source_verification)
            logger.warning("Moved file failed verification; retrying move: %s", item.relative_path)
            return TransferStatus.STAGED

        await asyncio.to_thread(os.replace, partial_path, final_path)
        await asyncio.to_thread(temp_path.unlink)
        _set_transfer(item.drive_item_id, TransferStatus.COMPLETE, verification=verification)
        logger.info("Transfer complete: %s", item.relative_path)
        return TransferStatus.COMPLETE

    async def _verify(
        self, item: OneDriveItem, transfer: TransferRecord, path: Path
    ) -> Verification | None:
        if item.size is None or item.hash_type is None or item.hash is None:
            return None
        try:
            stat = await asyncio.to_thread(path.stat)
        except OSError:
            return None
        if (
            stat.st_size == item.size
            and transfer.verified_size == stat.st_size
            and transfer.verified_mtime == stat.st_mtime_ns
            and transfer.verified_hash == item.hash
        ):
            return Verification(stat.st_size, stat.st_mtime_ns, item.hash)
        size, mtime_ns, digest = await asyncio.to_thread(_verify_file, path, item.size, item.hash_type)
        if digest != item.hash:
            return None
        return Verification(size, mtime_ns, digest)


def _verify_file(path: Path, expected_size: int, hash_type: str) -> tuple[int, int, str]:
    stat = path.stat()
    if stat.st_size != expected_size:
        return stat.st_size, stat.st_mtime_ns, ""
    return stat.st_size, stat.st_mtime_ns, file_hash(path, hash_type)


def _set_transfer(
    drive_item_id: str,
    status: TransferStatus,
    *,
    aria2_gid: str | None | object = ...,
    verification: Verification | None = None,
    last_error: str | None = None,
) -> None:
    values: dict[str, str | int | None] = {"status": status.value, "last_error": last_error}
    if aria2_gid is not ...:
        values["aria2_gid"] = aria2_gid  # type: ignore[assignment]
    if verification is not None:
        values.update(
            verified_size=verification.size,
            verified_mtime=verification.mtime_ns,
            verified_hash=verification.digest,
        )
    TransferRecord.update(**values).where(TransferRecord.drive_item_id == drive_item_id).execute()


def _required(value: str | None, label: str) -> str:
    if value is None:
        raise ValueError(f"OneDrive item has no {label}")
    return value


def _metadata_error(item: OneDriveItem) -> str | None:
    if item.size is None:
        return "OneDrive item has no size metadata"
    if item.hash_type not in {"sha1", "sha256", "quickXorHash"} or item.hash is None:
        return "OneDrive item has no supported hash metadata"
    return None
