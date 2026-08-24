"""Filesystem reconciliation, verification, and final-file moves."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
import logging
import os
from pathlib import Path
import shutil

from ..aria2 import Aria2DownloadManager
from ..database import OneDriveItem
from ..task import MigrationTask
from ..utils.hashing import file_hash


logger = logging.getLogger(__name__)


class TransferStatus(StrEnum):
    PENDING = "pending"
    DOWNLOADING = "downloading"
    STAGED = "staged"
    MOVING = "moving"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ManifestFile:
    item: OneDriveItem
    relative_path: str


@dataclass(slots=True)
class Transfer:
    file: ManifestFile
    status: TransferStatus = TransferStatus.PENDING
    aria2_gid: str | None = None
    last_error: str | None = None


class MigrationWorker:
    """Reconcile one manifest file against local staging and final paths."""

    def __init__(self, task: MigrationTask, downloads: Aria2DownloadManager) -> None:
        self._task = task
        self._downloads = downloads

    async def reconcile(self, transfer: Transfer) -> TransferStatus:
        """Derive transient transfer state entirely from local filesystem artifacts."""
        item = transfer.file.item
        error = _metadata_error(item)
        if error is not None:
            return self._fail(transfer, error)
        final_path = self._task.dist_path(transfer.file.relative_path)
        temp_path = self._temp_path(item)
        partial_path = self._task.partial_path(final_path)

        if final_path.exists():
            if await self._verify(item, final_path):
                transfer.status = TransferStatus.COMPLETE
                return transfer.status
            await asyncio.to_thread(final_path.unlink)

        if partial_path.exists():
            if await self._verify(item, partial_path):
                await asyncio.to_thread(os.replace, partial_path, final_path)
                if temp_path.exists():
                    await asyncio.to_thread(temp_path.unlink)
                transfer.status = TransferStatus.COMPLETE
                return transfer.status
            await asyncio.to_thread(partial_path.unlink)

        control_path = Path(f"{temp_path}.aria2")
        if temp_path.exists() and control_path.exists():
            logger.info("Resuming interrupted download: %s", transfer.file.relative_path)
            transfer.aria2_gid = await self._downloads.resume(item, transfer.file.relative_path)
            transfer.status = TransferStatus.DOWNLOADING
            return transfer.status

        if temp_path.exists():
            if await self._verify(item, temp_path):
                transfer.status = TransferStatus.STAGED
                logger.info("Verified staged download: %s", transfer.file.relative_path)
                return transfer.status
            await asyncio.to_thread(temp_path.unlink)

        transfer.status = TransferStatus.PENDING
        return transfer.status

    async def submit(self, transfer: Transfer) -> None:
        transfer.aria2_gid = await self._downloads.submit(
            transfer.file.item, transfer.file.relative_path
        )
        transfer.status = TransferStatus.DOWNLOADING

    async def poll(self, transfer: Transfer) -> TransferStatus:
        if transfer.aria2_gid is None:
            return self._fail(transfer, "Download has no aria2 GID")
        status, error = await self._downloads.poll(transfer.aria2_gid)
        if status in {"active", "waiting", "paused"}:
            return transfer.status
        if status in {"error", "removed"}:
            return self._fail(transfer, error or f"aria2 task {status}")
        logger.info("Download finished, verifying: %s", transfer.file.relative_path)
        return await self.check_download(transfer)

    async def check_download(self, transfer: Transfer) -> TransferStatus:
        item = transfer.file.item
        temp_path = self._temp_path(item)
        if await self._verify(item, temp_path):
            transfer.status = TransferStatus.STAGED
            logger.info("Verified downloaded file: %s", transfer.file.relative_path)
            return transfer.status
        if temp_path.exists():
            await asyncio.to_thread(temp_path.unlink)
        control_path = Path(f"{temp_path}.aria2")
        if control_path.exists():
            await asyncio.to_thread(control_path.unlink)
        logger.warning("Downloaded file failed verification; retrying: %s", transfer.file.relative_path)
        transfer.aria2_gid = None
        transfer.status = TransferStatus.PENDING
        return transfer.status

    async def move(self, transfer: Transfer) -> TransferStatus:
        item = transfer.file.item
        temp_path = self._temp_path(item)
        final_path = self._task.dist_path(transfer.file.relative_path)
        partial_path = self._task.partial_path(final_path)
        if not await self._verify(item, temp_path):
            return await self.reconcile(transfer)

        transfer.status = TransferStatus.MOVING
        logger.info("Moving to final directory: %s", transfer.file.relative_path)
        await asyncio.to_thread(final_path.parent.mkdir, parents=True, exist_ok=True)
        if partial_path.exists():
            await asyncio.to_thread(partial_path.unlink)
        await asyncio.to_thread(shutil.copyfile, temp_path, partial_path)
        if not await self._verify(item, partial_path):
            await asyncio.to_thread(partial_path.unlink)
            transfer.status = TransferStatus.STAGED
            logger.warning("Moved file failed verification; retrying move: %s", transfer.file.relative_path)
            return transfer.status

        await asyncio.to_thread(os.replace, partial_path, final_path)
        await asyncio.to_thread(temp_path.unlink)
        transfer.status = TransferStatus.COMPLETE
        logger.info("Transfer complete: %s", transfer.file.relative_path)
        return transfer.status

    def _temp_path(self, item: OneDriveItem) -> Path:
        if item.name is None:
            raise ValueError(f"OneDrive item {item.drive_item_id} has no file name")
        return self._task.temp_path(item.drive_item_id, item.name)

    async def _verify(self, item: OneDriveItem, path: Path) -> bool:
        error = _metadata_error(item)
        if error is not None:
            return False
        try:
            stat = await asyncio.to_thread(path.stat)
        except OSError:
            return False
        if stat.st_size != item.size:
            return False
        digest = await asyncio.to_thread(file_hash, path, item.hash_type)
        return digest == item.hash

    @staticmethod
    def _fail(transfer: Transfer, message: str) -> TransferStatus:
        transfer.status = TransferStatus.FAILED
        transfer.last_error = message
        logger.error("Transfer failed for %s: %s", transfer.file.relative_path, message)
        return transfer.status


def _metadata_error(item: OneDriveItem) -> str | None:
    if item.size is None:
        return "OneDrive item has no size metadata"
    if item.hash_type not in {"sha1", "sha256", "quickXorHash"} or item.hash is None:
        return "OneDrive item has no supported hash metadata"
    return None
