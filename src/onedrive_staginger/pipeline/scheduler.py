"""Concurrency limits for aria2 downloads and filesystem moves."""

from __future__ import annotations

import asyncio

from ..aria2 import Aria2DownloadManager
from ..config import SchedulerConfig
from ..database import OneDriveItem, TransferRecord, TransferStatus, get_transfer_items
from .migration import MigrationWorker


class TransferScheduler:
    """Apply persistent download and in-process move concurrency limits."""

    def __init__(
        self,
        downloads: Aria2DownloadManager,
        worker: MigrationWorker,
        config: SchedulerConfig,
        manifest_root: str = "",
    ) -> None:
        self._downloads = downloads
        self._worker = worker
        self._max_downloads = config.max_downloads
        self._download_lock = asyncio.Lock()
        self._move_slots = asyncio.Semaphore(config.max_moves)
        self._manifest_root = manifest_root

    async def submit(self, item: OneDriveItem) -> bool:
        """Submit a download only when fewer than ``max_downloads`` are active."""
        async with self._download_lock:
            if self._manifest_root:
                active_downloads = len(
                    get_transfer_items([TransferStatus.DOWNLOADING], self._manifest_root)
                )
            else:
                active_downloads = TransferRecord.select().where(
                    TransferRecord.status == TransferStatus.DOWNLOADING.value
                ).count()
            if active_downloads >= self._max_downloads:
                return False
            await self._downloads.submit(item)
            return True

    async def poll(self, item: OneDriveItem, transfer: TransferRecord) -> TransferStatus:
        """Poll aria2 and validate a completed download before it is staged."""
        status = await self._downloads.poll(transfer)
        if status != "complete":
            refreshed = TransferRecord.get(TransferRecord.drive_item_id == item.drive_item_id)
            return TransferStatus(refreshed.status)
        refreshed = TransferRecord.get(TransferRecord.drive_item_id == item.drive_item_id)
        return await self._worker.check_download(item, refreshed)

    async def move(self, item: OneDriveItem, transfer: TransferRecord) -> TransferStatus:
        """Move one staged file while respecting the shared move limit."""
        async with self._move_slots:
            return await self._worker.move(item, transfer)
