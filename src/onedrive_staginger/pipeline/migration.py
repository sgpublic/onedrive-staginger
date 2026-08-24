"""Filesystem reconciliation, verification, and final-file moves."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import logging
import os
from pathlib import Path
import time

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
    resume: bool = False
    downloaded_bytes: int = 0
    download_speed: int = 0

    @property
    def id(self) -> str:
        return self.file.item.drive_item_id


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
            transfer.resume = True
            transfer.downloaded_bytes = temp_path.stat().st_size
            transfer.status = TransferStatus.PENDING
            return transfer.status

        if temp_path.exists():
            if await self._verify(item, temp_path):
                transfer.status = TransferStatus.STAGED
                logger.info("已校验中转文件：%s", transfer.file.relative_path)
                return transfer.status
            await asyncio.to_thread(temp_path.unlink)

        transfer.status = TransferStatus.PENDING
        return transfer.status

    async def submit(self, transfer: Transfer) -> None:
        if transfer.resume:
            logger.info("正在续传中断的下载：%s", transfer.file.relative_path)
            transfer.aria2_gid = await self._downloads.resume(
                transfer.file.item, transfer.file.relative_path
            )
        else:
            transfer.aria2_gid = await self._downloads.submit(
                transfer.file.item, transfer.file.relative_path
            )
        transfer.status = TransferStatus.DOWNLOADING

    async def poll(self, transfer: Transfer) -> TransferStatus:
        if transfer.aria2_gid is None:
            return self._fail(transfer, "Download has no aria2 GID")
        progress = await self._downloads.poll(transfer.aria2_gid)
        transfer.downloaded_bytes = progress.completed_bytes
        transfer.download_speed = progress.download_speed
        if progress.status in {"active", "waiting", "paused"}:
            return transfer.status
        if progress.status in {"error", "removed"}:
            return self._fail(transfer, progress.error or f"aria2 task {progress.status}")
        logger.info("下载完成，正在校验：%s", transfer.file.relative_path)
        return await self.check_download(transfer)

    async def check_download(self, transfer: Transfer) -> TransferStatus:
        item = transfer.file.item
        temp_path = self._temp_path(item)
        if await self._verify(item, temp_path):
            transfer.downloaded_bytes = item.size or 0
            transfer.status = TransferStatus.STAGED
            logger.info("下载文件校验通过：%s", transfer.file.relative_path)
            return transfer.status
        if temp_path.exists():
            await asyncio.to_thread(temp_path.unlink)
        control_path = Path(f"{temp_path}.aria2")
        if control_path.exists():
            await asyncio.to_thread(control_path.unlink)
        logger.warning("下载文件校验失败，正在重试：%s", transfer.file.relative_path)
        transfer.aria2_gid = None
        transfer.status = TransferStatus.PENDING
        return transfer.status

    async def move(
        self, transfer: Transfer, on_progress: Callable[[int, int], None] | None = None
    ) -> TransferStatus:
        item = transfer.file.item
        temp_path = self._temp_path(item)
        final_path = self._task.dist_path(transfer.file.relative_path)
        partial_path = self._task.partial_path(final_path)
        if not await self._verify(item, temp_path):
            return await self.reconcile(transfer)

        transfer.status = TransferStatus.MOVING
        logger.info("正在搬运到最终目录：%s", transfer.file.relative_path)
        await asyncio.to_thread(final_path.parent.mkdir, parents=True, exist_ok=True)
        if partial_path.exists():
            await asyncio.to_thread(partial_path.unlink)
        await asyncio.to_thread(_copy_file, temp_path, partial_path, on_progress)
        if not await self._verify(item, partial_path):
            await asyncio.to_thread(partial_path.unlink)
            transfer.status = TransferStatus.STAGED
            logger.warning("搬运后文件校验失败，正在重试搬运：%s", transfer.file.relative_path)
            return transfer.status

        logger.info("搬运完成，正在原子重命名：%s", transfer.file.relative_path)
        await asyncio.to_thread(os.replace, partial_path, final_path)
        await asyncio.to_thread(temp_path.unlink)
        transfer.status = TransferStatus.COMPLETE
        logger.info("传输完成：%s", transfer.file.relative_path)
        return transfer.status

    def _temp_path(self, item: OneDriveItem) -> Path:
        if item.name is None:
            raise ValueError(f"OneDrive item {item.drive_item_id} has no file name")
        return self._task.temp_path(item.drive_item_id, item.name)

    async def _verify(self, item: OneDriveItem, path: Path) -> bool:
        error = _metadata_error(item)
        if error is not None:
            return False
        logger.info("正在校验文件：%s", path)
        try:
            stat = await asyncio.to_thread(path.stat)
        except OSError:
            logger.warning("无法读取待校验文件：%s", path)
            return False
        if stat.st_size != item.size:
            logger.warning("文件大小不匹配：%s", path)
            return False
        remote_mtime_ns = _remote_mtime_ns(item.remote_mtime)
        if remote_mtime_ns is not None and stat.st_mtime_ns == remote_mtime_ns:
            logger.info("文件大小和修改时间匹配，跳过哈希校验：%s", path)
            return True
        logger.info("正在计算文件哈希：%s", path)
        digest = await asyncio.to_thread(file_hash, path, item.hash_type)
        if digest != item.hash:
            logger.warning("文件哈希不匹配：%s", path)
            return False
        if remote_mtime_ns is not None:
            await asyncio.to_thread(os.utime, path, ns=(remote_mtime_ns, remote_mtime_ns))
            logger.info("文件哈希校验通过，已修正修改时间：%s", path)
        else:
            logger.info("文件哈希校验通过：%s", path)
        return True

    @staticmethod
    def _fail(transfer: Transfer, message: str) -> TransferStatus:
        transfer.status = TransferStatus.FAILED
        transfer.last_error = message
        logger.error("传输失败：%s，原因：%s", transfer.file.relative_path, message)
        return transfer.status


def _metadata_error(item: OneDriveItem) -> str | None:
    if item.size is None:
        return "OneDrive item has no size metadata"
    if item.hash_type not in {"sha1", "sha256", "quickXorHash"} or item.hash is None:
        return "OneDrive item has no supported hash metadata"
    return None


def _remote_mtime_ns(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    utc_value = parsed.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc_value - epoch
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _copy_file(
    source: Path, destination: Path, on_progress: Callable[[int, int], None] | None
) -> None:
    copied = 0
    started_at = time.monotonic()
    with source.open("rb") as input_file, destination.open("wb") as output_file:
        while chunk := input_file.read(1024 * 1024):
            output_file.write(chunk)
            copied += len(chunk)
            if on_progress is not None:
                elapsed = time.monotonic() - started_at
                on_progress(copied, int(copied / elapsed) if elapsed else 0)
