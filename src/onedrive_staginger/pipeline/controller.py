"""Pre-scan one manifest subtree and execute bounded transfer slots."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterator
import logging

from ..config import SchedulerConfig
from ..database import get_manifest_child, get_manifest_root_id, iter_manifest_children
from ..progress import TransferProgress
from ..task import MigrationTask
from .migration import ManifestFile, MigrationWorker, Transfer, TransferStatus


logger = logging.getLogger(__name__)


class MigrationError(ValueError):
    """One or more transfers could not finish."""


class MigrationController:
    """Hold each bounded transfer slot until its final rename has completed."""

    def __init__(
        self,
        worker: MigrationWorker,
        config: SchedulerConfig,
        task: MigrationTask,
        progress: TransferProgress | None = None,
    ) -> None:
        self._worker = worker
        self._max_slots = config.max_downloads
        self._max_moves = config.max_moves
        self._poll_interval = config.poll_interval_seconds
        self._task = task
        self._progress = progress
        self._slots: dict[str, Transfer] = {}
        self._moves: dict[asyncio.Task[TransferStatus], Transfer] = {}
        self._failures: list[Transfer] = []
        self._download_ids: set[str] = set()

    async def run(self) -> None:
        logger.info("正在将清单子树读入内存：%s", self._task.remote_root_path)
        transfers = [Transfer(file) for file in self._iter_files()]
        logger.info("清单扫描完成：%d 个文件", len(transfers))
        for transfer in transfers:
            status = await self._worker.reconcile(transfer)
            if status == TransferStatus.FAILED:
                self._failures.append(transfer)

        queued = deque(
            transfer
            for transfer in transfers
            if transfer.status not in {TransferStatus.COMPLETE, TransferStatus.FAILED}
        )
        self._download_ids = {
            transfer.id for transfer in queued if transfer.status == TransferStatus.PENDING
        }
        total_bytes = sum(
            transfer.file.item.size or 0 for transfer in queued if transfer.id in self._download_ids
        )
        logger.info(
            "已准备 %d 个网络下载任务，共 %d 字节",
            len(self._download_ids),
            total_bytes,
        )
        if self._progress is not None:
            self._progress.start(total_bytes, len(self._download_ids))

        while queued or self._slots:
            await self._collect_moves()
            await self._poll_downloads()
            self._start_moves()
            await self._fill_slots(queued)
            if self._slots:
                await asyncio.sleep(self._poll_interval)

        self._raise_failures()

    def _iter_files(self) -> Iterator[ManifestFile]:
        root_id = get_manifest_root_id()
        for component in filter(None, self._task.remote_root_path.strip("/").split("/")):
            child = get_manifest_child(root_id, component)
            if child is None or not child.is_folder:
                raise ValueError(f"Remote manifest directory not found: {self._task.remote_root_path}")
            root_id = child.drive_item_id
        yield from self._walk(root_id, "")

    def _walk(self, parent_id: str, relative_parent: str) -> Iterator[ManifestFile]:
        for child in iter_manifest_children(parent_id):
            if child.name is None:
                continue
            relative_path = child.name if not relative_parent else f"{relative_parent}/{child.name}"
            if child.is_file:
                yield ManifestFile(child, relative_path)
            elif child.is_folder:
                yield from self._walk(child.drive_item_id, relative_path)

    async def _fill_slots(self, queued: deque[Transfer]) -> None:
        while queued and len(self._slots) < self._max_slots:
            transfer = queued.popleft()
            self._slots[transfer.id] = transfer
            counts_download = transfer.id in self._download_ids
            if self._progress is not None:
                self._progress.start_slot(
                    transfer.id,
                    f"准备传输：{transfer.file.relative_path}",
                    transfer.file.item.size or 0,
                    transfer.downloaded_bytes,
                    counts_download=counts_download,
                )
            if transfer.status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
            self._start_moves()

    async def _poll_downloads(self) -> None:
        for transfer in list(self._slots.values()):
            if transfer.status != TransferStatus.DOWNLOADING:
                continue
            status = await self._worker.poll(transfer)
            if self._progress is not None:
                self._progress.update_download(
                    transfer.id,
                    transfer.downloaded_bytes,
                    transfer.file.item.size or 0,
                    transfer.download_speed,
                )
            if status == TransferStatus.STAGED:
                self._start_moves()
            elif status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
            elif status == TransferStatus.FAILED:
                self._fail_slot(transfer)

    def _start_moves(self) -> None:
        for transfer in self._slots.values():
            if len(self._moves) >= self._max_moves:
                return
            if transfer.status != TransferStatus.STAGED:
                continue
            if self._progress is not None:
                self._progress.start_move(transfer.id, transfer.file.item.size or 0)
                loop = asyncio.get_running_loop()
                on_progress = lambda completed, speed, transfer=transfer: loop.call_soon_threadsafe(
                    self._progress.update_move, transfer.id, completed, speed
                )
            else:
                on_progress = None
            self._moves[asyncio.create_task(self._worker.move(transfer, on_progress))] = transfer

    async def _collect_moves(self) -> None:
        for task in [task for task in self._moves if task.done()]:
            transfer = self._moves.pop(task)
            status = await task
            if status == TransferStatus.COMPLETE:
                self._complete_slot(transfer)
            elif status == TransferStatus.STAGED:
                continue
            elif status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
            elif status == TransferStatus.DOWNLOADING:
                continue
            else:
                self._fail_slot(transfer)

    def _complete_slot(self, transfer: Transfer) -> None:
        self._slots.pop(transfer.id)
        if self._progress is not None:
            self._progress.finish_slot(transfer.id, counts_download=transfer.id in self._download_ids)

    def _fail_slot(self, transfer: Transfer) -> None:
        self._slots.pop(transfer.id, None)
        self._failures.append(transfer)
        if self._progress is not None:
            self._progress.finish_slot(transfer.id, counts_download=False)

    def _raise_failures(self) -> None:
        if not self._failures:
            return
        details = "; ".join(
            f"{transfer.file.relative_path}: {transfer.last_error or 'unknown error'}"
            for transfer in self._failures
        )
        raise MigrationError(f"Migration failed for {len(self._failures)} file(s): {details}")
