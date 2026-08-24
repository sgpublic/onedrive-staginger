"""Stream one manifest subtree while respecting download backpressure."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterator
import logging

from ..config import SchedulerConfig
from ..database import get_manifest_child, get_manifest_root_id, iter_manifest_children
from ..task import MigrationTask
from .migration import ManifestFile, MigrationWorker, Transfer, TransferStatus


logger = logging.getLogger(__name__)


class MigrationError(ValueError):
    """One or more transfers could not finish."""


class MigrationController:
    """Stream manifest files only while an aria2 download slot is available."""

    def __init__(self, worker: MigrationWorker, config: SchedulerConfig, task: MigrationTask) -> None:
        self._worker = worker
        self._max_downloads = config.max_downloads
        self._max_moves = config.max_moves
        self._poll_interval = config.poll_interval_seconds
        self._task = task
        self._active: list[Transfer] = []
        self._staged: deque[Transfer] = deque()
        self._moves: dict[asyncio.Task[TransferStatus], Transfer] = {}
        self._failures: list[Transfer] = []
        self._blocked_logged = False

    async def run(self) -> None:
        files = self._iter_files()
        exhausted = False
        while True:
            await self._poll_downloads()
            await self._collect_moves()
            self._start_moves()

            if not exhausted:
                exhausted = await self._fill_download_slots(files)

            if exhausted and not self._active and not self._staged and not self._moves:
                self._raise_failures()
                return

            if len(self._active) >= self._max_downloads and not self._blocked_logged:
                logger.info("Download slots full; pausing manifest traversal")
                self._blocked_logged = True
            if self._active or self._moves:
                await asyncio.sleep(self._poll_interval)

    def _iter_files(self) -> Iterator[ManifestFile]:
        root_id = get_manifest_root_id()
        root_path = self._task.remote_root_path.strip("/")
        for component in filter(None, root_path.split("/")):
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

    async def _fill_download_slots(self, files: Iterator[ManifestFile]) -> bool:
        while len(self._active) < self._max_downloads:
            try:
                transfer = Transfer(next(files))
            except StopIteration:
                return True
            status = await self._worker.reconcile(transfer)
            if status == TransferStatus.COMPLETE:
                continue
            if status == TransferStatus.FAILED:
                self._failures.append(transfer)
                continue
            if status == TransferStatus.STAGED:
                self._staged.append(transfer)
                self._start_moves()
                continue
            if status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
            self._active.append(transfer)
            self._blocked_logged = False
        return False

    async def _poll_downloads(self) -> None:
        for transfer in list(self._active):
            status = await self._worker.poll(transfer)
            if status == TransferStatus.DOWNLOADING:
                continue
            self._active.remove(transfer)
            self._blocked_logged = False
            if status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
                self._active.append(transfer)
            elif status == TransferStatus.STAGED:
                self._staged.append(transfer)
            elif status == TransferStatus.FAILED:
                self._failures.append(transfer)

    def _start_moves(self) -> None:
        while self._staged and len(self._moves) < self._max_moves:
            transfer = self._staged.popleft()
            self._moves[asyncio.create_task(self._worker.move(transfer))] = transfer

    async def _collect_moves(self) -> None:
        done = [task for task in self._moves if task.done()]
        for task in done:
            transfer = self._moves.pop(task)
            status = await task
            if status == TransferStatus.STAGED:
                # A failed destination verification is retried without re-downloading.
                self._staged.append(transfer)
            elif status == TransferStatus.PENDING:
                await self._worker.submit(transfer)
                self._active.append(transfer)
            elif status == TransferStatus.DOWNLOADING:
                self._active.append(transfer)
            elif status == TransferStatus.FAILED:
                self._failures.append(transfer)

    def _raise_failures(self) -> None:
        if not self._failures:
            return
        details = "; ".join(
            f"{transfer.file.relative_path}: {transfer.last_error or 'unknown error'}"
            for transfer in self._failures
        )
        raise MigrationError(f"Migration failed for {len(self._failures)} file(s): {details}")
