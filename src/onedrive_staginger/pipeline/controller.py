"""Top-level transfer loop consuming manifest-published files."""

from __future__ import annotations

import asyncio

from ..config import SchedulerConfig
from ..database import (
    MANIFEST_STATE_KEY,
    ManifestState,
    TransferStatus,
    get_transfer_items,
    get_value,
)
from .migration import MigrationWorker
from .scheduler import TransferScheduler


class MigrationError(ValueError):
    """The static manifest completed but one or more transfers could not finish."""


class MigrationController:
    """Reconcile local state, then schedule transfers until terminal completion."""

    def __init__(
        self,
        worker: MigrationWorker,
        scheduler: TransferScheduler,
        config: SchedulerConfig,
        manifest_root: str,
    ) -> None:
        self._worker = worker
        self._scheduler = scheduler
        self._poll_interval = config.poll_interval_seconds
        self._manifest_root = manifest_root

    async def run(self) -> None:
        """Run until every published transfer completes or terminal failures remain."""
        await self._reconcile_all()
        while True:
            await self._reconcile_pending()
            await self._poll_downloads()
            await self._check_downloads()
            await self._move_staged()
            await self._submit_pending()

            if self._manifest_is_complete():
                self._raise_failures_if_idle()
                if not get_transfer_items(
                    [
                        TransferStatus.PENDING,
                        TransferStatus.CHECKING,
                        TransferStatus.DOWNLOADING,
                        TransferStatus.STAGED,
                        TransferStatus.MOVING,
                    ],
                    self._manifest_root,
                ):
                    return

            await asyncio.sleep(self._poll_interval)

    async def _reconcile_all(self) -> None:
        for item, transfer in get_transfer_items(relative_root=self._manifest_root):
            await self._worker.reconcile(item, transfer)

    async def _reconcile_pending(self) -> None:
        for item, transfer in get_transfer_items([TransferStatus.PENDING], self._manifest_root):
            await self._worker.reconcile(item, transfer)

    async def _poll_downloads(self) -> None:
        for item, transfer in get_transfer_items([TransferStatus.DOWNLOADING], self._manifest_root):
            await self._scheduler.poll(item, transfer)

    async def _check_downloads(self) -> None:
        for item, transfer in get_transfer_items([TransferStatus.CHECKING], self._manifest_root):
            await self._worker.check_download(item, transfer)

    async def _move_staged(self) -> None:
        staged = get_transfer_items([TransferStatus.STAGED], self._manifest_root)
        if staged:
            await asyncio.gather(
                *(self._scheduler.move(item, transfer) for item, transfer in staged)
            )

    async def _submit_pending(self) -> None:
        for item, _ in get_transfer_items([TransferStatus.PENDING], self._manifest_root):
            if not await self._scheduler.submit(item):
                return

    @staticmethod
    def _manifest_is_complete() -> bool:
        return get_value(MANIFEST_STATE_KEY) == ManifestState.COMPLETE.value

    def _raise_failures_if_idle(self) -> None:
        active = get_transfer_items(
            [
                TransferStatus.PENDING,
                TransferStatus.CHECKING,
                TransferStatus.DOWNLOADING,
                TransferStatus.STAGED,
                TransferStatus.MOVING,
            ],
            self._manifest_root,
        )
        if active:
            return
        failures = get_transfer_items([TransferStatus.FAILED], self._manifest_root)
        if failures:
            details = "; ".join(
                f"{item.relative_path}: {transfer.last_error or 'unknown error'}"
                for item, transfer in failures
            )
            raise MigrationError(f"Migration failed for {len(failures)} file(s): {details}")
