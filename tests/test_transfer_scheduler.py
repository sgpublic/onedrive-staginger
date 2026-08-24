from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from onedrive_staginger.config import SchedulerConfig
from onedrive_staginger.database import OneDriveItem, TransferRecord, TransferStatus, database, initialize_database
from onedrive_staginger.pipeline import TransferScheduler


class FakeDownloads:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def submit(self, item: OneDriveItem) -> None:
        self.submitted.append(item.drive_item_id)
        TransferRecord.update(status=TransferStatus.DOWNLOADING.value).where(
            TransferRecord.drive_item_id == item.drive_item_id
        ).execute()
        self.started.set()
        await self.release.wait()

    async def poll(self, _: TransferRecord) -> str:
        return "active"


class FakeWorker:
    def __init__(self) -> None:
        self.active_moves = 0
        self.max_active_moves = 0

    async def move(self, _: OneDriveItem, __: TransferRecord) -> TransferStatus:
        self.active_moves += 1
        self.max_active_moves = max(self.max_active_moves, self.active_moves)
        await asyncio.sleep(0)
        self.active_moves -= 1
        return TransferStatus.COMPLETE

    async def check_download(self, _: OneDriveItem, __: TransferRecord) -> TransferStatus:
        return TransferStatus.STAGED


class TransferSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        initialize_database(Path(self.temp_dir.name) / "state.sqlite")
        self.items = [
            OneDriveItem.create(drive_item_id=f"item-{index}", name=f"{index}.mkv", is_file=True)
            for index in range(2)
        ]
        for item in self.items:
            TransferRecord.create(drive_item_id=item.drive_item_id)

    async def asyncTearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    async def test_limits_concurrent_download_submissions(self) -> None:
        downloads = FakeDownloads()
        scheduler = TransferScheduler(
            downloads,  # type: ignore[arg-type]
            FakeWorker(),  # type: ignore[arg-type]
            SchedulerConfig(max_downloads=1),
        )

        first = asyncio.create_task(scheduler.submit(self.items[0]))
        await downloads.started.wait()
        second = asyncio.create_task(scheduler.submit(self.items[1]))
        await asyncio.sleep(0)
        downloads.release.set()

        self.assertTrue(await first)
        self.assertFalse(await second)
        self.assertEqual(downloads.submitted, ["item-0"])

    async def test_limits_concurrent_moves(self) -> None:
        worker = FakeWorker()
        scheduler = TransferScheduler(
            FakeDownloads(),  # type: ignore[arg-type]
            worker,  # type: ignore[arg-type]
            SchedulerConfig(max_moves=1),
        )
        transfers = [TransferRecord.get(TransferRecord.drive_item_id == item.drive_item_id) for item in self.items]

        results = await asyncio.gather(
            *(scheduler.move(item, transfer) for item, transfer in zip(self.items, transfers, strict=True))
        )

        self.assertEqual(results, [TransferStatus.COMPLETE, TransferStatus.COMPLETE])
        self.assertEqual(worker.max_active_moves, 1)
