from __future__ import annotations

import asyncio
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

from onedrive_staginger.config import SchedulerConfig
from onedrive_staginger.database import (
    MANIFEST_STATE_KEY,
    KeyValue,
    ManifestState,
    OneDriveItem,
    TransferRecord,
    TransferStatus,
    database,
    initialize_database,
)
from onedrive_staginger.pipeline import MigrationController, MigrationError, MigrationWorker, TransferScheduler
from onedrive_staginger.task import MigrationTask


class FakeDownloads:
    async def submit(self, item: OneDriveItem) -> None:
        TransferRecord.update(status=TransferStatus.DOWNLOADING.value, aria2_gid="gid").where(
            TransferRecord.drive_item_id == item.drive_item_id
        ).execute()

    async def poll(self, _: TransferRecord) -> str:
        return "active"

    async def resume(self, item: OneDriveItem) -> str:
        await self.submit(item)
        return "gid"


class MigrationControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        initialize_database(root / "state.sqlite")
        self.task = MigrationTask(root / "temp", root / "dist", "/Media")
        self.downloads = FakeDownloads()
        self.worker = MigrationWorker(self.task, self.downloads)  # type: ignore[arg-type]
        self.scheduler = TransferScheduler(
            self.downloads,  # type: ignore[arg-type]
            self.worker,
            SchedulerConfig(poll_interval_seconds=1),
        )
        KeyValue.create(key=MANIFEST_STATE_KEY, value=ManifestState.COMPLETE.value)

    def tearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    def test_reconciles_staged_temp_and_exits_after_move(self) -> None:
        data = b"verified"
        item = self._item(data)
        temp_path = self.task.temp_path(item.drive_item_id, item.name)
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(data)

        asyncio.run(self._controller().run())

        final_path = self.task.download_path(item.relative_path)
        self.assertEqual(final_path.read_bytes(), data)
        self.assertEqual(self._transfer(item).status, TransferStatus.COMPLETE.value)

    def test_reports_failed_metadata_after_manifest_completion(self) -> None:
        item = OneDriveItem.create(
            drive_item_id="missing-hash",
            name="missing.mkv",
            relative_path="Media/missing.mkv",
            is_file=True,
            size=1,
        )
        TransferRecord.create(drive_item_id=item.drive_item_id)

        with self.assertRaisesRegex(MigrationError, "missing.mkv"):
            asyncio.run(self._controller().run())

        self.assertEqual(self._transfer(item).status, TransferStatus.FAILED.value)

    def _controller(self) -> MigrationController:
        return MigrationController(
            self.worker,
            self.scheduler,
            SchedulerConfig(poll_interval_seconds=1),
            self.task.manifest_root,
        )

    def _item(self, data: bytes) -> OneDriveItem:
        item = OneDriveItem.create(
            drive_item_id="item-1",
            name="01.mkv",
            relative_path="Media/Anime/01.mkv",
            is_file=True,
            size=len(data),
            hash_type="sha256",
            hash=base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"),
        )
        TransferRecord.create(drive_item_id=item.drive_item_id)
        return item

    @staticmethod
    def _transfer(item: OneDriveItem) -> TransferRecord:
        return TransferRecord.get(TransferRecord.drive_item_id == item.drive_item_id)
