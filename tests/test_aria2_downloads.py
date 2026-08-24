from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from onedrive_staginger.aria2 import Aria2DownloadManager
from onedrive_staginger.config import SchedulerConfig
from onedrive_staginger.database import OneDriveItem, TransferRecord, TransferStatus, database, initialize_database
from onedrive_staginger.task import MigrationTask


class FakeAria2Client:
    def __init__(self, *, gid: str = "gid-1", status: dict[str, str] | None = None) -> None:
        self.gid = gid
        self.status = status or {"status": "active"}
        self.added: tuple[list[str], dict[str, str]] | None = None

    async def addUri(self, urls: list[str], options: dict[str, str]) -> str:
        self.added = (urls, options)
        return self.gid

    async def tellStatus(self, _: str, __: list[str]) -> dict[str, str]:
        return self.status


class FakeOneDriveClient:
    def __init__(self) -> None:
        self.requests: list[tuple[str, str]] = []

    async def get_download_url(self, drive_id: str, item_id: str) -> str:
        self.requests.append((drive_id, item_id))
        return f"https://download.test/{item_id}"


class Aria2DownloadManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        initialize_database(root / "state.sqlite")
        self.task = MigrationTask(root / "temp", root / "dist", "/Media")
        self.item = OneDriveItem.create(
            drive_item_id="item-1",
            name="01.mkv",
            relative_path="Anime/01.mkv",
            is_file=True,
        )
        TransferRecord.create(drive_item_id=self.item.drive_item_id)
        self.aria2 = FakeAria2Client()
        self.onedrive = FakeOneDriveClient()
        self.manager = Aria2DownloadManager(
            self.aria2,  # type: ignore[arg-type]
            self.onedrive,  # type: ignore[arg-type]
            "drive-1",
            self.task,
            SchedulerConfig(connections_per_file=8, disable_http2=True),
        )

    async def asyncTearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    async def test_submits_flat_temp_file_and_persists_gid(self) -> None:
        gid = await self.manager.submit(self.item)

        self.assertEqual(gid, "gid-1")
        self.assertEqual(self.onedrive.requests, [("drive-1", "item-1")])
        self.assertEqual(
            self.aria2.added,
            (
                ["https://download.test/item-1"],
                {
                    "dir": str(self.task.temp_dir),
                    "out": "item-1-01.mkv",
                    "split": "8",
                    "max-connection-per-server": "8",
                    "enable-http2": "false",
                },
            ),
        )
        transfer = TransferRecord.get(TransferRecord.drive_item_id == "item-1")
        self.assertEqual(transfer.status, TransferStatus.DOWNLOADING.value)
        self.assertEqual(transfer.aria2_gid, "gid-1")

    async def test_resume_requests_a_fresh_url(self) -> None:
        await self.manager.resume(self.item)

        self.assertEqual(self.onedrive.requests, [("drive-1", "item-1")])

    async def test_poll_marks_complete_transfer_for_checking(self) -> None:
        TransferRecord.update(
            status=TransferStatus.DOWNLOADING.value, aria2_gid="gid-1"
        ).where(TransferRecord.drive_item_id == "item-1").execute()
        self.aria2.status = {"status": "complete"}
        transfer = TransferRecord.get(TransferRecord.drive_item_id == "item-1")

        status = await self.manager.poll(transfer)

        self.assertEqual(status, "complete")
        updated = TransferRecord.get(TransferRecord.drive_item_id == "item-1")
        self.assertEqual(updated.status, TransferStatus.CHECKING.value)

    async def test_poll_persists_aria2_errors(self) -> None:
        TransferRecord.update(
            status=TransferStatus.DOWNLOADING.value, aria2_gid="gid-1"
        ).where(TransferRecord.drive_item_id == "item-1").execute()
        self.aria2.status = {
            "status": "error",
            "errorCode": "3",
            "errorMessage": "Resource not found",
        }
        transfer = TransferRecord.get(TransferRecord.drive_item_id == "item-1")

        await self.manager.poll(transfer)

        updated = TransferRecord.get(TransferRecord.drive_item_id == "item-1")
        self.assertEqual(updated.status, TransferStatus.FAILED.value)
        self.assertEqual(updated.last_error, "aria2 error 3: Resource not found")
