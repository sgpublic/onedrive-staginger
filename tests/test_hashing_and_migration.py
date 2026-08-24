from __future__ import annotations

import asyncio
import base64
import hashlib
import tempfile
import unittest
from pathlib import Path

from onedrive_staginger.database import OneDriveItem, TransferRecord, TransferStatus, database, initialize_database
from onedrive_staginger.pipeline import MigrationWorker
from onedrive_staginger.task import MigrationTask
from onedrive_staginger.utils.hashing import file_hash


class FakeDownloads:
    def __init__(self) -> None:
        self.resumed: list[str] = []

    async def resume(self, item: OneDriveItem) -> str:
        self.resumed.append(item.drive_item_id)
        return "gid-1"


class HashingTests(unittest.TestCase):
    def test_returns_base64_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "file"
            path.write_bytes(b"hello")

            digest = file_hash(path, "sha256")

        self.assertEqual(digest, base64.b64encode(hashlib.sha256(b"hello").digest()).decode("ascii"))

    def test_implements_quick_xor_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "file"
            path.write_bytes(b"\x01\x02")

            digest = file_hash(path, "quickXorHash")

        expected = base64.b64encode(b"\x01\x10" + b"\0" * 10 + b"\x02" + b"\0" * 7).decode("ascii")
        self.assertEqual(digest, expected)


class MigrationWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        initialize_database(self.root / "state.sqlite")
        self.task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        self.data = b"verified file"
        self.item = OneDriveItem.create(
            drive_item_id="item-1",
            name="01.mkv",
            relative_path="Media/Anime/01.mkv",
            is_file=True,
            size=len(self.data),
            hash_type="sha256",
            hash=base64.b64encode(hashlib.sha256(self.data).digest()).decode("ascii"),
        )
        TransferRecord.create(drive_item_id=self.item.drive_item_id)
        self.downloads = FakeDownloads()
        self.worker = MigrationWorker(self.task, self.downloads)  # type: ignore[arg-type]

    def tearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    def test_reconcile_marks_verified_final_file_complete(self) -> None:
        final_path = self.task.download_path("Media/Anime/01.mkv")
        final_path.parent.mkdir(parents=True)
        final_path.write_bytes(self.data)

        status = asyncio.run(self.worker.reconcile(self.item, self._transfer()))

        self.assertEqual(status, TransferStatus.COMPLETE)
        transfer = self._transfer()
        self.assertEqual(transfer.status, TransferStatus.COMPLETE.value)
        self.assertEqual(transfer.verified_hash, self.item.hash)

    def test_reconcile_deletes_invalid_final_and_reuses_valid_temp(self) -> None:
        final_path = self.task.download_path("Media/Anime/01.mkv")
        final_path.parent.mkdir(parents=True)
        final_path.write_bytes(b"invalid")
        temp_path = self.task.temp_path("item-1", "01.mkv")
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(self.data)

        status = asyncio.run(self.worker.reconcile(self.item, self._transfer()))

        self.assertEqual(status, TransferStatus.STAGED)
        self.assertFalse(final_path.exists())
        self.assertTrue(temp_path.exists())

    def test_reconcile_resumes_temp_file_with_control_file(self) -> None:
        temp_path = self.task.temp_path("item-1", "01.mkv")
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(b"partial")
        Path(f"{temp_path}.aria2").write_text("control", encoding="utf-8")

        status = asyncio.run(self.worker.reconcile(self.item, self._transfer()))

        self.assertEqual(status, TransferStatus.DOWNLOADING)
        self.assertEqual(self.downloads.resumed, ["item-1"])

    def test_reconcile_deletes_invalid_partial_before_staging_temp(self) -> None:
        temp_path = self.task.temp_path("item-1", "01.mkv")
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(self.data)
        partial_path = self.task.partial_path(self.task.download_path("Media/Anime/01.mkv"))
        partial_path.parent.mkdir(parents=True)
        partial_path.write_bytes(b"invalid")

        status = asyncio.run(self.worker.reconcile(self.item, self._transfer()))

        self.assertEqual(status, TransferStatus.STAGED)
        self.assertFalse(partial_path.exists())

    def test_moves_staged_file_through_partial_then_removes_temp(self) -> None:
        temp_path = self.task.temp_path("item-1", "01.mkv")
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(self.data)

        status = asyncio.run(self.worker.move(self.item, self._transfer()))

        final_path = self.task.download_path("Media/Anime/01.mkv")
        self.assertEqual(status, TransferStatus.COMPLETE)
        self.assertEqual(final_path.read_bytes(), self.data)
        self.assertFalse(temp_path.exists())
        self.assertFalse(self.task.partial_path(final_path).exists())

    def _transfer(self) -> TransferRecord:
        return TransferRecord.get(TransferRecord.drive_item_id == self.item.drive_item_id)
