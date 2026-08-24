from __future__ import annotations

import asyncio
import base64
from collections import deque
import hashlib
from pathlib import Path
import tempfile
import unittest

from onedrive_staginger.config import SchedulerConfig
from onedrive_staginger.database import (
    MANIFEST_ROOT_ID_KEY,
    OneDriveItem,
    database,
    ensure_root_item,
    get_manifest_child,
    get_value,
    initialize_database,
    iter_manifest_children,
    persist_delta_page,
)
from onedrive_staginger.pipeline import ManifestFile, MigrationController, MigrationWorker, Transfer, TransferStatus
from onedrive_staginger.task import MigrationTask


class FakeDownloads:
    async def resume(self, *_: object) -> str:
        return "resume-gid"

    async def submit(self, *_: object) -> str:
        return "submit-gid"

    async def poll(self, _: str) -> tuple[str, str | None]:
        return "active", None


class PendingWorker:
    def __init__(self) -> None:
        self.submitted: list[str] = []

    async def reconcile(self, transfer: Transfer) -> TransferStatus:
        return transfer.status

    async def submit(self, transfer: Transfer) -> None:
        self.submitted.append(transfer.file.relative_path)
        transfer.status = TransferStatus.DOWNLOADING


class StatelessPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        initialize_database(self.root / "state.sqlite")

    def tearDown(self) -> None:
        database.close()
        self.directory.cleanup()

    def test_manifest_keeps_parent_links_without_paths_or_transfer_table(self) -> None:
        ensure_root_item("root")
        persist_delta_page(
            [
                {"drive_item_id": "media", "parent_drive_item_id": "root", "name": "Media", "is_folder": True},
                {"drive_item_id": "file", "parent_drive_item_id": "media", "name": "01.mkv", "is_file": True},
            ],
            "cursor",
        )

        self.assertEqual(get_value(MANIFEST_ROOT_ID_KEY), "root")
        self.assertEqual(get_manifest_child("root", "Media").drive_item_id, "media")  # type: ignore[union-attr]
        self.assertEqual([item.drive_item_id for item in iter_manifest_children("media")], ["file"])
        columns = {column.name for column in database.get_columns(OneDriveItem._meta.table_name)}
        self.assertNotIn("relative_path", columns)
        self.assertNotIn("transferrecord", database.get_tables())

    def test_worker_reconciles_final_file_without_database_transfer_state(self) -> None:
        data = b"verified"
        item = OneDriveItem.create(
            drive_item_id="file",
            name="01.mkv",
            is_file=True,
            size=len(data),
            hash_type="sha256",
            hash=base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"),
        )
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        final_path = task.dist_path("Anime/01.mkv")
        final_path.parent.mkdir(parents=True)
        final_path.write_bytes(data)
        transfer = Transfer(ManifestFile(item, "Anime/01.mkv"))

        status = asyncio.run(MigrationWorker(task, FakeDownloads()).reconcile(transfer))  # type: ignore[arg-type]

        self.assertEqual(status, TransferStatus.COMPLETE)
        self.assertEqual(transfer.status, TransferStatus.COMPLETE)

    def test_controller_streams_selected_subtree_with_relative_paths(self) -> None:
        ensure_root_item("root")
        media = OneDriveItem.create(
            drive_item_id="media", parent_drive_item_id="root", name="Media", is_folder=True
        )
        anime = OneDriveItem.create(
            drive_item_id="anime", parent_drive_item_id=media.drive_item_id, name="Anime", is_folder=True
        )
        OneDriveItem.create(
            drive_item_id="file", parent_drive_item_id=anime.drive_item_id, name="01.mkv", is_file=True
        )
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        controller = MigrationController(
            MigrationWorker(task, FakeDownloads()),  # type: ignore[arg-type]
            SchedulerConfig(max_downloads=1),
            task,
        )

        files = list(controller._iter_files())

        self.assertEqual([(file.item.drive_item_id, file.relative_path) for file in files], [("file", "Anime/01.mkv")])

    def test_controller_limits_submitted_tasks_to_available_slots(self) -> None:
        worker = PendingWorker()
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        controller = MigrationController(worker, SchedulerConfig(max_downloads=1), task)  # type: ignore[arg-type]
        first = OneDriveItem.create(drive_item_id="first", name="01.mkv", is_file=True)
        second = OneDriveItem.create(drive_item_id="second", name="02.mkv", is_file=True)
        queued = deque([Transfer(ManifestFile(first, "01.mkv")), Transfer(ManifestFile(second, "02.mkv"))])

        asyncio.run(controller._fill_slots(queued))

        self.assertEqual(worker.submitted, ["01.mkv"])
        self.assertEqual(queued[0].id, "second")


if __name__ == "__main__":
    unittest.main()
