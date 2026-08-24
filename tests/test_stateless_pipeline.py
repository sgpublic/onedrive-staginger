from __future__ import annotations

import asyncio
import base64
from collections import deque
import hashlib
from io import StringIO
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from rich.console import Console

from onedrive_staginger.config import SchedulerConfig
from onedrive_staginger.aria2 import Aria2DownloadManager
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
from onedrive_staginger.pipeline.migration import _remote_mtime_ns
from onedrive_staginger.progress import TransferProgress, _byte_text
from onedrive_staginger.task import MigrationTask
from onedrive_staginger.utils.hashing import file_hash


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


class CompletedWorker:
    async def reconcile(
        self, transfer: Transfer, *_: object
    ) -> TransferStatus:
        transfer.status = TransferStatus.COMPLETE
        return transfer.status


class RecordingProgress:
    def __init__(self) -> None:
        self.started: tuple[int, int] | None = None
        self.completed: list[tuple[str, int]] = []

    def start(self, total_bytes: int, total_files: int) -> None:
        self.started = total_bytes, total_files

    def complete_file(self, transfer_id: str, total: int) -> None:
        self.completed.append((transfer_id, total))

    def finish_verification(self, _: str) -> None:
        pass


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

    def test_verification_skips_hash_when_size_and_remote_mtime_match(self) -> None:
        data = b"verified"
        remote_mtime = "2026-08-24T12:00:00Z"
        item = self._hashed_item(data, remote_mtime)
        path = self.root / "file"
        path.write_bytes(data)
        mtime_ns = _remote_mtime_ns(remote_mtime)
        os.utime(path, ns=(mtime_ns, mtime_ns))
        worker = MigrationWorker(MigrationTask(self.root / "temp", self.root / "dist", "/Media"), FakeDownloads())  # type: ignore[arg-type]

        with patch("onedrive_staginger.pipeline.migration.file_hash") as file_hash_mock:
            verified = asyncio.run(worker._verify(item, path))

        self.assertTrue(verified)
        file_hash_mock.assert_not_called()

    def test_verification_sets_remote_mtime_only_after_hash_matches(self) -> None:
        data = b"verified"
        remote_mtime = "2026-08-24T12:00:00+00:00"
        item = self._hashed_item(data, remote_mtime)
        path = self.root / "file"
        path.write_bytes(data)
        worker = MigrationWorker(MigrationTask(self.root / "temp", self.root / "dist", "/Media"), FakeDownloads())  # type: ignore[arg-type]

        verified = asyncio.run(worker._verify(item, path))

        self.assertTrue(verified)
        self.assertEqual(path.stat().st_mtime_ns, _remote_mtime_ns(remote_mtime))

    def test_verification_logs_expected_and_actual_values(self) -> None:
        data = b"verified"
        item = self._hashed_item(data, "2026-08-24T12:00:00Z")
        path = self.root / "file"
        path.write_bytes(b"corrupt!")
        worker = MigrationWorker(MigrationTask(self.root / "temp", self.root / "dist", "/Media"), FakeDownloads())  # type: ignore[arg-type]

        with self.assertLogs("onedrive_staginger.pipeline.migration", level="INFO") as logs:
            verified = asyncio.run(worker._verify(item, path))

        output = "\n".join(logs.output)
        self.assertFalse(verified)
        self.assertIn("文件修改时间不匹配", output)
        self.assertIn("预期修改时间", output)
        self.assertIn("实际修改时间", output)
        self.assertIn("文件哈希不匹配", output)
        self.assertIn("算法 sha256", output)
        self.assertIn(item.hash, output)

    def test_size_mismatch_log_includes_expected_and_actual_bytes(self) -> None:
        item = self._hashed_item(b"verified", "2026-08-24T12:00:00Z")
        path = self.root / "file"
        path.write_bytes(b"short")
        worker = MigrationWorker(MigrationTask(self.root / "temp", self.root / "dist", "/Media"), FakeDownloads())  # type: ignore[arg-type]

        with self.assertLogs("onedrive_staginger.pipeline.migration", level="WARNING") as logs:
            verified = asyncio.run(worker._verify(item, path))

        self.assertFalse(verified)
        self.assertIn("预期 8 字节，实际 5 字节", logs.output[0])

    def test_fast_verification_does_not_report_progress(self) -> None:
        data = b"verified"
        remote_mtime = "2026-08-24T12:00:00Z"
        item = self._hashed_item(data, remote_mtime)
        path = self.root / "file"
        path.write_bytes(data)
        mtime_ns = _remote_mtime_ns(remote_mtime)
        os.utime(path, ns=(mtime_ns, mtime_ns))
        starts: list[None] = []
        progress: list[int] = []
        worker = MigrationWorker(MigrationTask(self.root / "temp", self.root / "dist", "/Media"), FakeDownloads())  # type: ignore[arg-type]

        verified = asyncio.run(worker._verify(item, path, lambda: starts.append(None), progress.append))

        self.assertTrue(verified)
        self.assertEqual(starts, [])
        self.assertEqual(progress, [])

    def test_download_verification_reports_hash_progress(self) -> None:
        data = b"verified download"
        item = self._hashed_item(data, "2026-08-24T12:00:00Z")
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        temp_path = task.temp_path(item.drive_item_id, item.name)
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(data)
        transfer = Transfer(ManifestFile(item, "01.mkv"), status=TransferStatus.DOWNLOADING)
        starts: list[None] = []
        progress: list[int] = []
        worker = MigrationWorker(task, FakeDownloads())  # type: ignore[arg-type]

        status = asyncio.run(
            worker.check_download(transfer, lambda: starts.append(None), progress.append)
        )

        self.assertEqual(status, TransferStatus.STAGED)
        self.assertEqual(starts, [None])
        self.assertEqual(progress[-1], len(data))

    def test_fast_download_verification_skips_hash_when_size_matches(self) -> None:
        data = b"verified download"
        item = self._hashed_item(data, "2026-08-24T12:00:00Z")
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        temp_path = task.temp_path(item.drive_item_id, item.name)
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(data)
        transfer = Transfer(ManifestFile(item, "01.mkv"), status=TransferStatus.DOWNLOADING)
        starts: list[None] = []
        progress: list[int] = []
        worker = MigrationWorker(
            task, FakeDownloads(), fast_verify_after_download=True  # type: ignore[arg-type]
        )

        with patch("onedrive_staginger.pipeline.migration.file_hash") as file_hash_mock:
            status = asyncio.run(
                worker.check_download(transfer, lambda: starts.append(None), progress.append)
            )

        self.assertEqual(status, TransferStatus.STAGED)
        self.assertEqual(starts, [])
        self.assertEqual(progress, [])
        file_hash_mock.assert_not_called()

    def test_verified_download_is_not_checked_again_while_moving(self) -> None:
        data = b"verified download"
        item = self._hashed_item(data, "2026-08-24T12:00:00Z")
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        temp_path = task.temp_path(item.drive_item_id, item.name)
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(data)
        transfer = Transfer(ManifestFile(item, "01.mkv"), status=TransferStatus.DOWNLOADING)
        worker = MigrationWorker(task, FakeDownloads())  # type: ignore[arg-type]

        self.assertEqual(asyncio.run(worker.check_download(transfer)), TransferStatus.STAGED)
        with patch.object(worker, "_verify", wraps=worker._verify) as verify_mock:
            status = asyncio.run(worker.move(transfer))

        self.assertEqual(status, TransferStatus.COMPLETE)
        verify_mock.assert_not_awaited()
        self.assertEqual(task.dist_path("01.mkv").read_bytes(), data)

    def test_unverified_staged_file_is_checked_once_while_moving(self) -> None:
        data = b"staged file"
        item = self._hashed_item(data, "2026-08-24T12:00:00Z")
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        temp_path = task.temp_path(item.drive_item_id, item.name)
        temp_path.parent.mkdir(parents=True)
        temp_path.write_bytes(data)
        transfer = Transfer(ManifestFile(item, "01.mkv"), status=TransferStatus.STAGED)
        worker = MigrationWorker(task, FakeDownloads())  # type: ignore[arg-type]

        with patch.object(worker, "_verify", wraps=worker._verify) as verify_mock:
            status = asyncio.run(worker.move(transfer))

        self.assertEqual(status, TransferStatus.COMPLETE)
        verify_mock.assert_awaited_once()
        self.assertTrue(transfer.verified)
        self.assertEqual(task.dist_path("01.mkv").read_bytes(), data)

    def test_file_hash_reports_each_chunk_for_all_supported_hashes(self) -> None:
        path = self.root / "file"
        path.write_bytes(b"abcdefgh")

        with patch("onedrive_staginger.utils.hashing.CHUNK_SIZE", 3):
            for hash_type in ("sha1", "sha256", "quickXorHash"):
                with self.subTest(hash_type=hash_type):
                    progress: list[int] = []
                    file_hash(path, hash_type, progress.append)
                    self.assertEqual(progress, [3, 6, 8])

    def test_quick_xor_hash_matches_onedrive_reference_value(self) -> None:
        path = self.root / "file"
        path.write_bytes(b"hello world")

        self.assertEqual(file_hash(path, "quickXorHash"), "aCgDG9jwBhDc4Q1yawMZAAAAAAA=")

    def test_invalid_remote_mtime_falls_back_without_crashing(self) -> None:
        self.assertIsNone(_remote_mtime_ns("not-a-time"))

    def test_total_progress_byte_format_uses_two_decimal_places(self) -> None:
        self.assertEqual(_byte_text(512), "512.00 B")
        self.assertEqual(_byte_text(1536), "1.50 KiB")
        self.assertEqual(_byte_text(2.5 * 1024**3), "2.50 GiB")

    def test_long_path_is_ellipsized_before_progress_details(self) -> None:
        console = Console(file=StringIO(), width=100, record=True)
        progress = TransferProgress(console)
        path = "nested/" * 20 + "movie.mkv"
        progress._slots.add_task(
            f"下载中：{path}", total=1024, completed=512, speed="50.0 MiB/s"
        )

        console.print(progress._slots)
        output = console.export_text()

        self.assertNotIn(path, output)
        self.assertIn("…", output)
        self.assertIn("50.0 MiB/s", output)
        self.assertIn("0.5/1.0 kB", output)
        self.assertEqual(output.count("\n"), 1)

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

    def test_controller_counts_previously_completed_files_in_total_progress(self) -> None:
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        progress = RecordingProgress()
        controller = MigrationController(
            CompletedWorker(), SchedulerConfig(max_downloads=1), task, progress  # type: ignore[arg-type]
        )
        files = [
            ManifestFile(OneDriveItem.create(drive_item_id="first", name="01.mkv", is_file=True, size=3), "01.mkv"),
            ManifestFile(OneDriveItem.create(drive_item_id="second", name="02.mkv", is_file=True, size=5), "02.mkv"),
        ]
        controller._iter_files = lambda: iter(files)  # type: ignore[method-assign]

        asyncio.run(controller.run())

        self.assertEqual(progress.started, (8, 2))
        self.assertEqual(progress.completed, [("first", 3), ("second", 5)])

    def test_aria2_options_cap_split_size_by_file_size_per_connection(self) -> None:
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        manager = Aria2DownloadManager(
            None,
            None,
            "drive",
            task,
            SchedulerConfig(min_split_size=1024 * 1024, max_split_size=4 * 1024 * 1024),  # type: ignore[arg-type]
        )

        options = manager._options(task.temp_path("item", "01.mkv"), 16 * 1024 * 1024)

        self.assertEqual(options["min-split-size"], str(2 * 1024 * 1024))

    def test_aria2_options_keep_configured_minimum_split_size_for_small_files(self) -> None:
        task = MigrationTask(self.root / "temp", self.root / "dist", "/Media")
        manager = Aria2DownloadManager(
            None,
            None,
            "drive",
            task,
            SchedulerConfig(min_split_size=1024 * 1024, max_split_size=4 * 1024 * 1024),  # type: ignore[arg-type]
        )

        options = manager._options(task.temp_path("item", "01.mkv"), 4 * 1024 * 1024)

        self.assertEqual(options["min-split-size"], str(1024 * 1024))

    @staticmethod
    def _hashed_item(data: bytes, remote_mtime: str) -> OneDriveItem:
        return OneDriveItem.create(
            drive_item_id=f"item-{OneDriveItem.select().count()}",
            name="01.mkv",
            is_file=True,
            size=len(data),
            hash_type="sha256",
            hash=base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"),
            remote_mtime=remote_mtime,
        )


if __name__ == "__main__":
    unittest.main()
