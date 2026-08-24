from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest

from onedrive_staginger.database import (
    DELTA_CURSOR_KEY,
    MANIFEST_STATE_KEY,
    ManifestState,
    OneDriveItem,
    TransferRecord,
    database,
    get_value,
    initialize_database,
    KeyValue,
)
from onedrive_staginger.onedrive import DeltaPage, DriveItem, FileHashes
from onedrive_staginger.pipeline import ManifestPipeline


class FakeOneDriveClient:
    def __init__(self, pages: list[DeltaPage]) -> None:
        self.pages = pages
        self.cursors: list[str | None] = []

    async def get_delta_page(
        self, drive_id: str, root_item_id: str, cursor: str | None = None
    ) -> DeltaPage:
        self.cursors.append(cursor)
        return self.pages.pop(0)


class ManifestPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        initialize_database(Path(self.temp_dir.name) / "state.sqlite")

    def tearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    def test_publishes_path_ready_files_before_manifest_completion(self) -> None:
        file = DriveItem(
            id="file",
            name="01.mkv",
            size=123,
            e_tag="etag",
            last_modified_date_time=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
            parent_id="folder",
            is_file=True,
            is_folder=False,
            is_deleted=False,
            hashes=FileHashes(sha256_hash="hash"),
        )
        folder = DriveItem(
            id="folder",
            name="Anime",
            size=0,
            e_tag="folder-etag",
            last_modified_date_time=None,
            parent_id="root",
            is_file=False,
            is_folder=True,
            is_deleted=False,
        )
        client = FakeOneDriveClient(
            [
                DeltaPage(items=(file,), next_link="next-page", delta_link=None),
                DeltaPage(items=(folder,), next_link=None, delta_link="final-cursor"),
            ]
        )
        asyncio.run(ManifestPipeline(client, "drive", "root").run())

        item = OneDriveItem.get(OneDriveItem.drive_item_id == "file")
        self.assertEqual(item.relative_path, "Anime/01.mkv")
        self.assertEqual(get_value(MANIFEST_STATE_KEY), ManifestState.COMPLETE.value)
        self.assertEqual(TransferRecord.get(TransferRecord.drive_item_id == "file").status, "pending")
        self.assertEqual(client.cursors, [None, "next-page"])

    def test_resumes_sync_from_persisted_delta_cursor(self) -> None:
        KeyValue.create(key=DELTA_CURSOR_KEY, value="saved-cursor")
        client = FakeOneDriveClient(
            [DeltaPage(items=(), next_link=None, delta_link="new-cursor")]
        )

        asyncio.run(ManifestPipeline(client, "drive", "root").run())

        self.assertEqual(client.cursors, ["saved-cursor"])
        self.assertEqual(get_value(DELTA_CURSOR_KEY), "new-cursor")

    def test_logs_page_progress(self) -> None:
        client = FakeOneDriveClient(
            [DeltaPage(items=(), next_link=None, delta_link="new-cursor")]
        )

        with self.assertLogs("onedrive_staginger.pipeline.manifest", level="INFO") as logs:
            asyncio.run(ManifestPipeline(client, "drive", "root").run())

        self.assertIn("Synchronizing OneDrive manifest", logs.output[0])
        self.assertIn("Synced manifest page 1: 0 item(s), 0 path(s) resolved (complete)", logs.output[1])

    def test_marks_manifest_enumerating_before_request_failure(self) -> None:
        class FailingOneDriveClient:
            async def get_delta_page(self, *_: object) -> DeltaPage:
                raise RuntimeError("network failed")

        with self.assertRaisesRegex(RuntimeError, "network failed"):
            asyncio.run(ManifestPipeline(FailingOneDriveClient(), "drive", "root").run())  # type: ignore[arg-type]

        self.assertEqual(get_value(MANIFEST_STATE_KEY), ManifestState.ENUMERATING.value)


if __name__ == "__main__":
    unittest.main()
