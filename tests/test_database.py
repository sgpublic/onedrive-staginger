from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest

from onedrive_staginger.database import (
    DELTA_CURSOR_KEY,
    KeyValue,
    OneDriveItem,
    TransferRecord,
    TransferStatus,
    claim_ready_transfers,
    database,
    ensure_root_item,
    get_transfer_items,
    initialize_database,
    persist_delta_page,
    resolve_relative_paths,
)


class DatabaseSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        initialize_database(Path(self.temp_dir.name) / "state.sqlite")

    def tearDown(self) -> None:
        database.close()
        self.temp_dir.cleanup()

    def test_persists_remote_tree_item_and_delta_cursor(self) -> None:
        persist_delta_page(
            [
                {
                    "drive_item_id": "item-1",
                    "parent_drive_item_id": "parent-1",
                    "name": "01.mkv",
                    "relative_path": "Anime/Frieren/01.mkv",
                    "is_file": True,
                    "size": 123,
                    "hash_type": "quickXorHash",
                    "hash": "hash",
                    "etag": "etag",
                    "remote_mtime": datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
                }
            ],
            "https://graph.test/delta?token=next",
        )

        record = OneDriveItem.get(OneDriveItem.drive_item_id == "item-1")
        self.assertEqual(record.parent_drive_item_id, "parent-1")
        self.assertEqual(record.relative_path, "Anime/Frieren/01.mkv")
        self.assertTrue(record.is_file)
        self.assertEqual(KeyValue.get_by_id(DELTA_CURSOR_KEY).value, "https://graph.test/delta?token=next")

    def test_upserts_changed_item_without_changing_local_row_id(self) -> None:
        record = OneDriveItem.create(
            drive_item_id="item-1",
            relative_path="Anime/Frieren/01.mkv",
            size=123,
            is_file=True,
        )
        persist_delta_page([{"drive_item_id": "item-1", "name": "renamed.mkv", "size": 456}], "cursor")

        updated = OneDriveItem.get(OneDriveItem.drive_item_id == "item-1")
        self.assertEqual(updated.id, record.id)
        self.assertEqual(updated.name, "renamed.mkv")
        self.assertEqual(updated.size, 456)

    def test_rolls_back_items_and_cursor_when_a_page_is_invalid(self) -> None:
        KeyValue.create(key=DELTA_CURSOR_KEY, value="old-cursor")

        with self.assertRaises(ValueError):
            persist_delta_page([{"drive_item_id": "item-1"}, {}], "new-cursor")

        self.assertEqual(OneDriveItem.select().count(), 0)
        self.assertEqual(KeyValue.get_by_id(DELTA_CURSOR_KEY).value, "old-cursor")

    def test_creates_remote_tree_and_key_value_columns(self) -> None:
        item_columns = {column.name for column in database.get_columns(OneDriveItem._meta.table_name)}
        key_value_columns = {column.name for column in database.get_columns(KeyValue._meta.table_name)}

        self.assertTrue(
            {
                "drive_item_id",
                "parent_drive_item_id",
                "name",
                "relative_path",
                "is_file",
                "is_folder",
                "is_deleted",
                "size",
                "hash_type",
                "hash",
                "etag",
                "remote_mtime",
            }.issubset(item_columns)
        )
        self.assertTrue({"key", "value", "updated_at"}.issubset(key_value_columns))

    def test_publishes_path_ready_files_to_transfer_pipeline(self) -> None:
        ensure_root_item("root")
        persist_delta_page(
            [
                {
                    "drive_item_id": "folder",
                    "parent_drive_item_id": "root",
                    "name": "Anime",
                    "is_folder": True,
                },
                {
                    "drive_item_id": "file",
                    "parent_drive_item_id": "folder",
                    "name": "01.mkv",
                    "is_file": True,
                    "size": 123,
                },
            ],
            "cursor",
        )

        self.assertEqual(claim_ready_transfers(1), [])
        resolve_relative_paths("root")
        claimed = claim_ready_transfers(1)

        self.assertEqual([record.drive_item_id for record in claimed], ["file"])
        self.assertEqual(claimed[0].status, TransferStatus.CHECKING.value)
        self.assertEqual(OneDriveItem.get(OneDriveItem.drive_item_id == "file").relative_path, "Anime/01.mkv")
        self.assertEqual(TransferRecord.get(TransferRecord.drive_item_id == "file").status, "checking")

    def test_changed_folder_rebuilds_descendant_paths_and_resets_transfers(self) -> None:
        ensure_root_item("root")
        folder = OneDriveItem.create(
            drive_item_id="folder", parent_drive_item_id="root", name="Old", relative_path="Old", is_folder=True
        )
        file = OneDriveItem.create(
            drive_item_id="file", parent_drive_item_id="folder", name="01.mkv", relative_path="Old/01.mkv", is_file=True
        )
        TransferRecord.create(
            drive_item_id=file.drive_item_id,
            status=TransferStatus.COMPLETE.value,
            verified_hash="old-hash",
        )

        persist_delta_page(
            [{"drive_item_id": folder.drive_item_id, "parent_drive_item_id": "root", "name": "New", "is_folder": True}],
            "next-cursor",
        )
        resolve_relative_paths("root")

        self.assertEqual(OneDriveItem.get_by_id(file.id).relative_path, "New/01.mkv")
        transfer = TransferRecord.get(TransferRecord.drive_item_id == file.drive_item_id)
        self.assertEqual(transfer.status, TransferStatus.PENDING.value)
        self.assertIsNone(transfer.verified_hash)

    def test_filters_transfers_to_selected_manifest_root(self) -> None:
        media = OneDriveItem.create(drive_item_id="media", relative_path="Media/01.mkv", is_file=True)
        other = OneDriveItem.create(drive_item_id="other", relative_path="Other/02.mkv", is_file=True)
        TransferRecord.create(drive_item_id=media.drive_item_id)
        TransferRecord.create(drive_item_id=other.drive_item_id)

        selected = get_transfer_items(relative_root="Media")

        self.assertEqual([item.drive_item_id for item, _ in selected], ["media"])
