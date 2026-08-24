from __future__ import annotations

import unittest
from pathlib import Path
from contextlib import redirect_stdout
from io import StringIO

from onedrive_staginger.__main__ import parse_args
from onedrive_staginger.task import MigrationTask


class MigrationTaskTests(unittest.TestCase):
    def test_normalizes_paths_and_remote_root(self) -> None:
        task = MigrationTask(Path("./temp"), Path("./dist"), "/Media/")

        self.assertTrue(task.temp_dir.is_absolute())
        self.assertTrue(task.dist_dir.is_absolute())
        self.assertEqual(task.remote_root_path, "/Media")

    def test_rejects_same_local_directory(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be different"):
            MigrationTask(Path("./same"), Path("./same"), "/Media")

    def test_rejects_non_absolute_remote_root(self) -> None:
        with self.assertRaisesRegex(ValueError, "must start"):
            MigrationTask(Path("./temp"), Path("./dist"), "Media")

    def test_builds_flat_temp_path_from_file_id_and_name(self) -> None:
        task = MigrationTask(Path("./temp"), Path("./dist"), "/Media")

        self.assertEqual(
            task.temp_path("01ABCDEF", "01.mkv"),
            task.temp_dir / "01ABCDEF-01.mkv",
        )

    def test_rejects_temp_path_components(self) -> None:
        task = MigrationTask(Path("./temp"), Path("./dist"), "/Media")

        with self.assertRaisesRegex(ValueError, "drive_item_id"):
            task.temp_path("nested/id", "01.mkv")
        with self.assertRaisesRegex(ValueError, "file_name"):
            task.temp_path("01ABCDEF", "nested/01.mkv")

    def test_truncates_temp_file_name_at_a_utf8_character_boundary(self) -> None:
        task = MigrationTask(Path("./temp"), Path("./dist"), "/Media")
        file_name = "a" * 251 + "中"

        path = task.temp_path("id", file_name)

        self.assertEqual(path.name, "id-" + "a" * 237)
        self.assertLessEqual(len(path.name.encode("utf-8")), 240)
        self.assertLessEqual(len(f"{path.name}.aria2".encode("utf-8")), 246)


class CliTests(unittest.TestCase):
    def test_parses_download_arguments(self) -> None:
        namespace = parse_args(["/etc/onedrive-staginger", "download", "/mnt/temp", "/mnt/dist", "/Media"])

        self.assertEqual(namespace.config_dir, Path("/etc/onedrive-staginger"))
        self.assertEqual(namespace.command, "download")
        self.assertEqual(namespace.temp_dir, Path("/mnt/temp"))
        self.assertEqual(namespace.dist_dir, Path("/mnt/dist"))
        self.assertEqual(namespace.remote_root_path, "/Media")

    def test_parses_login_with_only_common_configuration_directory(self) -> None:
        namespace = parse_args(["/etc/onedrive-staginger", "login"])

        self.assertEqual(namespace.config_dir, Path("/etc/onedrive-staginger"))
        self.assertEqual(namespace.command, "login")

    def test_parses_sync_with_only_common_configuration_directory(self) -> None:
        namespace = parse_args(["/etc/onedrive-staginger", "sync"])

        self.assertEqual(namespace.config_dir, Path("/etc/onedrive-staginger"))
        self.assertEqual(namespace.command, "sync")

    def test_login_help_includes_azure_registration_guide(self) -> None:
        output = StringIO()

        with self.assertRaises(SystemExit), redirect_stdout(output):
            parse_args(["/etc/onedrive-staginger", "login", "--help"])

        help_text = output.getvalue()
        self.assertIn("Allow public client flows", help_text)
        self.assertIn("offline_access", help_text)
        self.assertIn("不要创建 Client Secret", help_text)

    def test_download_path_removes_selected_root(self) -> None:
        task = MigrationTask(Path("./temp"), Path("./dist"), "/Media")

        self.assertEqual(task.download_path("Media/Anime/01.mkv"), task.dist_dir / "Anime/01.mkv")
