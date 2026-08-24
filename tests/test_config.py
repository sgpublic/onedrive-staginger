from __future__ import annotations

import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from onedrive_staginger.config import AppConfig, ConfigError


VALID_CONFIG = """
azure:
  tenant_id: tenant
  client_id: client
aria2:
  executable: aria2c
scheduler:
  max_downloads: 2
  max_moves: 1
  connections_per_file: 8
  min_split_size: "4M"
  poll_interval_seconds: 5
"""


class AppConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        AppConfig._instance = None
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.temp_dir.name)
        self.path = self.config_dir / "config.yaml"
        self.path.write_text(VALID_CONFIG, encoding="utf-8")

    def tearDown(self) -> None:
        AppConfig._instance = None
        self.temp_dir.cleanup()

    def test_initializes_immutable_singleton_from_yaml(self) -> None:
        config = AppConfig.initialize(self.config_dir)

        self.assertIs(config, AppConfig.get())
        self.assertEqual(config.azure.client_id, "client")
        self.assertEqual(config.scheduler.api_requests_per_second, 10)
        self.assertEqual(config.scheduler.min_split_size, "4M")
        with self.assertRaises(FrozenInstanceError):
            config.scheduler.max_downloads = 3  # type: ignore[misc]

    def test_rejects_second_initialization(self) -> None:
        AppConfig.initialize(self.config_dir)

        with self.assertRaisesRegex(ConfigError, "already been initialized"):
            AppConfig.initialize(self.config_dir)

    def test_rejects_invalid_scheduler_value(self) -> None:
        self.path.write_text(VALID_CONFIG.replace("max_downloads: 2", "max_downloads: 0"), encoding="utf-8")

        with self.assertRaisesRegex(ConfigError, "max_downloads"):
            AppConfig.initialize(self.config_dir)

    def test_rejects_invalid_min_split_size(self) -> None:
        self.path.write_text(VALID_CONFIG.replace('min_split_size: "4M"', "min_split_size: 4"), encoding="utf-8")

        with self.assertRaisesRegex(ConfigError, "min_split_size"):
            AppConfig.initialize(self.config_dir)
