from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import tempfile
import unittest

from onedrive_staginger.config import Account, AccountStore, ConfigError


class AccountStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_round_trips_account_with_private_permissions(self) -> None:
        account = Account(
            drive_id="b!drive",
            token_type="Bearer",
            access_token="access",
            refresh_token="refresh",
            expires_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
        )

        AccountStore.save(self.config_dir, account)

        self.assertEqual(AccountStore.load(self.config_dir), account)
        self.assertEqual(os.stat(self.config_dir / "account.yaml").st_mode & 0o777, 0o600)

    def test_requires_login_when_account_file_is_missing(self) -> None:
        with self.assertRaisesRegex(ConfigError, "Run the login command"):
            AccountStore.load(self.config_dir)
