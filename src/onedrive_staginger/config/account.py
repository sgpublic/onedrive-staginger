"""Persistent delegated-account state kept separate from static settings."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import tempfile
from typing import Any

import yaml

from .settings import ConfigError


@dataclass(frozen=True, slots=True)
class Account:
    drive_id: str
    token_type: str
    access_token: str
    refresh_token: str
    expires_at: datetime


class AccountStore:
    """Read and atomically replace the account state in a configuration directory."""

    @staticmethod
    def load(config_dir: Path) -> Account:
        path = _account_path(config_dir)
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except FileNotFoundError as error:
            raise ConfigError(f"Account file does not exist: {path}. Run the login command first.") from error
        except OSError as error:
            raise ConfigError(f"Unable to read account file: {path}") from error
        except yaml.YAMLError as error:
            raise ConfigError(f"Invalid YAML account file: {path}") from error
        if not isinstance(raw, dict):
            raise ConfigError("Account file root must be a YAML mapping")

        try:
            expires_at = datetime.fromisoformat(_required_str(raw, "expires_at"))
        except ValueError as error:
            raise ConfigError("Account field 'expires_at' must be an ISO 8601 datetime") from error
        if expires_at.tzinfo is None:
            raise ConfigError("Account field 'expires_at' must include a timezone")

        return Account(
            drive_id=_required_str(raw, "drive_id"),
            token_type=_required_str(raw, "token_type"),
            access_token=_required_str(raw, "access_token"),
            refresh_token=_required_str(raw, "refresh_token"),
            expires_at=expires_at,
        )

    @staticmethod
    def save(config_dir: Path, account: Account) -> None:
        config_dir = config_dir.expanduser().resolve(strict=False)
        if not config_dir.is_dir():
            raise ConfigError(f"Configuration directory does not exist: {config_dir}")

        content = yaml.safe_dump(
            {
                "drive_id": account.drive_id,
                "token_type": account.token_type,
                "access_token": account.access_token,
                "refresh_token": account.refresh_token,
                "expires_at": account.expires_at.isoformat(),
            },
            sort_keys=False,
        )
        destination = config_dir / "account.yaml"
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=config_dir, delete=False) as file:
                temporary_path = Path(file.name)
                file.write(content)
                file.flush()
                os.fsync(file.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, destination)
        except OSError as error:
            temporary_path.unlink(missing_ok=True) if "temporary_path" in locals() else None
            raise ConfigError(f"Unable to write account file: {destination}") from error


def _account_path(config_dir: Path) -> Path:
    return config_dir.expanduser().resolve(strict=False) / "account.yaml"


def _required_str(value: dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ConfigError(f"Account field '{key}' must be a non-empty string")
    return item
