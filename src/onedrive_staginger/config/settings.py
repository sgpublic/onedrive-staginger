"""YAML-backed immutable application configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import yaml


class ConfigError(ValueError):
    """The application configuration is absent, invalid, or already initialized."""


@dataclass(frozen=True, slots=True)
class AzureConfig:
    tenant_id: str
    client_id: str


@dataclass(frozen=True, slots=True)
class Aria2Config:
    executable: str


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    api_requests_per_second: int = 10
    max_downloads: int = 2
    max_moves: int = 1
    connections_per_file: int = 8
    disable_http2: bool = True
    poll_interval_seconds: int = 5


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Process-wide application settings initialized exactly once from YAML."""

    azure: AzureConfig
    aria2: Aria2Config
    scheduler: SchedulerConfig

    _instance: ClassVar[AppConfig | None] = None

    @classmethod
    def initialize(cls, config_dir: Path) -> AppConfig:
        if cls._instance is not None:
            raise ConfigError("Application configuration has already been initialized")

        config_dir = config_dir.expanduser().resolve(strict=False)
        path = config_dir / "config.yaml"
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except OSError as error:
            raise ConfigError(f"Unable to read configuration file: {path}") from error
        except yaml.YAMLError as error:
            raise ConfigError(f"Invalid YAML configuration: {path}") from error

        cls._instance = cls._from_mapping(raw, config_dir)
        return cls._instance

    @classmethod
    def get(cls) -> AppConfig:
        if cls._instance is None:
            raise ConfigError("Application configuration has not been initialized")
        return cls._instance

    @classmethod
    def _from_mapping(cls, raw: Any, config_dir: Path) -> AppConfig:
        if not isinstance(raw, dict):
            raise ConfigError("Configuration root must be a YAML mapping")

        azure = _required_mapping(raw, "azure")
        aria2 = _required_mapping(raw, "aria2")
        scheduler = _required_mapping(raw, "scheduler")
        return cls(
            azure=AzureConfig(
                tenant_id=_required_str(azure, "tenant_id"),
                client_id=_required_str(azure, "client_id"),
            ),
            aria2=Aria2Config(
                executable=_required_str(aria2, "executable"),
            ),
            scheduler=SchedulerConfig(
                api_requests_per_second=_positive_int(
                    scheduler, "api_requests_per_second", default=10
                ),
                max_downloads=_positive_int(scheduler, "max_downloads"),
                max_moves=_positive_int(scheduler, "max_moves"),
                connections_per_file=_positive_int(scheduler, "connections_per_file"),
                disable_http2=_required_bool(scheduler, "disable_http2"),
                poll_interval_seconds=_positive_int(scheduler, "poll_interval_seconds"),
            ),
        )


def _required_mapping(value: dict[str, Any], key: str) -> dict[str, Any]:
    item = value.get(key)
    if not isinstance(item, dict):
        raise ConfigError(f"Configuration field '{key}' must be a mapping")
    return item


def _required_str(value: dict[str, Any], key: str, *, allow_empty: bool = False) -> str:
    item = value.get(key)
    if not isinstance(item, str) or (not allow_empty and not item):
        raise ConfigError(f"Configuration field '{key}' must be a non-empty string")
    return item


def _required_bool(value: dict[str, Any], key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise ConfigError(f"Configuration field '{key}' must be a boolean")
    return item


def _positive_int(value: dict[str, Any], key: str, *, default: int | None = None) -> int:
    item = value.get(key, default)
    if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
        raise ConfigError(f"Configuration field '{key}' must be a positive integer")
    return item
