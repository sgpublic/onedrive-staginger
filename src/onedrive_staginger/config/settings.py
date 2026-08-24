"""YAML-backed immutable application configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
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
    disk_cache: int = 16 * 1024 * 1024
    file_allocation: str = "prealloc"


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    api_requests_per_second: int = 10
    max_downloads: int = 2
    max_moves: int = 1
    connections_per_file: int = 8
    min_split_size: int = 1024 * 1024
    max_split_size: int = 4 * 1024 * 1024
    poll_interval_seconds: int = 5
    fast_verify_after_download: bool = False


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
        min_split_size = _aria2_size_bytes(scheduler, "min_split_size", default="1M")
        max_split_size = _aria2_size_bytes(scheduler, "max_split_size", default="4M")
        if max_split_size < min_split_size:
            raise ConfigError("Configuration field 'max_split_size' must be at least min_split_size")
        return cls(
            azure=AzureConfig(
                tenant_id=_required_str(azure, "tenant_id"),
                client_id=_required_str(azure, "client_id"),
            ),
            aria2=Aria2Config(
                executable=_required_str(aria2, "executable"),
                disk_cache=_aria2_cache_bytes(aria2, "disk_cache", default="16M"),
                file_allocation=_aria2_file_allocation(aria2),
            ),
            scheduler=SchedulerConfig(
                api_requests_per_second=_positive_int(
                    scheduler, "api_requests_per_second", default=10
                ),
                max_downloads=_positive_int(scheduler, "max_downloads"),
                max_moves=_positive_int(scheduler, "max_moves"),
                connections_per_file=_positive_int(scheduler, "connections_per_file"),
                min_split_size=min_split_size,
                max_split_size=max_split_size,
                poll_interval_seconds=_positive_int(scheduler, "poll_interval_seconds"),
                fast_verify_after_download=_bool(scheduler, "fast_verify_after_download", default=False),
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


def _positive_int(value: dict[str, Any], key: str, *, default: int | None = None) -> int:
    item = value.get(key, default)
    if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
        raise ConfigError(f"Configuration field '{key}' must be a positive integer")
    return item


def _bool(value: dict[str, Any], key: str, *, default: bool) -> bool:
    item = value.get(key, default)
    if not isinstance(item, bool):
        raise ConfigError(f"Configuration field '{key}' must be a boolean")
    return item


def _aria2_size_bytes(value: dict[str, Any], key: str, *, default: str) -> int:
    item = value.get(key, default)
    if not isinstance(item, str) or not re.fullmatch(r"[1-9][0-9]*[KMG]?", item):
        raise ConfigError(f"Configuration field '{key}' must be an aria2 size such as '4M'")
    multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}
    suffix = item[-1]
    if suffix in multiplier:
        return int(item[:-1]) * multiplier[suffix]
    return int(item)


def _aria2_cache_bytes(value: dict[str, Any], key: str, *, default: str) -> int:
    item = value.get(key, default)
    if not isinstance(item, str) or not re.fullmatch(r"(?:0|[1-9][0-9]*[KMG]?)", item):
        raise ConfigError(f"Configuration field '{key}' must be an aria2 size such as '256M'")
    multiplier = {"K": 1024, "M": 1024**2, "G": 1024**3}
    suffix = item[-1]
    if suffix in multiplier:
        return int(item[:-1]) * multiplier[suffix]
    return int(item)


def _aria2_file_allocation(value: dict[str, Any]) -> str:
    item = value.get("file_allocation", "prealloc")
    if not isinstance(item, str) or item not in {"none", "prealloc", "trunc", "falloc"}:
        raise ConfigError(
            "Configuration field 'file_allocation' must be one of none, prealloc, trunc, falloc"
        )
    return item
