"""Managed local aria2c process lifecycle."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import os
import platform
import secrets
from pathlib import Path
import shutil
import tarfile
import tempfile
from urllib.error import URLError
from urllib.request import urlopen

import aiohttp
from aioaria2 import Aria2HttpClient

from .config import Aria2Config, SchedulerConfig
from .database import OneDriveItem
from .onedrive import OneDriveClient
from .task import MigrationTask
from .utils.network import available_local_port


logger = logging.getLogger(__name__)

ARIA2_ARCHIVE_URL = (
    "https://github.com/P3TERX/Aria2-Pro-Core/releases/download/1.36.0_2021.08.22/"
    "aria2-1.36.0-static-linux-amd64.tar.gz"
)


class Aria2BinaryError(ValueError):
    """The managed Aria2-Pro-Core binary could not be installed."""


@dataclass(frozen=True, slots=True)
class Aria2Status:
    status: str
    completed_bytes: int
    total_bytes: int
    download_speed: int
    error: str | None = None


class Aria2Process:
    """Run one aria2c daemon with an ephemeral local JSON-RPC endpoint."""

    def __init__(self, config: Aria2Config, config_dir: Path) -> None:
        self._config = config
        self._config_dir = config_dir.expanduser().resolve(strict=False)
        self._port = available_local_port()
        self._secret = secrets.token_urlsafe(32)
        self._process: asyncio.subprocess.Process | None = None

    @property
    def session_path(self) -> Path:
        return self._config_dir / "aria2.session"

    @property
    def binary_path(self) -> Path:
        return self._config_dir / "bin" / "aria2c"

    async def start(self) -> None:
        """Start aria2c if it is not already running."""
        if self._process is not None:
            raise RuntimeError("aria2c process has already been started")

        await self._ensure_binary()
        self._process = await asyncio.create_subprocess_exec(
            str(self.binary_path),
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            f"--rpc-listen-port={self._port}",
            f"--rpc-secret={self._secret}",
            "--continue=true",
            f"--disk-cache={self._config.disk_cache}",
            f"--file-allocation={self._config.file_allocation}",
            f"--save-session={self.session_path}",
            "--save-session-interval=60",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        logger.info("aria2c 已启动")

    async def _ensure_binary(self) -> None:
        binary_path = self.binary_path
        if binary_path.is_file():
            await asyncio.to_thread(_make_executable, binary_path)
            return
        if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
            raise Aria2BinaryError(
                "Aria2-Pro-Core managed binary supports only Linux x86_64"
            )
        logger.info("正在下载 Aria2-Pro-Core：%s", ARIA2_ARCHIVE_URL)
        await asyncio.to_thread(_install_binary, binary_path)
        logger.info("Aria2-Pro-Core 已安装：%s", binary_path)

    def create_client(self, session: aiohttp.ClientSession) -> Aria2HttpClient:
        """Create an authenticated RPC client for the running local aria2c process."""
        if self._process is None or self._process.returncode is not None:
            raise RuntimeError("aria2c process is not running")
        return Aria2HttpClient(
            f"http://127.0.0.1:{self._port}/jsonrpc",
            token=self._secret,
            client_session=session,
        )

    async def aclose(self) -> None:
        """Stop aria2c, escalating to kill when it does not exit promptly."""
        if self._process is None:
            return
        if self._process.returncode is None:
            self._process.terminate()
        try:
            await asyncio.wait_for(self._process.wait(), timeout=10)
        except TimeoutError:
            self._process.kill()
            await self._process.wait()
        finally:
            self._process = None
            logger.info("aria2c 已停止")

    async def __aenter__(self) -> Aria2Process:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class Aria2DownloadManager:
    """Submit and monitor one-file aria2 downloads using transient state."""

    def __init__(
        self,
        client: Aria2HttpClient,
        onedrive: OneDriveClient,
        drive_id: str,
        task: MigrationTask,
        scheduler: SchedulerConfig,
    ) -> None:
        self._client = client
        self._onedrive = onedrive
        self._drive_id = drive_id
        self._task = task
        self._scheduler = scheduler

    async def submit(self, item: OneDriveItem, relative_path: str) -> str:
        """Submit a file with a fresh Graph URL, resuming an existing control file."""
        if item.name is None:
            raise ValueError(f"OneDrive item {item.drive_item_id} has no file name")

        output_path = self._task.temp_path(item.drive_item_id, item.name)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("正在获取下载直链：%s", relative_path)
        download_url = await self._onedrive.get_download_url(self._drive_id, item.drive_item_id)
        logger.info("正在向 aria2 提交下载任务：%s", relative_path)
        if item.size is None:
            raise ValueError(f"OneDrive item {item.drive_item_id} has no size")
        gid = await self._client.addUri([download_url], self._options(output_path, item.size))
        return gid

    async def resume(self, item: OneDriveItem, relative_path: str) -> str:
        """Resume an interrupted file using a newly issued Graph download URL."""
        return await self.submit(item, relative_path)

    async def poll(self, gid: str) -> Aria2Status:
        """Return aria2's status without persisting transfer state."""
        response = await self._client.tellStatus(
            gid,
            ["status", "completedLength", "totalLength", "downloadSpeed", "errorCode", "errorMessage"],
        )
        if not isinstance(response, dict) or not isinstance(response.get("status"), str):
            raise ValueError(f"aria2 returned an invalid status for {gid}")

        status = response["status"]
        completed = _byte_value(response.get("completedLength"))
        total = _byte_value(response.get("totalLength"))
        speed = _byte_value(response.get("downloadSpeed"))
        if status in {"active", "waiting", "paused"}:
            return Aria2Status(status, completed, total, speed)
        elif status == "complete":
            return Aria2Status(status, completed, total, speed)
        elif status in {"error", "removed"}:
            message = response.get("errorMessage") or f"aria2 task {status}"
            code = response.get("errorCode")
            if code:
                message = f"aria2 error {code}: {message}"
            return Aria2Status(status, completed, total, speed, str(message))
        else:
            raise ValueError(f"aria2 returned unknown status '{status}'")

    def _options(self, output_path: Path, file_size: int) -> dict[str, str]:
        options = {
            "dir": str(output_path.parent),
            "out": output_path.name,
            "split": str(self._scheduler.connections_per_file),
            "max-connection-per-server": str(self._scheduler.connections_per_file),
            "min-split-size": str(self._effective_min_split_size(file_size)),
        }
        return options

    def _effective_min_split_size(self, file_size: int) -> int:
        per_connection = file_size // self._scheduler.connections_per_file
        return max(
            self._scheduler.min_split_size,
            min(self._scheduler.max_split_size, per_connection),
        )


def _byte_value(value: object) -> int:
    if not isinstance(value, str) or not value.isdigit():
        return 0
    return int(value)


def _install_binary(binary_path: Path) -> None:
    binary_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with urlopen(ARIA2_ARCHIVE_URL, timeout=60) as response:
            with tarfile.open(fileobj=response, mode="r|gz") as archive:
                for member in archive:
                    member_path = Path(member.name)
                    if not member.isfile() or member_path.name != "aria2c":
                        continue
                    source = archive.extractfile(member)
                    if source is None:
                        continue
                    with tempfile.NamedTemporaryFile(dir=binary_path.parent, delete=False) as output:
                        temporary_path = Path(output.name)
                        shutil.copyfileobj(source, output)
                    _make_executable(temporary_path)
                    os.replace(temporary_path, binary_path)
                    temporary_path = None
                    return
    except (OSError, tarfile.TarError, URLError) as error:
        raise Aria2BinaryError(f"Unable to install Aria2-Pro-Core: {error}") from error
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    raise Aria2BinaryError("Aria2-Pro-Core archive does not contain aria2c")


def _make_executable(path: Path) -> None:
    path.chmod(path.stat().st_mode | 0o111)
