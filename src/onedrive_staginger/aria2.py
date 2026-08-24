"""Managed local aria2c process lifecycle."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging
import secrets
from pathlib import Path

import aiohttp
from aioaria2 import Aria2HttpClient

from .config import Aria2Config, SchedulerConfig
from .database import OneDriveItem
from .onedrive import OneDriveClient
from .task import MigrationTask
from .utils.network import available_local_port


logger = logging.getLogger(__name__)


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

    async def start(self) -> None:
        """Start aria2c if it is not already running."""
        if self._process is not None:
            raise RuntimeError("aria2c process has already been started")

        self._process = await asyncio.create_subprocess_exec(
            self._config.executable,
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            f"--rpc-listen-port={self._port}",
            f"--rpc-secret={self._secret}",
            "--continue=true",
            f"--save-session={self.session_path}",
            "--save-session-interval=60",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        logger.info("Started aria2c")

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
            logger.info("Stopped aria2c")

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
        logger.info("Getting download URL: %s", relative_path)
        download_url = await self._onedrive.get_download_url(self._drive_id, item.drive_item_id)
        logger.info("Submitting download task to aria2: %s", relative_path)
        gid = await self._client.addUri([download_url], self._options(output_path))
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

    def _options(self, output_path: Path) -> dict[str, str]:
        options = {
            "dir": str(output_path.parent),
            "out": output_path.name,
            "split": str(self._scheduler.connections_per_file),
            "max-connection-per-server": str(self._scheduler.connections_per_file),
        }
        if self._scheduler.disable_http2:
            options["enable-http2"] = "false"
        return options


def _byte_value(value: object) -> int:
    if not isinstance(value, str) or not value.isdigit():
        return 0
    return int(value)
