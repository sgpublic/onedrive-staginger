"""Managed local aria2c process lifecycle."""

from __future__ import annotations

import asyncio
import secrets
from pathlib import Path

import aiohttp
from aioaria2 import Aria2HttpClient

from .config import Aria2Config, SchedulerConfig
from .database import OneDriveItem, TransferRecord, TransferStatus
from .onedrive import OneDriveClient
from .task import MigrationTask
from .utils.network import available_local_port


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

    async def __aenter__(self) -> Aria2Process:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


class Aria2DownloadManager:
    """Submit and monitor one-file aria2 downloads backed by SQLite state."""

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

    async def submit(self, item: OneDriveItem) -> str:
        """Submit a file with a fresh Graph URL, resuming an existing control file."""
        if item.name is None:
            raise ValueError(f"OneDrive item {item.drive_item_id} has no file name")

        output_path = self._task.temp_path(item.drive_item_id, item.name)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        download_url = await self._onedrive.get_download_url(self._drive_id, item.drive_item_id)
        gid = await self._client.addUri([download_url], self._options(output_path))
        _set_transfer_downloading(item.drive_item_id, gid)
        return gid

    async def resume(self, item: OneDriveItem) -> str:
        """Resume an interrupted file using a newly issued Graph download URL."""
        return await self.submit(item)

    async def poll(self, transfer: TransferRecord) -> str:
        """Persist aria2's current terminal or in-progress status for a transfer."""
        if transfer.aria2_gid is None:
            raise ValueError(f"Transfer {transfer.drive_item_id} has no aria2 GID")

        response = await self._client.tellStatus(
            transfer.aria2_gid, ["status", "errorCode", "errorMessage"]
        )
        if not isinstance(response, dict) or not isinstance(response.get("status"), str):
            raise ValueError(f"aria2 returned an invalid status for {transfer.aria2_gid}")

        status = response["status"]
        if status in {"active", "waiting", "paused"}:
            _set_transfer_downloading(transfer.drive_item_id, transfer.aria2_gid)
        elif status == "complete":
            _set_transfer_status(transfer.drive_item_id, TransferStatus.CHECKING)
        elif status in {"error", "removed"}:
            message = response.get("errorMessage") or f"aria2 task {status}"
            code = response.get("errorCode")
            if code:
                message = f"aria2 error {code}: {message}"
            _set_transfer_status(transfer.drive_item_id, TransferStatus.FAILED, str(message))
        else:
            raise ValueError(f"aria2 returned unknown status '{status}'")
        return status

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


def _set_transfer_downloading(drive_item_id: str, gid: str) -> None:
    _update_transfer(
        drive_item_id,
        status=TransferStatus.DOWNLOADING.value,
        aria2_gid=gid,
        last_error=None,
    )


def _set_transfer_status(
    drive_item_id: str, status: TransferStatus, last_error: str | None = None
) -> None:
    _update_transfer(drive_item_id, status=status.value, last_error=last_error)


def _update_transfer(drive_item_id: str, **values: str | None) -> None:
    updated = (
        TransferRecord.update(**values)
        .where(TransferRecord.drive_item_id == drive_item_id)
        .execute()
    )
    if updated != 1:
        raise ValueError(f"Transfer record does not exist for {drive_item_id}")

