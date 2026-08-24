"""Application startup orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp
from aioaria2 import Aria2HttpClient

from .config import Account, AccountStore, AppConfig, ConfigError
from .aria2 import Aria2DownloadManager, Aria2Process
from .database import MANIFEST_STATE_KEY, ManifestState, database, get_value, initialize_database
from .onedrive import (
    DriveItem,
    GraphApiError,
    OneDriveClient,
    acquire_device_code_token,
    get_current_user_drive_id,
    request_device_code,
)
from .task import MigrationTask
from .pipeline import ManifestPipeline, MigrationController, MigrationWorker, TransferScheduler


logger = logging.getLogger(__name__)


async def login(config_dir: Path, *, notify: Callable[[str], None] = print) -> Account:
    """Authenticate interactively and save the user's delegated OneDrive account."""
    config = AppConfig.initialize(config_dir)
    async with aiohttp.ClientSession() as session:
        device_code = await request_device_code(
            session, config.azure.tenant_id, config.azure.client_id
        )
        notify(device_code.message)
        token = await _wait_for_device_code_token(
            session,
            config.azure.tenant_id,
            config.azure.client_id,
            device_code.device_code,
            device_code.interval,
            device_code.expires_in,
        )
        if not token.refresh_token:
            raise ConfigError("Device login did not return a refresh token")
        drive_id = await get_current_user_drive_id(session, token.token)

    account = Account(
        drive_id=drive_id,
        token_type=token.token_type,
        access_token=token.token,
        refresh_token=token.refresh_token,
        expires_at=_expiry(token.expires_in),
    )
    AccountStore.save(config_dir, account)
    return account


async def sync(config_dir: Path) -> DriveItem:
    """Apply OneDrive delta pages to the persistent full-drive manifest."""
    logger.info("Starting manifest synchronization")
    config = AppConfig.initialize(config_dir)
    account = AccountStore.load(config_dir)
    config_dir = config_dir.expanduser().resolve(strict=False)
    initialize_database(config_dir / "staging.sqlite")

    async def save_refreshed_account(refreshed_account: Account) -> None:
        AccountStore.save(config_dir, refreshed_account)

    try:
        async with aiohttp.ClientSession() as session:
            client = OneDriveClient(
                session,
                account,
                tenant_id=config.azure.tenant_id,
                client_id=config.azure.client_id,
                on_account_refreshed=save_refreshed_account,
                api_requests_per_second=config.scheduler.api_requests_per_second,
            )
            root = await client.get_item_by_path(account.drive_id, "/")
            manifest = ManifestPipeline(client, account.drive_id, root.id)
            await manifest.run()
            return root
    finally:
        if not database.is_closed():
            database.close()


async def download(config_dir: Path, task: MigrationTask) -> None:
    """Download one completed static manifest subtree without enumerating OneDrive."""
    logger.info(
        "Starting download: OneDrive %s -> temp %s -> dist %s",
        task.remote_root_path,
        task.temp_dir,
        task.dist_dir,
    )
    config = AppConfig.initialize(config_dir)
    account = AccountStore.load(config_dir)
    config_dir = config_dir.expanduser().resolve(strict=False)
    initialize_database(config_dir / "staging.sqlite")
    try:
        if get_value(MANIFEST_STATE_KEY) != ManifestState.COMPLETE.value:
            raise ConfigError("Run sync successfully before download")

        async def save_refreshed_account(refreshed_account: Account) -> None:
            AccountStore.save(config_dir, refreshed_account)

        async with aiohttp.ClientSession() as session:
            client = OneDriveClient(
                session,
                account,
                tenant_id=config.azure.tenant_id,
                client_id=config.azure.client_id,
                on_account_refreshed=save_refreshed_account,
                api_requests_per_second=config.scheduler.api_requests_per_second,
            )
            async with Aria2Process(config.aria2, config_dir) as process:
                aria2_client = process.create_client(session)
                await _wait_for_aria2(aria2_client)
                downloads = Aria2DownloadManager(
                    aria2_client, client, account.drive_id, task, config.scheduler
                )
                worker = MigrationWorker(task, downloads)
                scheduler = TransferScheduler(downloads, worker, config.scheduler, task.manifest_root)
                controller = MigrationController(
                    worker, scheduler, config.scheduler, task.manifest_root
                )
                await controller.run()
    finally:
        if not database.is_closed():
            database.close()


async def _wait_for_device_code_token(
    session: aiohttp.ClientSession,
    tenant_id: str,
    client_id: str,
    device_code: str,
    interval: int,
    expires_in: int,
):
    deadline = asyncio.get_running_loop().time() + expires_in
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(interval)
        try:
            return await acquire_device_code_token(session, tenant_id, client_id, device_code)
        except GraphApiError as error:
            if error.code == "authorization_pending":
                continue
            if error.code == "slow_down":
                interval += 5
                continue
            raise
    raise ConfigError("Device login timed out before authorization completed")


def _expiry(expires_in: int) -> datetime:
    return datetime.now(UTC) + timedelta(seconds=expires_in)


async def _wait_for_aria2(client: Aria2HttpClient) -> None:
    """Wait briefly for the newly spawned aria2c RPC listener to accept requests."""
    last_error: Exception | None = None
    for _ in range(50):
        try:
            await client.getVersion()
            return
        except Exception as error:
            last_error = error
            await asyncio.sleep(0.1)
    raise ConfigError("aria2c did not start its RPC listener") from last_error
