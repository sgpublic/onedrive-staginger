"""Small asynchronous wrapper around the Microsoft Graph OneDrive endpoints."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
import inspect
import logging
from typing import Any
from urllib.parse import quote

import aiohttp
from aiolimiter import AsyncLimiter

from ..config import Account
from .models import (
    AccessToken,
    DeltaCursorExpiredError,
    DeltaPage,
    DeviceCode,
    DriveItem,
    GraphApiError,
)

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
IDENTITY_BASE_URL = "https://login.microsoftonline.com"
ITEM_FIELDS = (
    "id,name,size,eTag,lastModifiedDateTime,parentReference,file,folder,deleted"
)
DELEGATED_SCOPES = "User.Read Files.Read offline_access"
DOWNLOAD_URL_TIMEOUT_SECONDS = 60
DOWNLOAD_URL_RETRY_MAX_SECONDS = 5 * 60
AccountRefreshedCallback = Callable[[Account], Awaitable[None] | None]

logger = logging.getLogger(__name__)


async def request_device_code(
    session: aiohttp.ClientSession,
    tenant_id: str,
    client_id: str,
    *,
    scopes: str = DELEGATED_SCOPES,
    identity_base_url: str = IDENTITY_BASE_URL,
) -> DeviceCode:
    """Start interactive delegated authentication for a command-line application."""
    url = f"{identity_base_url.rstrip('/')}/{quote(tenant_id, safe='')}/oauth2/v2.0/devicecode"
    async with session.post(url, data={"client_id": client_id, "scope": scopes}) as response:
        payload = await _json_or_error(response)
    return DeviceCode(
        device_code=payload["device_code"],
        user_code=payload["user_code"],
        verification_uri=payload["verification_uri"],
        message=payload["message"],
        expires_in=payload["expires_in"],
        interval=payload["interval"],
    )


async def acquire_device_code_token(
    session: aiohttp.ClientSession,
    tenant_id: str,
    client_id: str,
    device_code: str,
    *,
    identity_base_url: str = IDENTITY_BASE_URL,
) -> AccessToken:
    """Poll the device-code token endpoint once."""
    url = f"{identity_base_url.rstrip('/')}/{quote(tenant_id, safe='')}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": device_code,
    }
    async with session.post(url, data=data) as response:
        payload = await _json_or_error(response)
    return _access_token_from_payload(payload)


async def refresh_access_token(
    session: aiohttp.ClientSession,
    tenant_id: str,
    client_id: str,
    refresh_token: str,
    *,
    identity_base_url: str = IDENTITY_BASE_URL,
) -> AccessToken:
    """Exchange a delegated refresh token for a new access token."""
    url = f"{identity_base_url.rstrip('/')}/{quote(tenant_id, safe='')}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": DELEGATED_SCOPES,
    }
    async with session.post(url, data=data) as response:
        payload = await _json_or_error(response)
    return _access_token_from_payload(payload)


async def get_current_user_drive_id(
    session: aiohttp.ClientSession,
    access_token: str,
    *,
    graph_base_url: str = GRAPH_BASE_URL,
) -> str:
    """Return the current delegated user's OneDrive ID before an Account exists."""
    url = f"{graph_base_url.rstrip('/')}/me/drive"
    async with session.get(url, headers={"Authorization": f"Bearer {access_token}"}) as response:
        payload = await _json_or_error(response)
    try:
        return payload["id"]
    except KeyError as error:
        raise GraphApiError(200, None, "Current user has no drive ID", None) from error


class OneDriveClient:
    """Access a OneDrive Business drive with a caller-owned HTTP session and token."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        account: Account,
        *,
        tenant_id: str | None = None,
        client_id: str | None = None,
        on_account_refreshed: AccountRefreshedCallback | None = None,
        api_requests_per_second: int = 10,
        graph_base_url: str = GRAPH_BASE_URL,
    ) -> None:
        refresh_values = (tenant_id, client_id, on_account_refreshed)
        if any(value is not None for value in refresh_values) and not all(
            value is not None for value in refresh_values
        ):
            raise ValueError(
                "tenant_id, client_id, and on_account_refreshed must all be set for automatic refresh"
            )
        if api_requests_per_second <= 0:
            raise ValueError("api_requests_per_second must be positive")
        self._session = session
        self._account = account
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._on_account_refreshed = on_account_refreshed
        self._graph_base_url = graph_base_url.rstrip("/")
        self._refresh_lock = asyncio.Lock()
        # A one-token bucket enforces an even gap instead of permitting a burst each second.
        self._request_limiter = AsyncLimiter(1, 1 / api_requests_per_second)

    @property
    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._account.access_token}"}

    async def get_item(self, drive_id: str, item_id: str) -> DriveItem:
        """Return a drive item's manifest metadata by ID."""
        url = self._item_url(drive_id, item_id)
        payload = await self._get_json(url, params={"$select": ITEM_FIELDS})
        return DriveItem.from_graph(payload)

    async def get_item_by_path(self, drive_id: str, path: str) -> DriveItem:
        """Resolve an absolute drive-relative path, including the root path ``/``."""
        normalized = path.strip("/")
        if normalized:
            encoded_path = quote(normalized, safe="/")
            url = f"{self._drive_url(drive_id)}/root:/{encoded_path}"
        else:
            url = f"{self._drive_url(drive_id)}/root"
        payload = await self._get_json(url, params={"$select": ITEM_FIELDS})
        return DriveItem.from_graph(payload)

    async def get_delta_page(
        self,
        drive_id: str,
        root_item_id: str,
        cursor: str | None = None,
    ) -> DeltaPage:
        """Get one delta page, following a complete Graph cursor URL when supplied."""
        url = cursor or f"{self._item_url(drive_id, root_item_id)}/delta"
        params = None if cursor else {"$select": ITEM_FIELDS}
        payload = await self._get_json(url, params=params)
        return DeltaPage(
            items=tuple(DriveItem.from_graph(item) for item in payload["value"]),
            next_link=payload.get("@odata.nextLink"),
            delta_link=payload.get("@odata.deltaLink"),
        )

    async def get_download_url(self, drive_id: str, item_id: str) -> str:
        """Fetch a fresh, short-lived download URL for a file by ID."""
        attempt = 1
        while True:
            try:
                payload = await self._get_json(
                    self._item_url(drive_id, item_id),
                    params={"$select": "@microsoft.graph.downloadUrl"},
                    timeout=aiohttp.ClientTimeout(total=DOWNLOAD_URL_TIMEOUT_SECONDS),
                )
                try:
                    return payload["@microsoft.graph.downloadUrl"]
                except KeyError as error:
                    raise GraphApiError(200, None, "Item has no download URL", None) from error
            except (asyncio.TimeoutError, aiohttp.ClientError) as error:
                reason = type(error).__name__
            except GraphApiError as error:
                if error.status not in {408, 429, 500, 502, 503, 504}:
                    raise
                reason = f"Graph {error.status}: {error.message}"
            delay = _download_url_retry_delay(attempt)
            logger.warning(
                "获取下载直链失败：文件 %s，尝试 #%d，原因 %s；%d 秒后重试",
                item_id,
                attempt,
                reason,
                delay,
            )
            await asyncio.sleep(delay)
            attempt += 1

    def _drive_url(self, drive_id: str) -> str:
        return f"{self._graph_base_url}/drives/{quote(drive_id, safe='')}"

    def _item_url(self, drive_id: str, item_id: str) -> str:
        return f"{self._drive_url(drive_id)}/items/{quote(item_id, safe='')}"

    async def _get_json(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> dict[str, Any]:
        failed_account = self._account
        try:
            return await self._get_json_once(url, params=params, timeout=timeout)
        except GraphApiError as error:
            if error.status != 401 or error.code != "InvalidAuthenticationToken":
                raise
        await self._refresh_after_auth_failure(failed_account)
        return await self._get_json_once(url, params=params, timeout=timeout)

    async def _get_json_once(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> dict[str, Any]:
        async with self._request_limiter:
            async with self._session.get(
                url, headers=self._headers, params=params, timeout=timeout
            ) as response:
                return await _json_or_error(response)

    async def _refresh_after_auth_failure(self, failed_account: Account) -> None:
        if self._tenant_id is None or self._client_id is None or self._on_account_refreshed is None:
            raise GraphApiError(
                401,
                "InvalidAuthenticationToken",
                "Access token expired and automatic refresh is not configured",
                None,
            )

        async with self._refresh_lock:
            if self._account is not failed_account:
                return
            token = await refresh_access_token(
                self._session,
                self._tenant_id,
                self._client_id,
                self._account.refresh_token,
            )
            refreshed_account = Account(
                drive_id=self._account.drive_id,
                token_type=token.token_type,
                access_token=token.token,
                refresh_token=token.refresh_token or self._account.refresh_token,
                expires_at=datetime.now(UTC) + timedelta(seconds=token.expires_in),
            )
            callback_result = self._on_account_refreshed(refreshed_account)
            if inspect.isawaitable(callback_result):
                await callback_result
            self._account = refreshed_account


async def _json_or_error(response: aiohttp.ClientResponse) -> dict[str, Any]:
    try:
        payload = await response.json(content_type=None)
    except (aiohttp.ContentTypeError, ValueError):
        payload = {}

    if response.status < 400:
        return payload

    error = payload.get("error", {})
    if isinstance(error, str):
        code = error
        message = payload.get("error_description", error)
    else:
        code = error.get("code")
        message = error.get("message", "Unknown error")
    request_id = response.headers.get("request-id") or response.headers.get("x-ms-request-id")
    exception_type = DeltaCursorExpiredError if response.status == 410 else GraphApiError
    raise exception_type(response.status, code, message, request_id)


def _access_token_from_payload(payload: dict[str, Any]) -> AccessToken:
    return AccessToken(
        token=payload["access_token"],
        token_type=payload["token_type"],
        expires_in=payload["expires_in"],
        refresh_token=payload.get("refresh_token"),
    )


def _download_url_retry_delay(attempt: int) -> int:
    if attempt >= 10:
        return DOWNLOAD_URL_RETRY_MAX_SECONDS
    return 2 ** (attempt - 1)
