from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import Any

from onedrive_staginger.config import Account
from onedrive_staginger.onedrive import (
    DeltaCursorExpiredError,
    GraphApiError,
    OneDriveClient,
    acquire_device_code_token,
    get_current_user_drive_id,
    refresh_access_token,
    request_device_code,
)


class FakeResponse:
    def __init__(
        self,
        status: int,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def json(self, *, content_type: str | None = None) -> dict[str, Any]:
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(("GET", url, kwargs))
        return self.responses.pop(0)

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append(("POST", url, kwargs))
        return self.responses.pop(0)


class OneDriveClientTests(unittest.IsolatedAsyncioTestCase):
    def _account(self, *, access_token: str = "access-token") -> Account:
        return Account(
            drive_id="b!drive-id",
            token_type="Bearer",
            access_token=access_token,
            refresh_token="refresh-token",
            expires_at=datetime(2026, 8, 24, 12, 0, tzinfo=UTC),
        )

    def _client(self, responses: list[FakeResponse]) -> tuple[OneDriveClient, FakeSession]:
        session = FakeSession(responses)
        return (
            OneDriveClient(session, self._account(), graph_base_url="https://graph.test/v1.0"),
            session,
        )

    async def test_acquires_delegated_device_code_and_refresh_tokens(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    200,
                    {
                        "device_code": "device-code",
                        "user_code": "USER-CODE",
                        "verification_uri": "https://microsoft.test/devicelogin",
                        "message": "Sign in.",
                        "expires_in": 900,
                        "interval": 5,
                    },
                ),
                FakeResponse(
                    200,
                    {
                        "access_token": "access-1",
                        "refresh_token": "refresh-1",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    },
                ),
                FakeResponse(
                    200,
                    {
                        "access_token": "access-2",
                        "refresh_token": "refresh-2",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    },
                ),
            ]
        )

        device_code = await request_device_code(session, "tenant", "client", identity_base_url="https://login.test")
        token = await acquire_device_code_token(session, "tenant", "client", device_code.device_code, identity_base_url="https://login.test")
        refreshed = await refresh_access_token(session, "tenant", "client", token.refresh_token, identity_base_url="https://login.test")

        self.assertEqual(device_code.user_code, "USER-CODE")
        self.assertEqual(token.refresh_token, "refresh-1")
        self.assertEqual(refreshed.token, "access-2")
        self.assertEqual(session.calls[1][2]["data"]["grant_type"], "urn:ietf:params:oauth:grant-type:device_code")
        self.assertEqual(session.calls[2][2]["data"]["grant_type"], "refresh_token")

    async def test_resolves_path_and_returns_manifest_metadata(self) -> None:
        client, session = self._client(
            [
                FakeResponse(
                    200,
                    {
                        "id": "item-1",
                        "name": "video.mkv",
                        "size": 123,
                        "eTag": "etag",
                        "lastModifiedDateTime": "2026-08-24T12:00:00Z",
                        "parentReference": {"id": "parent-1"},
                        "file": {"hashes": {"quickXorHash": "xor", "sha256Hash": "sha"}},
                    },
                )
            ]
        )

        item = await client.get_item_by_path("drive-1", "/Media/Anime/video.mkv")

        self.assertEqual(item.id, "item-1")
        self.assertEqual(item.parent_id, "parent-1")
        self.assertEqual(item.hashes.sha256_hash, "sha")
        self.assertEqual(
            session.calls[0][1],
            "https://graph.test/v1.0/drives/drive-1/root:/Media/Anime/video.mkv",
        )
        self.assertEqual(session.calls[0][2]["headers"]["Authorization"], "Bearer access-token")

    async def test_delta_follows_full_next_link_without_select_parameters(self) -> None:
        next_link = "https://graph.test/v1.0/drives/drive-1/items/root/delta?token=opaque&skiptoken=2"
        client, session = self._client(
            [
                FakeResponse(200, {"value": [{"id": "folder", "name": "Media", "folder": {}}], "@odata.nextLink": next_link}),
                FakeResponse(200, {"value": [{"id": "deleted", "deleted": {}}], "@odata.deltaLink": "delta-link"}),
            ]
        )

        first_page = await client.get_delta_page("drive-1", "root")
        second_page = await client.get_delta_page("ignored", "ignored", first_page.next_link)

        self.assertTrue(first_page.items[0].is_folder)
        self.assertEqual(second_page.delta_link, "delta-link")
        self.assertEqual(session.calls[0][2]["params"]["$select"], "id,name,size,eTag,lastModifiedDateTime,parentReference,file,folder,deleted")
        self.assertEqual(session.calls[1][1], next_link)
        self.assertIsNone(session.calls[1][2]["params"])

    async def test_gets_fresh_download_url(self) -> None:
        client, _ = self._client([FakeResponse(200, {"@microsoft.graph.downloadUrl": "https://download.test/file"})])

        url = await client.get_download_url("drive-1", "item-1")

        self.assertEqual(url, "https://download.test/file")

    async def test_gets_current_delegated_user_drive_id(self) -> None:
        session = FakeSession([FakeResponse(200, {"id": "b!drive-id"})])

        drive_id = await get_current_user_drive_id(
            session, "access-token", graph_base_url="https://graph.test/v1.0"
        )

        self.assertEqual(drive_id, "b!drive-id")
        self.assertEqual(session.calls[0][1], "https://graph.test/v1.0/me/drive")

    async def test_refreshes_expired_token_notifies_callback_and_retries_request(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    401,
                    {"error": {"code": "InvalidAuthenticationToken", "message": "Expired"}},
                ),
                FakeResponse(
                    200,
                    {
                        "access_token": "new-access-token",
                        "refresh_token": "new-refresh-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                    },
                ),
                FakeResponse(200, {"id": "item-1", "name": "file.txt", "file": {}}),
            ]
        )
        refreshed_accounts: list[Account] = []

        async def save_refreshed_account(account: Account) -> None:
            refreshed_accounts.append(account)

        client = OneDriveClient(
            session,
            self._account(),
            tenant_id="tenant",
            client_id="client",
            on_account_refreshed=save_refreshed_account,
            graph_base_url="https://graph.test/v1.0",
        )

        item = await client.get_item("drive-1", "item-1")

        self.assertEqual(item.id, "item-1")
        self.assertEqual(refreshed_accounts[0].access_token, "new-access-token")
        self.assertEqual(refreshed_accounts[0].refresh_token, "new-refresh-token")
        self.assertEqual(session.calls[0][2]["headers"]["Authorization"], "Bearer access-token")
        self.assertEqual(session.calls[1][2]["data"]["refresh_token"], "refresh-token")
        self.assertEqual(session.calls[2][2]["headers"]["Authorization"], "Bearer new-access-token")

    def test_rejects_non_positive_api_rate(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            OneDriveClient(FakeSession([]), self._account(), api_requests_per_second=0)

    async def test_raises_graph_error_with_request_id(self) -> None:
        client, _ = self._client(
            [FakeResponse(429, {"error": {"code": "tooManyRequests", "message": "Slow down"}}, {"request-id": "request-1"})]
        )

        with self.assertRaises(GraphApiError) as context:
            await client.get_item("drive-1", "item-1")

        self.assertEqual(context.exception.status, 429)
        self.assertEqual(context.exception.request_id, "request-1")

    async def test_raises_dedicated_error_for_expired_delta_cursor(self) -> None:
        client, _ = self._client([FakeResponse(410, {"error": {"code": "syncStateNotFound", "message": "Expired"}})])

        with self.assertRaises(DeltaCursorExpiredError):
            await client.get_delta_page("drive-1", "root", "https://graph.test/expired")


if __name__ == "__main__":
    unittest.main()
