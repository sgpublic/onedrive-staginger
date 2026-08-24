"""Persist and publish a static OneDrive file tree page by page."""

from __future__ import annotations

import logging

from ..database import (
    DELTA_CURSOR_KEY,
    ManifestState,
    ensure_root_item,
    get_value,
    persist_delta_page,
    set_manifest_state,
)
from ..onedrive import DriveItem, OneDriveClient


logger = logging.getLogger(__name__)


class ManifestPipeline:
    """Enumerate a static Graph delta feed while consumers process ready files."""

    def __init__(
        self,
        client: OneDriveClient,
        drive_id: str,
        root_drive_item_id: str,
    ) -> None:
        self._client = client
        self._drive_id = drive_id
        self._root_drive_item_id = root_drive_item_id

    async def run(self) -> None:
        ensure_root_item(self._root_drive_item_id)
        set_manifest_state(ManifestState.ENUMERATING)
        cursor = get_value(DELTA_CURSOR_KEY)
        logger.info("正在同步 OneDrive 清单%s", "（从已保存的游标继续）" if cursor else "")
        page_number = 0
        while True:
            page = await self._client.get_delta_page(
                self._drive_id,
                self._root_drive_item_id,
                cursor,
            )
            next_cursor = page.next_link or page.delta_link
            if next_cursor is None:
                raise RuntimeError("Graph delta response has neither nextLink nor deltaLink")
            page_number += 1
            persist_delta_page(
                (self._to_row(item) for item in page.items),
                next_cursor,
                manifest_state=(
                    ManifestState.ENUMERATING if page.next_link else ManifestState.COMPLETE
                ),
            )
            logger.info(
                "已同步清单第 %d 页：%d 个项目%s",
                page_number,
                len(page.items),
                "（完成）" if page.next_link is None else "",
            )
            if page.next_link is None:
                return
            cursor = page.next_link

    @staticmethod
    def _to_row(item: DriveItem) -> dict[str, object]:
        hash_type, digest = _preferred_hash(item)
        return {
            "drive_item_id": item.id,
            "parent_drive_item_id": item.parent_id,
            "name": item.name,
            "is_file": item.is_file,
            "is_folder": item.is_folder,
            "is_deleted": item.is_deleted,
            "size": item.size,
            "hash_type": hash_type,
            "hash": digest,
            "etag": item.e_tag,
            "remote_mtime": item.last_modified_date_time,
        }


def _preferred_hash(item: DriveItem) -> tuple[str | None, str | None]:
    if item.hashes is None:
        return None, None
    if item.hashes.sha256_hash:
        return "sha256", item.hashes.sha256_hash
    if item.hashes.sha1_hash:
        return "sha1", item.hashes.sha1_hash
    if item.hashes.quick_xor_hash:
        return "quickXorHash", item.hashes.quick_xor_hash
    return None, None
