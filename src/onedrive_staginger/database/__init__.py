"""SQLite database schema for persistent migration state."""

from .models import (
    DELTA_CURSOR_KEY,
    MANIFEST_ROOT_ID_KEY,
    MANIFEST_STATE_KEY,
    KeyValue,
    ManifestState,
    OneDriveItem,
    database,
    ensure_root_item,
    get_manifest_child,
    get_manifest_root_id,
    iter_manifest_children,
    initialize_database,
    persist_delta_page,
    get_value,
    set_manifest_state,
)

__all__ = [
    "DELTA_CURSOR_KEY",
    "MANIFEST_ROOT_ID_KEY",
    "MANIFEST_STATE_KEY",
    "KeyValue",
    "ManifestState",
    "OneDriveItem",
    "database",
    "ensure_root_item",
    "get_manifest_child",
    "get_manifest_root_id",
    "iter_manifest_children",
    "initialize_database",
    "persist_delta_page",
    "get_value",
    "set_manifest_state",
]
