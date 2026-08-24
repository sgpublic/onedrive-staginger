"""Command-line entry point for one migration task."""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .app import download, login, sync
from .config import ConfigError
from .guides import AZURE_CLIENT_GUIDE
from .onedrive import GraphApiError
from .task import MigrationTask


logger = logging.getLogger(__name__)


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OneDrive staging migration tool.")
    parser.add_argument("config_dir", type=Path, metavar="CONFIG_DIR")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "login",
        help="Sign in and save the current OneDrive account.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=AZURE_CLIENT_GUIDE,
    )
    commands.add_parser("sync", help="Apply OneDrive delta changes to the local manifest.")
    download_parser = commands.add_parser(
        "download", help="Download one static manifest subtree to local storage."
    )
    download_parser.add_argument("temp_dir", type=Path, metavar="TEMP_DIR")
    download_parser.add_argument("dist_dir", type=Path, metavar="DIST_DIR")
    download_parser.add_argument("remote_root_path", metavar="REMOTE_ROOT_PATH")
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    try:
        namespace = parse_args(args)
        if namespace.command == "login":
            account = asyncio.run(login(namespace.config_dir, notify=logger.info))
            logger.info("OneDrive account saved: %s", account.drive_id)
            return
        if namespace.command == "sync":
            root = asyncio.run(sync(namespace.config_dir))
            logger.info("Manifest synchronized: %s (%s)", root.id, root.name)
            return
        task = MigrationTask(namespace.temp_dir, namespace.dist_dir, namespace.remote_root_path)
        asyncio.run(download(namespace.config_dir, task))
    except (ConfigError, GraphApiError, ValueError) as error:
        raise SystemExit(f"Error: {error}") from error
    logger.info("Download complete")


if __name__ == "__main__":
    main()
