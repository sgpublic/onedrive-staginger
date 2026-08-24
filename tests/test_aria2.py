from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, sentinel, patch

from onedrive_staginger.aria2 import Aria2Process
from onedrive_staginger.config.settings import Aria2Config


class FakeProcess:
    def __init__(self, *, wait_result: int = 0) -> None:
        self.returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait = AsyncMock(return_value=wait_result)

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1


class Aria2ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_dir = Path(self.temp_dir.name)
        self.config = Aria2Config(
            executable="aria2c-test", disk_cache=256 * 1024 * 1024, file_allocation="falloc"
        )

    async def asyncTearDown(self) -> None:
        self.temp_dir.cleanup()

    async def test_starts_with_private_rpc_and_fixed_session_path(self) -> None:
        process = FakeProcess()
        manager = Aria2Process(self.config, self.config_dir)

        with patch("onedrive_staginger.aria2.asyncio.create_subprocess_exec", return_value=process) as create:
            await manager.start()

        args = create.await_args.args
        self.assertEqual(args[0], "aria2c-test")
        self.assertIn("--enable-rpc=true", args)
        self.assertIn("--rpc-listen-all=false", args)
        self.assertTrue(any(arg.startswith("--rpc-listen-port=") for arg in args))
        self.assertTrue(any(arg.startswith("--rpc-secret=") for arg in args))
        self.assertIn("--continue=true", args)
        self.assertIn("--disk-cache=268435456", args)
        self.assertIn("--file-allocation=falloc", args)
        self.assertIn(f"--save-session={self.config_dir / 'aria2.session'}", args)

    async def test_creates_client_with_private_rpc_credentials(self) -> None:
        manager = Aria2Process(self.config, self.config_dir)
        manager._process = FakeProcess()  # type: ignore[assignment]

        with patch("onedrive_staginger.aria2.Aria2HttpClient", return_value=sentinel.client) as client:
            result = manager.create_client(sentinel.session)

        self.assertIs(result, sentinel.client)
        client.assert_called_once_with(
            f"http://127.0.0.1:{manager._port}/jsonrpc",
            token=manager._secret,
            client_session=sentinel.session,
        )

    async def test_rejects_client_before_process_starts(self) -> None:
        manager = Aria2Process(self.config, self.config_dir)

        with self.assertRaisesRegex(RuntimeError, "not running"):
            manager.create_client(sentinel.session)

    async def test_closes_process_gracefully(self) -> None:
        process = FakeProcess()
        manager = Aria2Process(self.config, self.config_dir)
        manager._process = process  # type: ignore[assignment]

        await manager.aclose()

        self.assertEqual(process.terminate_calls, 1)
        process.wait.assert_awaited_once()
        self.assertEqual(process.kill_calls, 0)

    async def test_kills_process_after_graceful_shutdown_timeout(self) -> None:
        process = FakeProcess()
        process.wait = AsyncMock(side_effect=[TimeoutError(), 0])
        manager = Aria2Process(self.config, self.config_dir)
        manager._process = process  # type: ignore[assignment]

        await manager.aclose()

        self.assertEqual(process.terminate_calls, 1)
        self.assertEqual(process.kill_calls, 1)
        self.assertEqual(process.wait.await_count, 2)

    async def test_propagates_start_failure_without_process(self) -> None:
        manager = Aria2Process(self.config, self.config_dir)

        with patch(
            "onedrive_staginger.aria2.asyncio.create_subprocess_exec",
            side_effect=FileNotFoundError("aria2c-test"),
        ):
            with self.assertRaises(FileNotFoundError):
                await manager.start()

        self.assertIsNone(manager._process)


if __name__ == "__main__":
    unittest.main()
