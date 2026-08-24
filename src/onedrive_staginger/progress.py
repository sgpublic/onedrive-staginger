"""Live terminal progress display for one download invocation."""

from __future__ import annotations

import logging

from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import BarColumn, DownloadColumn, Progress, TaskID, TextColumn, TimeRemainingColumn


class TransferProgress:
    """Render total network progress and one persistent row per transfer slot."""

    def __init__(self) -> None:
        self._console = Console(stderr=True)
        self._slots = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TimeRemainingColumn(),
            console=self._console,
        )
        self._total = Progress(
            TextColumn("总下载 {task.fields[files]}"),
            BarColumn(),
            DownloadColumn(),
            console=self._console,
        )
        self._live = Live(Group(self._slots, self._total), console=self._console, refresh_per_second=10)
        self._total_task: TaskID | None = None
        self._slot_tasks: dict[str, TaskID] = {}
        self._slot_names: dict[str, str] = {}
        self._downloaded: dict[str, int] = {}
        self._completed_files = 0
        self._total_files = 0
        self._original_handlers: list[logging.Handler] | None = None

    def __enter__(self) -> TransferProgress:
        root = logging.getLogger()
        self._original_handlers = root.handlers[:]
        root.handlers = [RichHandler(console=self._console, show_path=False)]
        self._live.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._live.stop()
        if self._original_handlers is not None:
            logging.getLogger().handlers = self._original_handlers

    def start(self, total_bytes: int, total_files: int) -> None:
        self._total_files = total_files
        self._total_task = self._total.add_task(
            "total", total=total_bytes, files=f"0/{total_files} 个文件"
        )

    def start_slot(
        self,
        transfer_id: str,
        description: str,
        total: int,
        completed: int = 0,
        *,
        counts_download: bool,
    ) -> None:
        self._slot_tasks[transfer_id] = self._slots.add_task(description, total=total, completed=completed)
        self._slot_names[transfer_id] = description.removeprefix("准备传输：")
        if counts_download:
            self._downloaded[transfer_id] = completed
        self._update_total()

    def update_download(self, transfer_id: str, completed: int, total: int) -> None:
        task_id = self._slot_tasks[transfer_id]
        self._slots.update(
            task_id,
            completed=completed,
            total=total,
            description=f"下载中：{self._slot_names[transfer_id]}",
        )
        self._downloaded[transfer_id] = completed
        self._update_total()

    def start_move(self, transfer_id: str, total: int) -> None:
        task_id = self._slot_tasks[transfer_id]
        self._slots.update(
            task_id,
            description=f"移动中：{self._slot_names[transfer_id]}",
            total=total,
            completed=0,
        )

    def update_move(self, transfer_id: str, completed: int) -> None:
        self._slots.update(self._slot_tasks[transfer_id], completed=completed)

    def finish_slot(self, transfer_id: str, *, counts_download: bool) -> None:
        task_id = self._slot_tasks.pop(transfer_id)
        self._slot_names.pop(transfer_id)
        self._slots.remove_task(task_id)
        if counts_download:
            self._completed_files += 1
        self._update_total()

    def _update_total(self) -> None:
        if self._total_task is None:
            return
        self._total.update(
            self._total_task,
            completed=sum(self._downloaded.values()),
            files=f"{self._completed_files}/{self._total_files} 个文件",
        )
