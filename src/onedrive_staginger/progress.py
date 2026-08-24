"""Live terminal progress display for one download invocation."""

from __future__ import annotations

import logging

from rich.console import Console, Group
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
)
from rich.text import Text


class TwoDecimalDownloadColumn(ProgressColumn):
    """Render completed and total bytes with two decimal places."""

    def render(self, task: Task) -> Text:
        return Text(f"{_byte_text(task.completed)} / {_byte_text(task.total or 0)}")


class TransferProgress:
    """Render total network progress and one persistent row per transfer slot."""

    def __init__(self) -> None:
        self._console = Console(stderr=True)
        self._slots = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TextColumn("{task.fields[speed]}"),
            TimeRemainingColumn(),
            console=self._console,
        )
        self._total = Progress(
            TextColumn("总下载 {task.fields[files]}"),
            BarColumn(),
            TwoDecimalDownloadColumn(),
            console=self._console,
        )
        self._live = Live(Group(self._slots, self._total), console=self._console, refresh_per_second=10)
        self._total_task: TaskID | None = None
        self._slot_tasks: dict[str, TaskID] = {}
        self._slot_names: dict[str, str] = {}
        self._downloaded: dict[str, int] = {}
        self._completed_bytes = 0
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
        task_id = self._slot_tasks.get(transfer_id)
        if task_id is None:
            self._slot_tasks[transfer_id] = self._slots.add_task(
                description, total=total, completed=completed, speed=""
            )
        else:
            self._slots.update(task_id, description=description, total=total, completed=completed, speed="")
        self._slot_names[transfer_id] = description.removeprefix("准备传输：")
        if counts_download:
            self._downloaded[transfer_id] = completed
        self._update_total()

    def start_verification(self, transfer_id: str, description: str, total: int) -> None:
        task_id = self._slot_tasks.get(transfer_id)
        if task_id is None:
            self._slot_tasks[transfer_id] = self._slots.add_task(
                f"校验中：{description}", total=total, completed=0, speed=""
            )
        else:
            self._slots.update(
                task_id, description=f"校验中：{description}", total=total, completed=0, speed=""
            )
        self._slot_names[transfer_id] = description

    def update_verification(self, transfer_id: str, completed: int) -> None:
        task_id = self._slot_tasks.get(transfer_id)
        if task_id is not None:
            self._slots.update(task_id, completed=completed)

    def update_download(self, transfer_id: str, completed: int, total: int, speed: int) -> None:
        task_id = self._slot_tasks[transfer_id]
        self._slots.update(
            task_id,
            completed=completed,
            total=total,
            description=f"下载中：{self._slot_names[transfer_id]}",
            speed=_speed_text(speed),
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

    def update_move(self, transfer_id: str, completed: int, speed: int) -> None:
        self._slots.update(
            self._slot_tasks[transfer_id], completed=completed, speed=_speed_text(speed)
        )

    def finish_verification(self, transfer_id: str) -> None:
        task_id = self._slot_tasks.pop(transfer_id, None)
        self._slot_names.pop(transfer_id, None)
        if task_id is not None:
            self._slots.remove_task(task_id)

    def complete_file(self, transfer_id: str, total: int) -> None:
        self.finish_verification(transfer_id)
        self._downloaded.pop(transfer_id, None)
        self._completed_bytes += total
        self._completed_files += 1
        self._update_total()

    def finish_slot(self, transfer_id: str, *, completed: bool, total: int) -> None:
        self.finish_verification(transfer_id)
        self._downloaded.pop(transfer_id, None)
        if completed:
            self._completed_bytes += total
            self._completed_files += 1
        self._update_total()

    def _update_total(self) -> None:
        if self._total_task is None:
            return
        self._total.update(
            self._total_task,
            completed=self._completed_bytes + sum(self._downloaded.values()),
            files=f"{self._completed_files}/{self._total_files} 个文件",
        )


def _speed_text(bytes_per_second: int) -> str:
    if bytes_per_second <= 0:
        return ""
    units = ("B/s", "KiB/s", "MiB/s", "GiB/s")
    value = float(bytes_per_second)
    for unit in units[:-1]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} {units[-1]}"


def _byte_text(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units[:-1]:
        if value < 1024:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} {units[-1]}"
