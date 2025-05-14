# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Displays progress in bar form."""

from __future__ import annotations

import os
import warnings
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Iterator, Optional, Type

from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
)
from rich.progress import Progress as RichProgress
from rich.style import Style

from getml.events.handlers import engine_event_handler, monitor_event_handler
from getml.events.types import Event, EventTypeState


def _is_emacs_kernel() -> bool:
    try:
        from IPython.core.getipython import get_ipython  # type: ignore[name-defined]
    except ImportError:
        return False

    ip = get_ipython()
    if ip is None:
        return False
    if ip_kernel_app := ip.config.get("IPKernelApp"):
        connection_file = Path(ip_kernel_app.get("connection_file", ""))
        return connection_file.name.startswith("emacs")
    return False


def _is_jupyter() -> bool:
    return Console().is_jupyter


def _is_vscode() -> bool:
    return os.getenv("VSCODE_PID") is not None


def _is_jupyter_without_ipywidgets() -> bool:
    if not _is_jupyter():
        return False
    try:
        import ipywidgets  # type: ignore[name-defined]
    except ImportError:
        return not _is_emacs_kernel() and not _is_vscode()
    return False


_ENV_VAR_TRUTHY_VALUES = ("1", "t", "true", "True")

SPEED_ESTIMATE_PERIOD = 300
TASK_FAILED_DESCRIPTION = "[danger]Failed[/danger]"
FORCE_TEXTUAL_OUTPUT = (
    os.getenv("GETML_PROGRESS_FORCE_TEXTUAL_OUTPUT") in _ENV_VAR_TRUTHY_VALUES
    or _is_emacs_kernel()
    or _is_vscode()
    or _is_jupyter_without_ipywidgets()
)
"""
If set to True, forces the progress bar to be displayed in textual form.
Neccessary because terminal-based environments communicating with jupyter
kernels over zmq are falsely detected as jupyter environments with ipywidget
support by rich.
"""
FORCE_MONOCHROME_OUTPUT = (
    os.getenv("GETML_PROGRESS_FORCE_MONOCHROME_OUTPUT") in _ENV_VAR_TRUTHY_VALUES
    or FORCE_TEXTUAL_OUTPUT
    and not _is_emacs_kernel()
)
"""
If set to True, forces the progress bar to be displayed in monochrome form.
"""
SHOW_IPYWIDGETS_WARNING = (
    os.getenv("GETML_SHOW_IPYWIDGETS_WARNING", "True") in _ENV_VAR_TRUTHY_VALUES
)
"""
If set to True, shows a warning when ipywidgets are not available in a jupyter
environment.
"""


if SHOW_IPYWIDGETS_WARNING and _is_jupyter_without_ipywidgets():
    warnings.warn(
        """
        'ipywidgets' are currently not installed in your environment. For
        an optimal output experience, install 'ipywidgets'. To suppress
        this warning in the future, set the environment variable
        GETML_SHOW_IPYWIDGETS_WARNING to 'f'.
        """,
        UserWarning,
    )


class ProgressBarStyle(Enum):
    """
    Enum mapping progress bar styles to rich style specifications.
    Member values are the valid style specs for rich.

    See:
    https://github.com/Textualize/rich/blob/master/rich/default_styles.py
    """

    COMPLETE = Style(color="red")
    FINISHED = Style(color="green")
    PULSE = "bar.pulse"


class ProgressBarColumns(Enum):
    BASE = (
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(
            complete_style=ProgressBarStyle.COMPLETE.value,
            finished_style=ProgressBarStyle.FINISHED.value,
            pulse_style=ProgressBarStyle.PULSE.value,
        ),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    )
    TIME = (TimeRemainingColumn(elapsed_when_finished=True, compact=True),)
    DEFAULT = (
        *BASE,
        TextColumn("•"),
        *TIME,
    )
    DOWNLOAD = (
        *BASE,
        TextColumn("•"),
        DownloadColumn(),
        TextColumn("•"),
        *TIME,
    )


class ProgressType(Enum):
    """
    Enum representing different types of progress bars.
    """

    DEFAULT = "Default"
    DOWNLOAD = "Download"

    @property
    def columns(self):
        return ProgressBarColumns[self.name].value


class Progress:
    """
    Displays progress in bar form.

    Thin wrapper around rich.progress.Progress.
    """

    def __init__(
        self,
        *,
        progress_type: ProgressType = ProgressType.DEFAULT,
        finish_all_tasks_on_stop: bool = True,
    ):
        """
        Handles multiple progress bars and updates them.

        Args:
            progress_type: Type of progress bar to display. Defaults to
              ProgressType.DEFAULT.
            finish_all_tasks_on_exit: If True, all tasks are finished when
              __exit__ is called. Defaults to True.
        """
        self.finish_all_tasks_on_stop = finish_all_tasks_on_stop
        self.progress_type = progress_type

        console = Console(
            color_system="standard" if FORCE_TEXTUAL_OUTPUT else "auto",
            force_jupyter=False if FORCE_TEXTUAL_OUTPUT else None,
            force_terminal=FORCE_TEXTUAL_OUTPUT or None,
            force_interactive=FORCE_TEXTUAL_OUTPUT or None,
            no_color=FORCE_MONOCHROME_OUTPUT,
        )

        self._progress = RichProgress(
            *progress_type.columns,
            speed_estimate_period=SPEED_ESTIMATE_PERIOD,
            console=console,
        )
        self.descriptions = {}

    def add_task(
        self,
        description: str,
        *,
        total: int = 100,
        completed: int = 0,
        visible: bool = True,
    ) -> TaskID:
        if not self.live.is_started:
            self.start()
        task_id = self._progress.add_task(
            description, total=total, completed=completed, visible=visible
        )
        self.descriptions[task_id] = description
        return task_id

    def advance(self, task_id: TaskID, *, steps: float = 1):
        if task_id in self.task_ids:
            self._progress.advance(task_id, advance=steps)

    @property
    def console(self) -> Console:
        return self._progress.console

    def finish(self, task_id: TaskID, refresh: bool = True):
        if task_id in self.task_ids:
            self.update(task_id, completed=self.tasks[task_id].total, refresh=refresh)
        if task_id == len(self.task_ids) - 1:
            self._set_finished_time(task_id)

    def finish_all(self):
        for task in self.tasks:
            self.finish(task.id)

    @property
    def live(self):
        return self._progress.live

    def new_task(
        self,
        description: str,
        *,
        total: int = 100,
        completed: int = 0,
        visible: bool = True,
    ) -> TaskID:
        """
        Adds a new task and finishes all other tasks.
        """
        self.finish_all()
        task_id = self.add_task(
            description, total=total, completed=completed, visible=visible
        )
        return task_id

    def new_task_and_reconstruct_renderable_maybe(
        self,
        description: str,
        *,
        total: int = 100,
        completed: int = 0,
        visible: bool = True,
    ) -> TaskID:
        """
        Adds a new task and finishes all other tasks. Reconstructs the
        renderable if FORCE_TEXTUAL_OUTPUT is True.
        """
        if FORCE_TEXTUAL_OUTPUT and self.tasks:
            self.stop()
            self._progress = RichProgress(
                *self.progress_type.columns,
                speed_estimate_period=SPEED_ESTIMATE_PERIOD,
                console=self.console,
            )
            self.start()
        task_id = self.new_task(
            description, total=total, completed=completed, visible=visible
        )
        return task_id

    def refresh(self):
        self._progress.refresh()

    def _reset_description(self, task_id: TaskID):
        if (
            task_id in self.task_ids
            and self.tasks[task_id].description != self.descriptions[task_id]
            and self.tasks[task_id].description != TASK_FAILED_DESCRIPTION
        ):
            self.update(task_id, description=self.descriptions[task_id])

    def _set_finished_time(self, task_id: TaskID):
        if task_id in self.task_ids:
            self.tasks[task_id].finished_time = self.tasks[task_id].elapsed
            self.refresh()

    def set_all_failed(self):
        for task in self.tasks:
            if not task.finished:
                self.update(task.id, description=TASK_FAILED_DESCRIPTION)

    @property
    def tasks(self):
        return self._progress.tasks

    @property
    def task_ids(self):
        return self._progress.task_ids

    def start(self):
        self._progress.start()

    def stop(self):
        if self.finish_all_tasks_on_stop:
            self.finish_all()
        self._progress.stop()

    def update(
        self,
        task_id: TaskID,
        *,
        completed: Optional[float] = None,
        description: Optional[str] = None,
        refresh: bool = True,
    ):
        if task_id in self.task_ids:
            self._progress.update(
                task_id,
                description=description,
                completed=completed,
                refresh=refresh,
            )
            if self.tasks[task_id].finished:
                self._reset_description(task_id)

    def __enter__(self) -> Progress:
        # Nop because we start the live renderable lazily upon adding the first
        # task to avoid the display of control chars when forcefully outputting
        # text inside jupyter
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional[TracebackType] = None,
    ):
        if exc_type is not None:
            self.set_all_failed()

        self.stop()


class ProgressEventHandler:
    def __init__(self, progress: Progress):
        self.progress = progress

    @contextmanager
    def live_renderable(self, event: Event) -> Iterator[Live]:
        yield self.progress.live
        if event.type.state is EventTypeState.END:
            self.progress.stop()

    def create_or_update_progress(self, event: Event):
        """
        Create a new progress task or update an existing one.
        """

        with self.live_renderable(event):
            if event.type.state is EventTypeState.START:
                task_id = self.progress.new_task_and_reconstruct_renderable_maybe(
                    event.attributes["description"]
                )
            elif event.type.state is EventTypeState.PROGRESS and self.progress.tasks:
                *_, task_id = self.progress.task_ids
                self.progress.update(
                    task_id,
                    description=event.attributes["description"],
                    completed=event.attributes["progress"],
                )


progress_event_handler = ProgressEventHandler(progress=Progress())


@monitor_event_handler
@engine_event_handler
def show_progress(event: Event):
    progress_event_handler.create_or_update_progress(event)
