# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Displays progress in bar form."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Dict, Literal, Optional, Type

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.progress import Progress as RichProgress
from rich.style import Style


def _is_emacs_kernel() -> bool:
    try:
        from IPython.core.getipython import get_ipython  # type: ignore[name-defined]
    except ImportError:
        return False

    ip = get_ipython()
    if ip is None:
        return False
    if ip_kernel_app := ip.config.get("IPKernelApp"):
        connection_file = Path(ip_kernel_app.get("connection_file"))
        return connection_file.name.startswith("emacs")
    return False


def _is_jupyter_without_ipywidgets() -> bool:
    is_jupyter = Console().is_jupyter
    if not is_jupyter:
        return False
    try:
        import ipywidgets  # type: ignore[name-defined]
    except ImportError:
        return True
    return False


FORCE_TEXTUAL_OUTPUT = (
    os.getenv("GETML_PROGRESS_FORCE_TEXTUAL_OUTPUT") in ("1", "t", "true", "True")
    or _is_emacs_kernel()
    or _is_jupyter_without_ipywidgets()
)
"""
If set to True, forces the progress bar to be displayed in textual form.
Neccessary because terminal-based environments communicating with jupyter
kernels over zmq are falsely detected as jupyter environments with ipywidget
support.
"""


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


class _Progress:
    """displays progress in bar form."""

    def __init__(
        self,
        *,
        progress_type: Literal["Default", "Download"] = "Default",
        set_finished_on_exit: bool = True,
    ):
        """
        Handles multiple progress bars and updates them.
        Args:
            progress_type (Literal["Default", "Download"], optional): Type of
              progress bar to display. Defaults to "Default".
            set_finished_on_exit (bool, optional): If True, sets the progress
              bar to 100% when __exit__ is called. Defaults to True.
        """
        self._task_descriptions: Dict[int, str] = {}
        self._set_finished_on_close = set_finished_on_exit

        if progress_type == "Default":
            progress_columns = (
                TextColumn("[progress.description]{task.description}"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                BarColumn(
                    complete_style=ProgressBarStyle.COMPLETE.value,
                    finished_style=ProgressBarStyle.FINISHED.value,
                    pulse_style=ProgressBarStyle.PULSE.value,
                ),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            )
        elif progress_type == "Download":
            progress_columns = (
                TextColumn("[progress.description]{task.description}"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                BarColumn(
                    complete_style=ProgressBarStyle.COMPLETE.value,
                    finished_style=ProgressBarStyle.FINISHED.value,
                    pulse_style=ProgressBarStyle.PULSE.value,
                ),
                TextColumn("•"),
                DownloadColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            )

        console = Console(
            color_system="standard" if FORCE_TEXTUAL_OUTPUT else "auto",
            force_jupyter=False if FORCE_TEXTUAL_OUTPUT else None,
            force_terminal=FORCE_TEXTUAL_OUTPUT or None,
            force_interactive=FORCE_TEXTUAL_OUTPUT or None,
        )
        self._progress = RichProgress(
            *progress_columns, speed_estimate_period=300, console=console
        )

    def update_if_possible(
        self, *, sub_description: str = "", completed: int = 0, refresh: bool = True
    ):
        """
        Updates the progress bar if there is an active task which is not finished.
        Args:
            sub_description (str, optional): Sub description to add to the
              progress bar description which was set when calling new(). Defaults
              to "".
            completed (int, optional): Amount of progress completed. Defaults to 0.
            refresh (bool, optional): If True, refreshes the progress bar.
              Defaults to True.
        """
        if not self._progress.tasks:
            return
        task_id = self._progress.tasks[-1].id
        description = self._task_descriptions[task_id]
        new_description = (
            description + " " + sub_description if sub_description else description
        )
        self._progress.update(
            task_id, description=new_description, completed=completed, refresh=refresh
        )

    def new(
        self,
        description: str,
        total: int = 100,
        completed: int = 0,
        visible: bool = True,
    ) -> TaskID:
        """
        Adds a new progress bar and sets the old one to fitting.
        Args:
            description (str): Description of the progress bar.
            total (int, optional): Total amount of progress bar. Defaults to
              100.
            completed (int, optional): Amount of progress completed. Defaults to
              0.
            visible (bool, optional): If True, the progress bar is visible.
              Defaults to True.
        """
        if self._progress.tasks and not self._progress.tasks[-1].finished:
            task = self._progress.tasks[-1]
            self._progress.update(
                task.id,
                description=self._task_descriptions[task.id],
                completed=task.total,
            )
        task_id = self._progress.add_task(
            description, total=total, completed=completed, visible=visible
        )
        self._task_descriptions[task_id] = description
        return task_id

    def start(self):
        self._progress.start()

    def close(self):
        self._progress.stop()

    def __enter__(self) -> _Progress:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if not self._progress.tasks:
            self.close()
            return

        task = self._progress.tasks[-1]

        if exc_type is not None:
            self._progress.update(
                task.id,
                description=task.description + " [danger]Failed[/danger]",
                refresh=True,
            )
        elif self._set_finished_on_close and not task.finished:
            self._progress.update(
                task.id,
                description=self._task_descriptions[task.id],
                completed=task.total,
                refresh=True,
            )

        self.close()
