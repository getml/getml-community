# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Displays progress in bar form."""

from __future__ import annotations

from types import TracebackType
from typing import Dict, Literal, Optional, Type

from rich.progress import Progress as RichProgress, TaskID
from rich.progress import (
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    DownloadColumn,
)


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
            progress_type (Literal["Default", "Download"], optional): Type of progress bar to display. Defaults to "Default".
            set_finished_on_exit (bool, optional): If True, sets the progress bar to 100% when __exit__ is called. Defaults to True.
        """
        self._task_descriptions: Dict[int, str] = {}
        self._set_finished_on_close = set_finished_on_exit

        if progress_type == "Default":
            progress_columns = (
                TextColumn("[progress.description]{task.description}"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                BarColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            )
        elif progress_type == "Download":
            progress_columns = (
                TextColumn("[progress.description]{task.description}"),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                BarColumn(),
                TextColumn("•"),
                DownloadColumn(),
                TextColumn("•"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
            )
        self._progress = RichProgress(*progress_columns, speed_estimate_period=300)

    def update_if_possible(
        self, *, sub_description: str = "", completed: int = 0, refresh: bool = True
    ):
        """
        Updates the progress bar if there is an active task which is not finished.
        Args:
            sub_description (str, optional): Sub description to add to the progress bar description which was set when calling new(). Defaults to "".
            completed (int, optional): Amount of progress completed. Defaults to 0.
            refresh (bool, optional): If True, refreshes the progress bar. Defaults to True.
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
            total (int, optional): Total amount of progress bar. Defaults to 100.
            completed (int, optional): Amount of progress completed. Defaults to 0.
            visible (bool, optional): If True, the progress bar is visible. Defaults to True.
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
