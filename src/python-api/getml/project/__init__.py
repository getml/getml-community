# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This module helps you handle your current project.
"""

from sys import modules
from types import ModuleType
from typing import TYPE_CHECKING

from getml.engine import is_alive

from .attrs import __getattr__, _all

if TYPE_CHECKING:
    from .attrs import delete, load, restart, save, suspend, switch  # noqa: F401
    from .containers import DataFrames, Hyperopts, Pipelines

    data_frames: DataFrames
    hyperopts: Hyperopts
    pipelines: Pipelines
    name: str


def __dir__():
    return _all


# Fix mkdocs (griffe) warning about non-string item in _all
_all = [str(item) for item in _all]

__all__ = _all + ["DataFrames", "Hyperopts", "Pipelines"] + ["__getattr__"]  # noqa: PLE0605


class ProjectModule(ModuleType):
    def __repr__(self):
        output = "No project set. To set: `getml.set_project(...)`"
        # resolving name has the side effect of reaching out to the Monitor and
        # the Engine and therefore triggers a message if either is unreachable
        project_name = self.name  # pylint: disable=E1101
        if is_alive():
            output = f"Current project:\n\n{project_name}"
        return output


modules[__name__].__class__ = ProjectModule
