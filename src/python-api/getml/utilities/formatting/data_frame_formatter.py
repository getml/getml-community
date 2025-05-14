# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Holds the DataFrameFormatter.
"""

import re
from collections import namedtuple
from copy import deepcopy
from inspect import cleandoc
from typing import Any, Dict, List, Union

import numpy as np

from getml.data.roles import (
    join_key,
    numerical,
    target,
    time_stamp,
    unused_float,
)

from ..templates import environment
from .formatter import (
    MAX_CELL_WIDTH,
    _clip_cell,
    _FormatColumn,
    _Formatter,
    _IndexColumn,
    _infer_precision,
)
from .helpers import _get_data_frame_content

UNITS = ["D", "h", "m", "s", "ms"]
"""
Numpy datetime units.
"""

UNITS_FORMATS = dict(
    D="{:%Y-%m-%d}",
    h="{:%Y-%m-%d %H}",
    m="{:%Y-%m-%d %H:%M}",
    s="{:%Y-%m-%d %H:%M:%S}",
    ms="{:%Y-%m-%d %H:%M:%S.%f}",
)
"""
A mapping of numpy datetime units to strftime format specifiers.
"""

# --------------------------------------------------------------------


def _get_subroles(col):
    subroles = {}

    subroles["exclude"] = [
        _parse_subrole(subrole)
        for subrole in getattr(col, "subroles", [])
        if subrole.startswith("exclude ")
    ]
    subroles["include"] = [
        _parse_subrole(subrole)
        for subrole in getattr(col, "subroles", [])
        if subrole.startswith("include ")
    ]
    subroles["only"] = [
        _parse_subrole(subrole)
        for subrole in getattr(col, "subroles", [])
        if subrole.startswith("only ")
    ]

    return subroles


# --------------------------------------------------------------------


def _parse_subrole(subrole):
    prefix = re.compile(r"^(exclude |include |only )")
    return re.sub(prefix, "", subrole)


# --------------------------------------------------------------------


def _structure_subroles(df):
    subroles: Dict[str, List[str]] = {"exclude": [], "include": [], "only": []}
    df_subroles = [_get_subroles(df[col]) for col in df.colnames]

    for col_subroles in df_subroles:
        for cat, values in subroles.items():
            values.append(col_subroles[cat])

    return subroles


# --------------------------------------------------------------------


def _infer_resolution(cells):
    cells = np.array(cells, dtype="datetime64")
    deltas = np.unique(cells[1:] - cells[:-1])

    for unit in UNITS:
        normalized_deltas = np.array(
            [np.timedelta64(delta, "ms") for delta in deltas]
        ) / np.timedelta64(1, unit)
        if all(np.mod(normalized_deltas, 1) == 0):
            return unit


# --------------------------------------------------------------------


def _max_digits(cells):
    cells = [cell for cell in cells if "." in cell]
    if len(cells) > 0:
        return max(len(cell.split(".")[1]) for cell in cells)
    return 0


# --------------------------------------------------------------------


class _DataFrameFormatter(_Formatter):
    """
    A Formatter for DataFrames.
    """

    max_rows = 10

    template = environment.get_template("data_frame.jinja2")

    # ------------------------------------------------------------

    def __init__(self, df, num_head=max_rows // 2, num_tail=max_rows // 2):
        self.colnames = df.colnames
        self.n_rows = len(df)
        self.name = df.name
        self.memory_usage = df.memory_usage

        num_head = min(num_head, self.n_rows // 2 + 1)
        num_tail = min(num_tail, self.n_rows // 2)

        roles = [df[colname].role for colname in df.colnames]

        units = [df[colname].unit for colname in df.colnames]

        self.units = None

        self.subroles = {
            cat: subroles
            for cat, subroles in _structure_subroles(df).items()
            if any(subrole for subrole in subroles)
        }

        head = _get_data_frame_content(name=df.name, start=0, length=num_head)["data"]
        if num_tail > 0:
            tail = _get_data_frame_content(
                name=df.name, start=int(self.n_rows - num_tail), length=num_tail
            )["data"]
        else:
            tail = []

        rows = head + tail

        columns = [list(col) for col in zip(*rows)]

        headers = [self.colnames] + [roles]

        if any(unit != "" for unit in units):
            self.units = units
            headers += [units]

        if self.subroles:
            headers += [[""] * len(columns)] + [
                subroles
                for subroles in self.subroles.values()
                if any(subrole for subrole in subroles)
            ]

        headers = list(zip(*headers))

        self.data = [
            _DataFrameFormatColumn(
                headers=list(header),
                cells=cells,
                max_cells=self.max_rows,
                n_cells=self.n_rows,
                role=role,
            )
            for header, role, cells in zip(headers, roles, columns)
        ]

        index_headers = ["name"]
        if self.roles:
            index_headers += ["role"]
        if self.units:
            index_headers += ["unit"]
        if self.subroles:
            index_headers += ["subroles:"] + [f"- {cat:7}" for cat in self.subroles]

        self._add_index(index_headers)

    # ------------------------------------------------------------

    def __len__(self):
        return self.n_rows

    # ------------------------------------------------------------

    def _render_footer_lines(self, footer):
        if footer.url is None or footer.name is None:
            return cleandoc(
                f"""
                {footer.n_rows} rows x {footer.n_cols} columns
                memory usage: {footer.memory_usage:.2f} MB
                type: {footer.type}
                """
            )
        return cleandoc(
            f"""
            {footer.n_rows} rows x {footer.n_cols} columns
            memory usage: {footer.memory_usage:.2f} MB
            name: {footer.name}
            type: {footer.type}
            url: {footer.url}
            """
        )

    # ------------------------------------------------------------

    def _add_index(self, headers):
        index_column = _IndexColumn(
            headers=headers,
            n_cells=self.n_rows or self.max_rows + 1,
            length=self.max_rows,
        )

        self.data = [index_column] + self.data

    # ------------------------------------------------------------

    def _render_html(self, footer=None):
        headers = self._render_head()
        rows = self._render_body(as_html=True)

        precisions = [getattr(column, "precision", None) for column in self.data]

        cell = namedtuple("cell", ["role", "value"])
        headers = [
            [cell(role, header) for role, header in zip(self.roles, headers)]
            for headers in headers
        ]
        rows = [
            [cell(role, value) for role, value in zip(self.roles, row)] for row in rows
        ]

        return self.template.render(
            headers=headers, rows=rows, precisions=precisions, footer=footer
        )

    # ------------------------------------------------------------

    def _render_body(self, as_html=False):
        for col in self.data:
            if col._instances_are(str):
                col.data = [_clip_cell(cell, MAX_CELL_WIDTH) for cell in col.data]

        columns_formatted = [column._format_cells() for column in self.data]

        columns_clipped = [column._clip() for column in columns_formatted]

        if as_html:
            cells = [column._unnest_and_strip_html() for column in columns_clipped]
        else:
            cells = [column._expand_cells() for column in columns_clipped]

        rows = [list(row) for row in zip(*cells)]

        return rows

    # ------------------------------------------------------------

    def _render_string(self, footer=None):
        lines = super()._render_string()

        footer_lines = self._render_footer_lines(footer) if footer else ""

        return lines + "\n\n\n" + footer_lines

    @property
    def roles(self):
        return [getattr(column, "role", None) for column in self.data]


# --------------------------------------------------------------------


class _DataFrameFormatColumn(_FormatColumn):
    """
    A Formatter for columns of a DataFrame.

    The cell format is set based on the column's role.
    """

    # ------------------------------------------------------------

    def __init__(self, *args, n_cells, role=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.n_cells = n_cells
        self.resolution = None
        self.role = role

        # self.parse_cells()
        if self.role in [numerical, target, time_stamp, join_key, unused_float]:
            self.precision = _max_digits(self.data)

        self.cell_template = self.default_cell_template
        self.header_template = self.default_header_template

    # ------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ------------------------------------------------------------

    def _format_cells(self):
        """
        A patch to justify time stamps. This is necessary because afaik we are not able
        to specify justification within strftime format strings.
        """
        cells_formatted = super()._format_cells()

        if self.role == "time_stamp":
            cells_formatted.data = [
                cell.ljust(self.width) for cell in cells_formatted.data
            ]

        return cells_formatted

    # ------------------------------------------------------------

    def _clip(self):
        ellipses: Union[str, List[Any]] = "..." + getattr(self, "precision", 0) * " "
        column_clipped = deepcopy(self)

        if self.role is None:
            ellipses = [self.header_template.format(ellipses)]
            if self.nested:
                ellipses = [ellipses]
            column_clipped.data += ellipses
            return column_clipped

        return super()._clip(ellipses=ellipses)

    # ------------------------------------------------------------

    def _parse_cells(self):
        # some fun parsing
        if self.role == time_stamp:
            if all(cell.isdigit() for cell in self.data):
                self.data = np.array(self.data, dtype="float")
                self.data = np.array(self.data, dtype="datetime64[s]").tolist()
            else:
                self.data = np.array(self.data, dtype="datetime64[ms]").tolist()

            resolutions = list(
                set(
                    [
                        _infer_resolution(self.data[: len(self.data) // 2]),
                        _infer_resolution(self.data[len(self.data) // 2 :]),
                    ]
                )
            )

            # sort resolutions in ascending order and pick the smallest one
            self.resolution = sorted(
                resolutions, key=lambda res: UNITS.index(res), reverse=True
            )[0]

        if self.role in [numerical, target, join_key, unused_float]:
            # this logic might be too simplistic, but data comes from the Engine
            # in a standardized format anyway, so...¯\_(ツ)_/¯
            if any("." in cell for cell in self.data):
                self.data = [float(cell) for cell in self.data]
                self.precision = _infer_precision(self.data)
            elif all(cell.isdigit() for cell in self.data):
                self.data = [int(cell) for cell in self.data]

    # ------------------------------------------------------------

    @property
    def default_cell_template(self):
        if getattr(self, "role", None) in [
            join_key,
            numerical,
            target,
            time_stamp,
            unused_float,
        ]:
            if all(str(cell).isdigit() for cell in self.data):
                return "{:" f">{self.width}" "}"
            return "{:" f" {self.width}.{self.precision}d" "}"

        return super().default_cell_template

    # ------------------------------------------------------------

    @property
    def default_header_template(self):
        if getattr(self, "role", None) in [
            join_key,
            numerical,
            target,
            time_stamp,
            unused_float,
        ]:
            return "{:" f">{self.width}" "}"

        return super().default_header_template
