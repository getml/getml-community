# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Holds Formatter and FormatColumn classes which are used for formatting tabular output
within the getML python API.
"""

from __future__ import annotations

import datetime
import html
import itertools as it
import math
from collections import namedtuple
from copy import deepcopy
from numbers import Number
from typing import Optional

import numpy as np

from ..templates import environment
from .cell_formatter import CellFormatter

BASE_PRECISION = 4
"""
The baseline (minimum) number of significant digits when formatting floats.
"""
MAX_CELL_WIDTH = 32

MIN_CELL_WIDTH = 16

# --------------------------------------------------------------------


def _clip_lines(lines, max_cols):
    if len(lines[0]) > max_cols:
        threshold = max_cols // 2
        lines_clipped = []
        ellipses = ["..." if row[0].strip() else "   " for row in lines]
        for line, ellipsis in zip(lines, ellipses):
            line_ = list(line)
            lines_clipped.append(line_[:threshold] + [ellipsis] + line_[-threshold:])
        return lines_clipped
    return lines


# --------------------------------------------------------------------


def _expand_cells(cells, depths, width=4):
    cells_expanded = []
    pad_cell = " " * width

    for cell, row_depth in zip(cells, depths):
        cell_expanded = [cell] if not isinstance(cell, list) else list(cell)
        depth = len(cell)
        if depth < row_depth:
            remaining = row_depth - depth
            cell_expanded += [pad_cell] * remaining
        cells_expanded.extend(cell_expanded)

    return cells_expanded


# --------------------------------------------------------------------


def _clip_cell(cell, width):
    if len(cell) > width:
        return f"{cell:.{width}}..."
    return cell


# --------------------------------------------------------------------


def _get_depth(cell):
    return len(cell) if isinstance(cell, list) else 1


# --------------------------------------------------------------------


def _get_width(cell, decimal_places=None):
    if isinstance(cell, list):
        if len(cell) > 0:
            return max(_get_width(elem, decimal_places) for elem in cell)
        return 0
    if isinstance(cell, float) and decimal_places is not None:
        return len(f"{cell: .{decimal_places}f}")
    return min(len(str(cell)), MAX_CELL_WIDTH)


# --------------------------------------------------------------------


def _infer_precision(cells):
    cells = np.array(cells)
    cells = cells[~np.isnan(cells)]

    if len(cells) == 0:
        return 0

    max_frac = max(len(str(cell).split(".")[1].rstrip("0")) for cell in cells)

    if all(cells > 1):
        return min(max_frac, BASE_PRECISION)

    if all(cells == 0):
        return 1

    for i in range(10):
        if all(cells // 10**-i >= 1):
            return min(max_frac, i - 1 + BASE_PRECISION)

    return BASE_PRECISION


# --------------------------------------------------------------------


def _split_lines(lines, margin, max_width):
    lines_split = []
    lines_remaining = []

    for line in lines:
        truncated = list(_truncate_line(line, margin, max_width))
        remaining = line[len(truncated) :]

        lines_split.append(truncated)

        if remaining:
            index = [line[0]]
            lines_remaining.append(index + remaining)

    if lines_remaining:
        lines_split.append([""])
        lines_split.extend(_split_lines(lines_remaining, margin, max_width))

    return lines_split


# --------------------------------------------------------------------


def _truncate_line(line, margin, max_width, template="{}"):
    used_width = 0

    for cell in line:
        cell_width = len(template.format(cell)) + margin
        available_width = max_width - used_width
        if cell_width + used_width <= max_width:
            yield cell
        elif cell_width > max_width and available_width >= MIN_CELL_WIDTH:
            clipped_cell = _clip_cell(cell, available_width)
            cell_width = len(template.format(clipped_cell)) + margin
            yield clipped_cell
        used_width += cell_width


# --------------------------------------------------------------------


class _Formatter:
    """A formatter for tabular output of data (usually a container of some kind)."""

    # the margin between columns (in chars)
    margin = 3
    # maximum tolerable width of an output line (in chars)
    max_width = 130
    # maximum number of output rows
    max_rows = 10
    # maximum number of output columns
    max_cols = 10

    template = environment.get_template("container.jinja2")

    # ------------------------------------------------------------

    def __init__(self, headers, rows):
        self.data = []

        depths = [max(_get_depth(cell) for cell in row) for row in rows]

        columns = zip(*rows)
        headers = zip(*headers)

        for header, cells in it.zip_longest(headers, columns, fillvalue=[]):  # type: ignore
            self.data.append(
                _FormatColumn(
                    list(header),
                    list(cells),
                    self.max_rows,
                    depths,
                )
            )

        self.n_rows: Optional[int] = self.data[0].n_cells

        self._add_index()

    # ------------------------------------------------------------

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            fmt = deepcopy(self)
            cols = self.data[key]
            fmt.data = cols
            return fmt
        if isinstance(key, str):
            try:
                return [column for column in self.data if column._name == key][0]  # type: ignore
            except IndexError as exc:
                raise KeyError(f"No FormatColumn with name: {key}") from exc
        raise TypeError(
            f"Formatters can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ------------------------------------------------------------

    def __repr__(self):
        output = [f"{'column':20}{'cell template':25}{'header template':15}"]
        for column in self.data:
            output.append(
                f"{column.name:20}{column.cell_template!r:25}{column.header_template!r:15}"
            )
        return "\n".join(output)

    # ------------------------------------------------------------

    def _pivot(self):
        columns = [column.data for column in self.data]
        return list(zip(*columns))

    # ------------------------------------------------------------

    def _add_index(self):
        index_column = _IndexColumn(
            headers=[""],
            n_cells=self.n_rows,
            length=self.max_rows,
        )

        self.data = [index_column] + self.data

    # ------------------------------------------------------------

    def _remove_index(self):
        if self.index:
            self.data.pop(0)

    # ------------------------------------------------------------

    def _render_head(self):
        columns_formatted = [column._format_cells() for column in self.data]

        depths = [
            [len(cell) if isinstance(cell, list) else 1 for cell in column.sub_headers]
            for column in self.data
        ]

        max_depths = [
            max(row_depth) for row_depth in it.zip_longest(*depths, fillvalue=0)
        ]

        headers = [column.header for column in columns_formatted]
        sub_headers_cols = [
            _expand_cells(column.sub_headers, max_depths, column.width)
            for column in columns_formatted
        ]
        sub_headers = [list(sub_header) for sub_header in zip(*sub_headers_cols)]

        return [headers] + sub_headers

    # ------------------------------------------------------------

    def _render_body(self, as_html=False):
        if not as_html:
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

    def _render_string(self):
        headers = self._render_head()
        rows = self._render_body()

        lines = headers + rows
        lines_clipped = _clip_lines(lines, self.max_cols)
        lines_split = _split_lines(lines_clipped, self.margin, self.max_width)

        margin = " " * self.margin
        lines_rendered = [margin.join(line) for line in lines_split]

        return "\n".join(lines_rendered)

    # ------------------------------------------------------------

    def _render_html(self):
        headers = self._render_head()
        rows = self._render_body(as_html=True)

        # fall back to str for columns holding multiple types
        dtypes = [
            column.dtype.__name__ if isinstance(column.dtype, type) else "str"
            for column in self.data
        ]

        cell = namedtuple("cell", ["dtype", "value"])
        headers = [
            [cell(role, header) for role, header in zip(dtypes, headers)]
            for headers in headers
        ]
        rows = [
            [cell(dtype, value) for dtype, value in zip(dtypes, row)] for row in rows
        ]

        return self.template.render(headers=headers, rows=rows)

    # ------------------------------------------------------------

    @property
    def index(self):
        """
        Returns the IndexColumn of a Formatter, if any is present.
        """
        if isinstance(self.data[0], _IndexColumn):
            return self.data[0]


# --------------------------------------------------------------------


class _FormatColumn:
    """A formatable column of an output table."""

    # ------------------------------------------------------------

    def __init__(self, headers, cells, max_cells, depths=None, index=None):
        self.data = cells
        if self._instances_are(float):
            self.precision = _infer_precision(self._unnest())
        self.depths = depths or [1] * (len(cells) + 1)
        self.formatted = False
        self.header = headers[0]
        self.max_cells = max_cells
        self.n_cells = len(self.data)  # just an alias here
        self.sub_headers = []
        if len(headers) > 1:
            self.sub_headers = headers[1:]

        self.cell_template = self.default_cell_template
        self.header_template = self.default_header_template
        self.index = index

    # ------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ------------------------------------------------------------

    def __repr__(self):
        max_length = 10
        if self.formatted:
            cells = self._unnest()[:max_length]
        else:
            cells = [str(cell) for cell in self._unnest()[:max_length]]

        output = "\n".join(
            [self.header] + [f"{self.cell_template:^{self.width}}"] + cells
        )

        if len(self) > max_length:
            output += f"\n{'...':^{self.width}}"

        return output

    # ------------------------------------------------------------

    def _expand_cells(self):
        return _expand_cells(self.data, self.depths, self.width)

    # ------------------------------------------------------------

    def _format_cells(self):
        # make format-relevant params accessible as local vars
        fmt_params = {"width": self.width}

        fmt = CellFormatter()

        column_formatted = deepcopy(self)

        def fmt_cell(cell, template):
            if isinstance(cell, list):
                if not cell:
                    yield cell
                for sub_cell in cell:
                    yield from fmt_cell(sub_cell, template)
            else:
                yield fmt.format(template, cell, cell=cell, **fmt_params)

        if self.nested:
            cells_formatted = [
                [
                    fmt.format(self.cell_template, elem, cell=elem, **fmt_params)
                    for elem in cell
                ]
                for cell in self.data
            ]

        else:
            cells_formatted = [
                fmt.format(self.cell_template, cell, cell=cell, **fmt_params)  # type: ignore
                for cell in self.data
            ]

        column_formatted.data = cells_formatted

        column_formatted.header = fmt.format(
            self.header_template, self.header, cell=self.header, **fmt_params
        )

        column_formatted.sub_headers = list(
            fmt_cell(self.sub_headers, self.header_template)
        )

        column_formatted.formatted = True

        return column_formatted

    # ------------------------------------------------------------

    def _instances_are(self, type_):
        if self.nested:
            return all(isinstance(elem, type_) for cell in self.data for elem in cell)
        return all(isinstance(cell, type_) for cell in self.data)

    # ------------------------------------------------------------

    def _unnest(self):
        if self.nested:
            return [elem for cell in self.data for elem in cell]
        return self.data

    # ------------------------------------------------------------

    def _unnest_and_strip_html(self):
        if self.nested:
            return [
                "<br>".join([html.escape(elem).strip() for elem in cell])
                for cell in self.data
            ]
        return [html.escape(cell.strip()) for cell in self.data]

    # ------------------------------------------------------------

    @property
    def default_cell_template(self):
        """
        The default cell template. Will be set based on the type
        of the column's cells.
        """
        if self._instances_are(float):
            return "{:" f" {self.width}.{self.precision}fd" "}"
        if self._instances_are(Number):
            return "{:" f">{self.width}" "}"
        if self._instances_are(datetime.datetime):
            return "{:" "%Y-%m-%d %H:%M:%S" "}"
        return "{:" f"{self.width}" "}"

    # ------------------------------------------------------------

    @property
    def default_header_template(self):
        """
        The default header template. Will be set based on the type
        of the column's cells.
        """
        if self._instances_are(float):
            return "{:" f">{self.width}" "}"
        if self._instances_are(datetime.datetime):
            # the default datetime format has a fixed
            # width of 19 chars
            return "{:" f"{self.width}" "}"
        return self.default_cell_template

    # ------------------------------------------------------------

    def _clip(self, ellipses="...") -> _FormatColumn:
        column_clipped = deepcopy(self)
        if self.n_cells > self.max_cells:
            head = self.data[: self.max_cells // 2]
            tail = self.data[-self.max_cells // 2 :]
            ellipses = [self.header_template.format(ellipses, cell=ellipses)]
            if self.nested:
                ellipses = [ellipses]
            column_clipped.data = head + ellipses + tail
            return column_clipped

        return column_clipped

    # ------------------------------------------------------------

    @property
    def dtype(self):
        """
        Returns the type of a column's cells. If a column contains cells of more than
        one type, returns the set of the types it holds. Returns str if a column
        contains only empty lists or Nones.
        """
        # TODO Implement more sanity checks
        if self.nested:
            dtypes = set(type(elem) for cell in self.data for elem in cell)
        else:
            dtypes = set(type(cell) for cell in self.data)

        if len(dtypes) == 1:
            return dtypes.pop()

        if len(dtypes) > 1:
            return dtypes

        # fallback to str for empty lists or None
        return str

    # ------------------------------------------------------------

    @property
    def name(self):
        """
        The name of the column (usually the header).
        """
        return self.header

    # ------------------------------------------------------------

    @property
    def nested(self):
        """
        If a column contains only lists, it is regarded as "nested". Nested
        columns can be formatted like normal columns. For rendering, nested columns can
        be unnested, while the unnesting strategy depends on the rendering target. See:
        _expand_cells, _unnest and _unnest_and_strip_html.
        """
        return all(isinstance(cell, list) for cell in self.data)

    # ------------------------------------------------------------

    @property
    def width(self):
        """
        The width of a column (in chars). With parsing enabled the final width of column
        of floats depends on the precision specified.
        """
        return max(
            _get_width(cell, getattr(self, "precision", None))
            for cell in [self.header] + self.sub_headers + self.data
        )


# --------------------------------------------------------------------


class _IndexColumn(_FormatColumn):
    def __init__(self, headers, n_cells, length):
        length = min(length, n_cells)
        head = list(range(math.ceil(length / 2)))
        tail = list(range(n_cells - length // 2, n_cells))
        cells = head + tail
        super().__init__(cells=cells, headers=headers, max_cells=length)
        self.n_cells = n_cells

    # ------------------------------------------------------------

    def _clip(self):
        return super()._clip(ellipses="")

    # ------------------------------------------------------------

    @property
    def name(self):
        """
        The name of an IndexColumn is always "index".
        """
        return "index"
