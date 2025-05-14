# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of functions for visualizing the data model
that are not intended to be used by
the end-user.
"""

import html

from getml import constants

from .relationship import many_to_many

TABLE_WIDTH = 150
TABLE_HEIGHT = 90

LABEL_WIDTH = 150
LABEL_HEIGHT = 70

LINE_COLOR = "#808080;"


def _make_line(origin, target, color, width):
    line = '<line x1="'
    line += str(origin[1]) + '"'
    line += ' y1="'
    line += str(origin[0]) + '"'
    line += ' x2="'
    line += str(target[1]) + '"'
    line += ' y2="'
    line += str(target[0]) + '"'
    line += ' style="stroke:'
    line += color + ";"
    line += "stroke-width:"
    line += str(width) + '" />'
    return line


def _make_icon(origin, size=48):
    icon = '<rect x="'
    icon += str(origin[1]) + '"'
    icon += ' y="'
    icon += str(origin[0]) + '"'
    icon += ' rx="4" ry="4"'
    icon += ' width="'
    icon += str(size) + '"'
    icon += ' height="'
    icon += str(size) + '"'
    icon += ' style="'
    icon += " fill:#6829c2;"
    icon += "stroke:#ffffff;"
    icon += "stroke-width:3;"
    icon += '" />'

    icon += _make_line(
        origin=(origin[0], origin[1] + size / 3),
        target=(origin[0] + size, origin[1] + size / 3),
        color="white",
        width=3,
    )

    icon += _make_line(
        origin=(origin[0], origin[1] + 2 * size / 3),
        target=(origin[0] + size, origin[1] + 2 * size / 3),
        color="white",
        width=3,
    )

    icon += _make_line(
        origin=(origin[0] + size / 3, origin[1]),
        target=(origin[0] + size / 3, origin[1] + size),
        color="white",
        width=3,
    )

    icon += _make_line(
        origin=(origin[0] + 2 * size / 3, origin[1]),
        target=(origin[0] + 2 * size / 3, origin[1] + size),
        color="white",
        width=3,
    )

    return icon


def _make_table(name, origin):
    table = '<rect y="'
    table += str(origin[0])
    table += '" x="'
    table += str(origin[1])
    table += '" rx="10" ry="10"'
    table += ' width="'
    table += str(TABLE_WIDTH)
    table += '" height="'
    table += str(TABLE_HEIGHT)
    table += '" '
    table += 'style="fill:#6829c2;stroke-width:0;"'
    table += " />"

    table += '<text y="'
    table += str(origin[0] + TABLE_HEIGHT * 0.82) + '"'
    table += ' x="'
    table += str(origin[1] + TABLE_WIDTH / 2) + '"'
    table += ' dominant-baseline="middle" text-anchor="middle"'
    table += ' fill="white">'
    table += name
    table += "</text>"

    table += _make_icon((origin[0] + 10, origin[1] + 51))

    return table


def _make_triangle_down(origin):
    triangle = '<polygon points="'
    triangle += str(origin[1]) + ", "
    triangle += str(origin[0]) + " "
    triangle += str(origin[1] - 6) + ", "
    triangle += str(origin[0] - 10) + " "
    triangle += str(origin[1] + 6) + ", "
    triangle += str(origin[0] - 10) + " "
    triangle += '" style="fill:'
    triangle += LINE_COLOR + ";"
    triangle += "stroke-width:0;"
    triangle += '" />'

    return triangle


def _make_triangle_right(origin):
    triangle = '<polygon points="'
    triangle += str(origin[1]) + ", "
    triangle += str(origin[0]) + " "
    triangle += str(origin[1] - 10) + ", "
    triangle += str(origin[0] - 6) + " "
    triangle += str(origin[1] - 10) + ", "
    triangle += str(origin[0] + 6) + " "
    triangle += '" style="fill:'
    triangle += LINE_COLOR + ";"
    triangle += "stroke-width:0;"
    triangle += '" />'

    return triangle


def _make_label(content, origin):
    table = '<rect y="'
    table += str(origin[0])
    table += '" x="'
    table += str(origin[1])
    table += '" rx="10" ry="10"'
    table += ' width="'
    table += str(LABEL_WIDTH)
    table += '" height="'
    table += str(LABEL_HEIGHT)
    table += '"'
    table += ' style="fill:#6829c2;stroke-width:0;"'
    table += " />"

    table += "<text"
    table += ' dominant-baseline="middle" text-anchor="middle"'
    table += ' fill="white">'

    x = origin[1] + LABEL_WIDTH / 2
    y = origin[0] + LABEL_HEIGHT / 2 - (len(content) - 1) * 5

    for c in content:
        table += '<tspan y="'
        table += str(y) + '"'
        table += ' x="'
        table += str(x) + '"'
        table += ' font-size="7pt"'
        table += " >"
        table += c
        table += "</tspan>"
        y += 10

    table += "</text>"

    return table


def _make_arrow(label, origin, target):
    adj_origin = (origin[0] + TABLE_HEIGHT / 2 - 2, origin[1] + TABLE_WIDTH)
    adj_target = (target[0], target[1] + TABLE_WIDTH / 2 - 2)
    if origin[0] == target[0]:
        adj_target = (target[0] + TABLE_HEIGHT / 2, target[1] - 2)

    label_pos0 = adj_origin[0] - LABEL_HEIGHT / 2 + 2

    if origin[0] == target[0]:
        label_pos1 = adj_origin[1] + (adj_target[1] - adj_origin[1] - LABEL_WIDTH) / 2
    else:
        label_pos1 = (
            adj_origin[1]
            + (adj_target[1] - adj_origin[1] - LABEL_WIDTH) / 2
            - TABLE_WIDTH / 4
        )
    if target[0] == origin[0]:
        arrow = _make_line(
            origin=adj_origin,
            target=(adj_origin[0], adj_target[1] - 8),
            color=LINE_COLOR,
            width=4,
        )

        arrow += _make_triangle_right((adj_target[0] - 2, adj_target[1] + 2))
    else:
        arrow = _make_line(
            origin=adj_origin,
            target=(adj_origin[0], adj_target[1]),
            color=LINE_COLOR,
            width=4,
        )

        arrow += _make_line(
            origin=(adj_origin[0] - 2, adj_target[1]),
            target=(adj_target[0] - 10, adj_target[1]),
            color=LINE_COLOR,
            width=4,
        )

        arrow += _make_triangle_down(adj_target)

    arrow += _make_label(label, (label_pos0, label_pos1))

    return arrow


def _split_multiple_join_keys(multiple):
    multiple = multiple.replace(constants.MULTIPLE_JOIN_KEYS_BEGIN, "")
    multiple = multiple.replace(constants.MULTIPLE_JOIN_KEYS_END, "")
    return multiple.split(constants.JOIN_KEY_SEP)


def _make_join_keys(placeholder, i):
    if constants.JOIN_KEY_SEP in placeholder["join_keys_used_"][i]:
        join_keys = _split_multiple_join_keys(placeholder["join_keys_used_"][i])
        other_join_keys = _split_multiple_join_keys(
            placeholder["other_join_keys_used_"][i]
        )

        if len(join_keys) != len(other_join_keys):
            raise ValueError("Number of join keys does not match!")

        return [other + " = " + jk for other, jk in zip(other_join_keys, join_keys)]

    return [
        placeholder["other_join_keys_used_"][i]
        + " = "
        + placeholder["join_keys_used_"][i]
    ]


def _make_time_stamps(placeholder, i):
    lower = (
        [
            placeholder["other_time_stamps_used_"][i]
            + html.escape(" <= ")
            + placeholder["time_stamps_used_"][i]
        ]
        if placeholder["time_stamps_used_"][i] != constants.ROWID
        else ["rowid <= rowid"]
    )

    upper = (
        [
            placeholder["upper_time_stamps_used_"][i]
            + html.escape(" > ")
            + placeholder["time_stamps_used_"][i]
        ]
        if placeholder["upper_time_stamps_used_"][i]
        else []
    )

    return lower + upper


def _make_window(diff, ts_used):
    if ts_used == constants.ROWID:
        return str(diff) + " time steps"

    seconds_per_day = 24.0 * 60.0 * 60.0
    seconds_per_hour = 60.0 * 60.0
    seconds_per_minute = 60.0

    window_formatted = str(diff) + " seconds"

    if diff >= seconds_per_day:
        window_formatted = str(diff / seconds_per_day) + " days"

    elif diff >= seconds_per_hour:
        window_formatted = str(diff / seconds_per_hour) + " hours"

    elif diff >= seconds_per_minute:
        window_formatted = str(diff / seconds_per_minute) + " minutes"

    return window_formatted


def _make_label_content(placeholder, i):
    jk_used = (
        _make_join_keys(placeholder, i)
        if placeholder["join_keys_used_"][i] != constants.NO_JOIN_KEY
        else []
    )

    ts_used = (
        _make_time_stamps(placeholder, i) if placeholder["time_stamps_used_"][i] else []
    )

    memory = (
        [
            "Memory: "
            + _make_window(
                placeholder["memory_"][i], placeholder["time_stamps_used_"][i]
            )
        ]
        if placeholder["memory_"][i] > 0.0
        else []
    )

    horizon = (
        [
            "Horizon: "
            + _make_window(
                placeholder["horizon_"][i], placeholder["time_stamps_used_"][i]
            )
        ]
        if placeholder["horizon_"][i] != 0.0
        else []
    )

    lagged_targets = (
        ["Lagged targets allowed"] if placeholder["allow_lagged_targets_"][i] else []
    )

    relationship = (
        ["Relationship: " + placeholder["relationship_"][i]]
        if placeholder["relationship_"][i] != many_to_many
        else []
    )

    return (jk_used + ts_used + memory + horizon + lagged_targets + relationship) or [
        "1 = 1"
    ]


class _Diagram:
    def __init__(self, placeholder):
        self.rectangles = []
        self.links = []

        self.height = 0
        self.width = 0

        self._make_structure(0, placeholder._getml_deserialize())
        self._calc_positions()
        self._calc_size()

    def _add_children(self, depth, placeholder):
        child_ids = []

        for joined_table in placeholder["joined_tables_"]:
            if joined_table["joined_tables_"]:
                self._make_structure(depth + 1, joined_table)
            else:
                self._add_placeholder(depth + 1, joined_table)

            child_ids += [len(self.rectangles) - 1]

        return child_ids

    def _add_links(self, placeholder, child_ids, parent_id):
        for i, child_id in enumerate(child_ids):
            label = _make_label_content(placeholder, i)
            link = dict()
            link["source_id"] = child_id
            link["target_id"] = parent_id
            link["label"] = label
            self.links += [link]

    def _add_placeholder(self, depth, placeholder):
        rectangle = dict()
        rectangle["depth"] = depth
        rectangle["id"] = len(self.rectangles)
        rectangle["name"] = placeholder["name_"]
        self.rectangles += [rectangle]
        return rectangle["id"]

    def _calc_positions(self):
        max_depth = self._find_max_depth()

        pos0 = 0

        for i, rectangle in enumerate(self.rectangles):
            if i > 0 and rectangle["depth"] >= self.rectangles[i - 1]["depth"]:
                pos0 += 110

            pos1 = 500 * (max_depth - rectangle["depth"])

            self.rectangles[i]["pos"] = (pos0, pos1)

    def _calc_size(self):
        last = len(self.rectangles) - 1
        self.height = self.rectangles[last]["pos"][0] + TABLE_HEIGHT
        self.width = self.rectangles[last]["pos"][1] + TABLE_WIDTH

    def _find_max_depth(self):
        max_depth = 0

        for rectangle in self.rectangles:
            if rectangle["depth"] > max_depth:
                max_depth = rectangle["depth"]

        return max_depth

    def _make_structure(self, depth, placeholder):
        child_ids = self._add_children(depth, placeholder)
        parent_id = self._add_placeholder(depth, placeholder)
        self._add_links(placeholder, child_ids, parent_id)

    def to_html(self):
        """
        Expresses the data model in pure HTML code.
        """
        output = """<div style="height:""" + str(self.height + 10) + "px;"
        output += "width:" + str(self.width + 10) + "px;"
        output += """position:relative;">"""
        output += '<svg height="' + str(self.height) + '"'
        output += ' width="' + str(self.width) + '"'
        output += ">"

        for rectangle in self.rectangles:
            output += _make_table(rectangle["name"], rectangle["pos"])

        for link in self.links:
            origin = self.rectangles[link["source_id"]]["pos"]
            target = self.rectangles[link["target_id"]]["pos"]
            output += _make_arrow(link["label"], origin, target)

        output += "</svg></div>"

        return output
