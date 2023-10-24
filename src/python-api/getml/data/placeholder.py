# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Abstract representation of tables and their relations."""

# ------------------------------------------------------------------------------

from dataclasses import dataclass, fields
from inspect import cleandoc
from textwrap import indent
from typing import Dict, List, Optional, Sequence, Tuple, Union

from getml.utilities.formatting import _SignatureFormatter

from .helpers import _check_join_key, _handle_on, _handle_ts
from .relationship import many_to_many
from .roles_obj import Roles

# --------------------------------------------------------------------

OnType = Optional[Union[str, Tuple[str, str], List[Union[str, Tuple[str, str]]]]]
TimeStampsType = Optional[Union[str, Tuple[str, str]]]

# ------------------------------------------------------------------------------


class Placeholder:
    """Abstract representation of tables and their relations.

    This class is an abstract representation of the
    :class:`~getml.DataFrame` or :class:`~getml.data.View`.
    However, it does not contain any actual data.

    You might also want to refer to :class:`~getml.data.DataModel`.

    Args:
        name (str):
            The name used for this placeholder. This name will appear
            in the generated SQL code.

    Examples:
        This example will construct a data model in which the
        'population_table' depends on the 'peripheral_table' via
        the 'join_key' column. In addition, only those rows in
        'peripheral_table' for which 'time_stamp' is smaller or
        equal to the 'time_stamp' in 'population_table' are considered:

        .. code-block:: python

            dm = getml.data.DataModel(
                population_table.to_placeholder("POPULATION")
            )

            dm.add(peripheral_table.to_placeholder("PERIPHERAL"))

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on="join_key",
                time_stamps="time_stamp"
            )

        If you want to add more than one peripheral table, you can
        use :func:`~getml.data.to_placeholder`:

        .. code-block:: python

            dm = getml.data.DataModel(
                population_table.to_placeholder("POPULATION")
            )

            dm.add(
                getml.data.to_placeholder(
                    PERIPHERAL1=peripheral_table_1,
                    PERIPHERAL2=peripheral_table_2,
                )
            )

        If the relationship between two tables is many-to-one or one-to-one
        you should clearly say so:

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on="join_key",
                time_stamps="time_stamp",
                relationship=getml.data.relationship.many_to_one,
            )

        Please also refer to :mod:`~getml.data.relationship`.

        If the join keys or time stamps are named differently in the two
        different tables, use a tuple:

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on=("join_key", "other_join_key"),
                time_stamps=("time_stamp", "other_time_stamp"),
            )

        You can join over more than one join key:

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on=["join_key1", "join_key2", ("join_key3", "other_join_key3")],
                time_stamps="time_stamp",
            )

        You can also limit the scope of your joins using *memory*. This
        can significantly speed up training time. For instance, if you
        only want to consider data from the last seven days, you could
        do something like this:

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on="join_key",
                time_stamps="time_stamp",
                memory=getml.data.time.days(7),
            )

        In some use cases, particularly those involving time series, it
        might be a good idea to use targets from the past. You can activate
        this using *lagged_targets*. But if you do that, you must
        also define a prediction *horizon*. For instance, if you want to
        predict data for the next hour, using data from the last seven days,
        you could do this:

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on="join_key",
                time_stamps="time_stamp",
                lagged_targets=True,
                horizon=getml.data.time.hours(1),
                memory=getml.data.time.days(7),
            )

        Please also refer to :mod:`~getml.data.time`.

        If the join involves many matches, it might be a good idea to set the
        relationship to :const:`~getml.data.relationship.propositionalization`.
        This forces the pipeline to always use a propositionalization
        algorithm for this join, which can significantly speed things up.

        .. code-block:: python

            dm.POPULATION.join(
                dm.PERIPHERAL,
                on="join_key",
                time_stamps="time_stamp",
                relationship=getml.data.relationship.propositionalization,
            )

        Please also refer to :mod:`~getml.data.relationship`.

        In some cases, it is necessary to have more than one placeholder
        on the same table. This is necessary to create more complicated
        data models. In this case, you can do something like this:

        .. code-block:: python

            dm.add(
                getml.data.to_placeholder(
                    PERIPHERAL=[peripheral_table]*2,
                )
            )

            # We can now access our two placeholders like this:
            placeholder1 = dm.PERIPHERAL[0]
            placeholder2 = dm.PERIPHERAL[1]

        If you want to check out a real-world example where this
        is necessary, refer to the
        `CORA notebook <https://nbviewer.getml.com/github/getml/getml-demo/blob/master/cora.ipynb>`_.

    """

    def __init__(
        self, name: str, roles: Optional[Union[Roles, Dict[str, List[str]]]] = None
    ):
        self._name = name

        if roles is None:
            self._roles: Roles = Roles()
        elif isinstance(roles, dict):
            self._roles = Roles(**roles)
        else:
            self._roles = roles

        self.joins: List[Join] = []
        self.parent = None

    def __dir__(self):
        attrs = dir(type(self)) + list(self.__dict__.keys())
        attrs.extend(col for col in self.columns if col.isidentifier())
        return attrs

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            super().__getattribute__(key)

    def __getitem__(self, key):
        if key in vars(self)["_roles"].columns:
            return key
        else:
            raise KeyError(
                f"No column with with name {key!r} on the Placeholder's signature."
            )

    def _getml_deserialize(self):
        cmd = {"name_": self.name, "roles_": self.roles.to_dict()}

        cmd["allow_lagged_targets_"] = [join.lagged_targets for join in self.joins]

        cmd["horizon_"] = [join.horizon or 0.0 for join in self.joins]

        cmd["join_keys_used_"] = [_handle_on(join.on)[0] for join in self.joins]

        cmd["joined_tables_"] = [join.right._getml_deserialize() for join in self.joins]

        cmd["memory_"] = [join.memory or 0.0 for join in self.joins]

        cmd["other_join_keys_used_"] = [_handle_on(join.on)[1] for join in self.joins]

        cmd["other_time_stamps_used_"] = [
            _handle_ts(join.time_stamps)[1] for join in self.joins
        ]

        cmd["relationship_"] = [join.relationship for join in self.joins]

        cmd["time_stamps_used_"] = [
            _handle_ts(join.time_stamps)[0] for join in self.joins
        ]

        cmd["upper_time_stamps_used_"] = [
            join.upper_time_stamp or "" for join in self.joins
        ]

        return cmd

    def __repr__(self) -> str:
        template = cleandoc(
            """
            {name}:
              columns:
            {cols}
            """
        )

        if self.joins:
            template += "\n\n" + cleandoc(
                """
                  joins:
                {joins}
                """
            )

        def format_on(on, join: Join):
            template = "({left.name}.{on[0]}, {join.right.name}.{on[1]})"

            if isinstance(on, list) and all(isinstance(key, tuple) for key in on):
                formatted = "\n- " + "\n- ".join(
                    template.format(on=keys, left=self, join=join) for keys in on
                )
                return indent(formatted, " " * 2)

            return template.format(on=on, left=self, join=join)

        cols = [
            f"- {col}: {role}" for col, role in zip(self.columns, self.roles.to_list())
        ]

        if len(cols) > 5:
            cols = cols[:5] + ["- ..."]

        joins = []

        for join in self.joins:
            for param, value in vars(join).items():
                if param == "right":
                    joins.append(f"- right: {join.right.name!r}")
                    continue

                if value is not None:
                    if param == "on":
                        joins.append(f"  on: {format_on(value, join)}")
                    elif param == "time_stamps":
                        joins.append(
                            f"  {param}: ({self.name}.{value[0]}, {join.right.name}.{value[1]})"
                        )
                    else:
                        joins.append(f"  {param}: {value!r}")

        joins = indent("\n".join(joins), " " * 2)  # type: ignore

        cols = indent("\n".join(cols), " " * 2)  # type: ignore

        return template.format(name=self.name, cols=cols, joins=joins)

    def _ipython_key_completions_(self):
        return self.columns

    @property
    def children(self):
        return set([self]) ^ set(self.to_list())

    @property
    def name(self) -> str:
        return self._name

    def join(
        self,
        right,
        on: OnType = None,
        time_stamps: TimeStampsType = None,
        relationship: str = many_to_many,
        memory: Optional[float] = None,
        horizon: Optional[float] = None,
        lagged_targets: bool = False,
        upper_time_stamp: Optional[str] = None,
    ):
        """
        Joins another to placeholder to this placeholder.

        Args:
            right (:class:`~getml.data.Placeholder`):
                The placeholder you would like to join.

            on (None, string, Tuple[str, str] or List[Union[str, Tuple[str, str]]]):
                The join keys to use. If none is passed, then everything
                will be joined to everything else.

            time_stamps (string or Tuple[str, str]):
                The time stamps used to limit the join.

            relationship (str):
                The relationship between the two tables. Must be from
                :mod:`~getml.data.relationship`.

            memory (float):
                The difference between the time stamps until data is 'forgotten'.
                Limiting your joins using memory can significantly speed up
                training time. Also refer to :mod:`~getml.data.time`.

            horizon (float):
                The prediction horizon to apply to this join.
                Also refer to :mod:`~getml.data.time`.

            lagged_targets (bool):
                Whether you want to allow lagged targets. If this is set to True,
                you must also pass a positive, non-zero *horizon*.

            upper_time_stamp (str):
                Name of a time stamp in *right* that serves as an upper limit
                on the join.
        """

        if not isinstance(right, type(self)):
            msg = (
                "'right' must be a getml.data.Placeholder. "
                + "You can create a placeholder by calling .to_placeholder() "
                + "on DataFrames or Views."
            )
            raise TypeError(msg)

        if self in right.to_list():
            raise ValueError(
                "Cicular references to other placeholders are not allowed."
            )

        if isinstance(on, str):
            on = (on, on)

        if isinstance(time_stamps, str):
            time_stamps = (time_stamps, time_stamps)

        keys = (
            list(zip(*on))
            if isinstance(on, list) and all(isinstance(key, tuple) for key in on)
            else on
        )

        for i, ph in enumerate([self, right]):
            if ph.roles.join_key and keys:
                not_a_join_key = _check_join_key(keys[i], ph.roles.join_key)  # type: ignore
                if not_a_join_key:
                    raise ValueError(f"Not a join key: {not_a_join_key}.")

            if ph.roles.time_stamp and time_stamps:
                if time_stamps[i] not in ph.roles.time_stamp:
                    raise ValueError(f"Not a time stamp: {time_stamps[i]}.")

        if lagged_targets and horizon in (0.0, None):
            raise ValueError(
                "If you allow lagged targets, then you must also set a "
                + "horizon > 0.0. This is to avoid 'easter eggs'."
            )

        if horizon not in (0.0, None) and time_stamps is None:
            raise ValueError(
                "Setting 'horizon' (i.e. a relative look-back window) "
                + "requires a 'time_stamp'."
            )

        if memory not in (0.0, None) and time_stamps is None:
            raise ValueError(
                "Setting 'memory' (i.e. a relative look-back window) "
                + "requires a 'time_stamp'."
            )

        join = Join(
            right=right,
            on=on,
            time_stamps=time_stamps,
            relationship=relationship,
            memory=memory,
            horizon=horizon,
            lagged_targets=lagged_targets,
            upper_time_stamp=upper_time_stamp,
        )

        if any(join == existing for existing in self.joins):
            raise ValueError(
                "A join with the following set of parameters already exists on "
                f"the placeholder {self.name!r}:"
                f"\n\n{join}\n\n"
                "Redundant joins are not allowed."
            )

        self.joins.append(join)
        right.parent = self  # type: ignore

    @property
    def population(self):
        if self.parent is None:
            return self
        return self.parent.population

    @property
    def roles(self):
        return self._roles

    @roles.setter
    def roles(self, roles):
        if not isinstance(roles, (Roles, dict)):
            raise TypeError("'roles' must be a dict or getml.data.Roles")
        if isinstance(roles, dict):
            self._roles = Roles(**roles)
        else:
            self._roles = roles

    def to_list(self):
        """
        Returns a list of this placeholder and all of its descendants.
        """
        return [self] + [ph for join in self.joins for ph in join.right.to_list()]

    def to_dict(self):
        """
        Expresses this placeholder and all of its descendants as a dictionary.
        """
        phs = {}
        for ph in self.to_list():
            key = ph.name
            if ph.children:
                i = 2
                while key in phs:
                    key = f"{ph.name}{i}"
                    i += 1
            phs[key] = ph
        return phs

    @property
    def columns(self):
        return self.roles.columns


# ------------------------------------------------------------------------------


@dataclass
class Join:
    right: Placeholder
    on: OnType = None
    time_stamps: TimeStampsType = None
    upper_time_stamp: Optional[str] = ""
    relationship: Optional[str] = many_to_many
    memory: Optional[float] = None
    horizon: Optional[float] = None
    lagged_targets: Optional[bool] = None

    def __eq__(self, other):
        return vars(self) == vars(other)

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except TypeError:
            raise KeyError(key)

    def __iter__(self):
        yield from vars(self)

    def __len__(self):
        return len(fields(self))

    def __repr__(self):
        sig = _SignatureFormatter(self)
        sig.data["right"] = self.right.name

        return sig._format()
