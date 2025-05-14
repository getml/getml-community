# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Dataclass for handling the roles.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field, fields
from inspect import cleandoc
from typing import Dict, Iterable, Iterator, List, Mapping, Tuple, Union, cast

from getml.data.roles import sets as roles_sets
from getml.data.roles.types import Role
from getml.helpers import _is_iterable_not_str_of_type

INVALID_ROLE_ERROR_MESSAGE_TEMPLATE = cleandoc(
    """
    '{{candidate_role}}' is not a proper role. Please provide a dictionary with valid
    roles as keys and lists of column names as values.

    Possible roles are: {valid_roles}
    """
).format(valid_roles=f"{{{set(roles_sets.all_)!r}}}")


@dataclass(frozen=True)
class Roles:
    """
    Roles can be passed to [`DataFrame`][getml.DataFrame] to
    predefine the roles assigned to certain columns.

    Attributes:
        categorical:
            Names of the categorical columns.

        join_key:
            Names of the join key columns.

        numerical:
            Names of the numerical columns.

        target:
            Names of the target columns.

        text:
            Names of the text columns.

        time_stamp:
            Names of the time stamp columns.

        unused_float:
            Names of the unused float columns.

        unused_string:
            Names of the unused string columns.



    ??? example
        ```python
        roles = getml.data.Roles(
            categorical=["col1", "col2"], target=["col3"]
        )

        df_expd = data.DataFrame.from_csv(
            fnames=["file1.csv", "file2.csv"],
            name="MY DATA FRAME",
            sep=';',
            quotechar='"',
            roles=roles
        )
        ```
    """

    categorical: Iterable[str] = field(default_factory=tuple)
    join_key: Iterable[str] = field(default_factory=tuple)
    numerical: Iterable[str] = field(default_factory=tuple)
    target: Iterable[str] = field(default_factory=tuple)
    text: Iterable[str] = field(default_factory=tuple)
    time_stamp: Iterable[str] = field(default_factory=tuple)
    unused_float: Iterable[str] = field(default_factory=tuple)
    unused_string: Iterable[str] = field(default_factory=tuple)

    def __post_init__(self):
        self.validate()

        for field in fields(self):
            if not isinstance(value := getattr(self, field.name), list):
                object.__setattr__(self, field.name, list(value))

    def __getitem__(self, key) -> Tuple[str, ...]:
        try:
            return getattr(self, key)
        except TypeError:
            raise KeyError(key)

    def __iter__(self) -> Iterator[Role]:
        return cast(Iterator[Role], (field_.name for field_ in fields(self)))

    def __len__(self) -> int:
        return len(fields(self))

    def __repr__(self) -> str:
        template = cleandoc(
            """
            {role}:
              - {cols}
            """
        )

        blocks = []

        for role in self:
            if self[role]:
                cols = "\n  - ".join(self[role])
                blocks.append(template.format(role=role, cols=cols))

        return "\n".join(blocks)

    @property
    def columns(self) -> Tuple[str, ...]:
        """
        The name of all columns contained in the roles object.

        Returns:
            The names of all columns.
        """
        return tuple(column for role in self for column in self[role])

    def column(self, colname: str) -> Role:
        """
        Gets the role of a column by its column name.

        Args:
            colname:
                The name of the column.

        Returns:
            The role of the column as a string.
        """
        for role in self:
            if colname in self[role]:
                return role
        raise ValueError("Column named '" + colname + "' not found.")

    @classmethod
    def from_dict(cls, roles_dict: Mapping[Union[Role, str], Iterable[str]]) -> Roles:
        """
        Creates a roles object from a dictionary.

        Args:
            roles_dict:
                A dictionary where keys are role names and values are lists of column names.

        Returns:
            A roles object.
        """
        roles: Dict[Role, List[str]] = {}
        for role in roles_dict:
            if role not in roles_sets.all_:
                raise ValueError(
                    INVALID_ROLE_ERROR_MESSAGE_TEMPLATE.format(candidate_role=role)
                )
            roles[role] = list(roles_dict[role])

        return cls(**roles)

    @classmethod
    def from_mapping(cls, roles_mapping: Mapping[str, Role]) -> Roles:
        """
        Creates a roles object from a mapping of column names to roles.

        Args:
            roles_mapping:
                A dictionary where keys are column names and values are role names.

        Returns:
            A roles object.
        """
        roles: Dict[Role, List[str]] = {
            cast(Role, field.name): [] for field in fields(cls)
        }
        for column, role in roles_mapping.items():
            roles[role].append(column)
        return cls.from_dict(roles)

    def infer(self, colname: str) -> Role:
        """
        Infers the role of a column by its name.

        Args:
            colname:
                The name of the column to be inferred.

        Returns:
            The role of the column as a string.
        """
        warnings.warn(
            "The 'infer' method is deprecated and will be removed in a future "
            "release. To get a specific column's role, use 'column' instead.",
            DeprecationWarning,
        )
        return self.column(colname)

    def to_dict(self) -> Dict[Role, List[str]]:
        """
        Expresses the roles object as a dictionary.

        Returns:
            A dictionary where keys are role names and values are lists of column names.
        """
        return {role: list(self[role]) for role in self}

    def to_list(self) -> List[Role]:
        """
        Returns a list containing the roles, without the corresponding
        columns names.

        Returns:
            A list where each element is a role name, repeated by the number of columns in that role.
        """
        return [role for role in self for _ in self[role]]

    def to_mapping(self) -> Dict[str, Role]:
        """
        Maps column names to their roles.

        Returns:
            A dictionary where keys are column names and values are role names.
        """
        return {column: role for role in self for column in self[role]}

    @property
    def unused(self) -> List[str]:
        """
        Names of all unused columns (unused_float + unused_string).

        Returns:
            A list of column names that are categorized as unused, combining both float and string types.
        """
        return [*self.unused_float, *self.unused_string]

    def update(self, other: Roles) -> Roles:
        """
        Merges the roles of two roles objects.

        Args:
            other:
                The roles object to be merged with the current one.

        Returns:
            A new roles object containing the merged roles.
        """

        current = self.to_mapping()
        new = other.to_mapping()

        updated: dict[str, Role] = {**current, **new}

        return Roles.from_mapping(updated)

    def validate(self) -> None:
        """
        Checks if the roles are consistent.

        Raises:
            ValueError:
                If the roles are inconsistent.
        """

        seen = dict()

        for role in self:
            if not _is_iterable_not_str_of_type(self[role], type=str):
                raise TypeError(
                    f"Argument for '{role}' must be an iterable of column names "
                    "(strings): Iterable[str]."
                )

            for column in self[role]:
                if (already_defined_role := seen.get(column)) is not None:
                    raise ValueError(
                        f"Column names must be unique across all roles. Found "
                        f"duplicate roles set for column '{column}': '{role}' and "
                        f"'{already_defined_role}'."
                    )
                else:
                    seen.update({column: role})
