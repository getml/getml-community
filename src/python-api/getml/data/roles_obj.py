# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Dataclass for handling the roles.
"""

from dataclasses import dataclass, field, fields
from inspect import cleandoc
from typing import Dict, List


@dataclass
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



    Example:
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

    categorical: List[str] = field(default_factory=list)
    join_key: List[str] = field(default_factory=list)
    numerical: List[str] = field(default_factory=list)
    target: List[str] = field(default_factory=list)
    text: List[str] = field(default_factory=list)
    time_stamp: List[str] = field(default_factory=list)
    unused_float: List[str] = field(default_factory=list)
    unused_string: List[str] = field(default_factory=list)

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except TypeError:
            raise KeyError(key)

    def __iter__(self):
        yield from (field_.name for field_ in fields(self))

    def __len__(self):
        return len(fields(self))

    def __repr__(self):
        template = cleandoc(
            """
            {role}:
            - {cols}
            """
        )

        blocks = []

        for role in self:
            if self[role]:
                cols = "\n- ".join(self[role])
                blocks.append(template.format(role=role, cols=cols))

        return "\n\n".join(blocks)

    @property
    def columns(self) -> List[str]:
        """
        The name of all columns contained in the roles object.

        Returns:
            The names of all columns.
        """
        return [r for role in self for r in self[role]]

    def infer(self, colname: str) -> str:
        """
        Infers the role of a column.

        Args:
            colname:
                The name of the column to be inferred.

        Returns:
            The role of the column as a string.
        """
        for role in self:
            if colname in self[role]:
                return role
        raise ValueError("Column named '" + colname + "' not found.")

    def to_dict(self) -> Dict[str, List[str]]:
        """
        Expresses the roles object as a dictionary.

        Returns:
            A dictionary where keys are role names and values are lists of column names.
        """
        return {role: self[role] for role in self}

    def to_list(self) -> List[str]:
        """
        Returns a list containing the roles, without the corresponding
        columns names.

        Returns:
            A list where each element is a role name, repeated by the number of columns in that role.
        """
        return [r for role in self for r in [role] * len(self[role])]

    @property
    def unused(self) -> List[str]:
        """
        Names of all unused columns (unused_float + unused_string).

        Returns:
            A list of column names that are categorized as unused, combining both float and string types.
        """
        return self.unused_float + self.unused_string
