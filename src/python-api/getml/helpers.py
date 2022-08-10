# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper classes that are relevant
for many submodules.
"""

# --------------------------------------------------------------------


def _check_parameter_bounds(parameter, parameter_name, bounds):
    """Checks whether a particular parameter does lie within the provided
    `bounds`.

    Args:
        parameter (numeric): Particular value of an instance variable.
        key_name (string): Name of the parameter used for an
            expressive Exception
        bounds (list[numeric]): Numerical list of length 2
            specifying the lower and upper bound (in that order)
            of `parameter`.
    """
    if parameter < bounds[0] or parameter > bounds[1]:
        raise ValueError(
            "'"
            + parameter_name
            + "' is only defined for range ["
            + str(bounds[0])
            + ", "
            + str(bounds[1])
            + "]"
        )
