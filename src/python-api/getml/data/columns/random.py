# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Generates random numbers
"""

import numbers

from .columns import FloatColumnView


def random(seed: int = 5849) -> FloatColumnView:
    """
    Create random column.

    The numbers will be uniformly distributed from 0.0 to 1.0. This can be
    used to randomly split a population table into a training and a test
    set

    Args:
        seed:
            Seed used for the random number generator.

    Returns:
            FloatColumn containing random numbers

    ??? example
        ```python
        population = getml.DataFrame('population')
        population.add(numpy.zeros(100), 'column_01')

        idx = random(seed=42)
        population_train = population[idx > 0.7]
        population_test = population[idx <= 0.7]
        ```
    """

    if not isinstance(seed, numbers.Real):
        raise TypeError("'seed' must be a real number")

    col = FloatColumnView(operator="random", operand1=None, operand2=None)
    col.cmd["seed_"] = seed
    return col
