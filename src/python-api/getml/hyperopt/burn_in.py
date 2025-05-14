# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of burn-in algorithms to be used by the
hyperparameter optimizations.
"""

latin_hypercube = "latinHypercube"
"""
Samples from the hyperparameter space
almost randomly, but ensures that the
different draws are sufficiently different
from each other.
"""

random = "random"
"""
Samples from the hyperparameter space
at random.
"""

_all_burn_ins = [latin_hypercube, random]
