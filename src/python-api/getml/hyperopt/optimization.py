# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of optimization algorithms to be used by the
hyperparameter optimizations.
"""

bfgs = "bfgs"
"""
Broyden-Fletcher-Goldbarb-Shanno optimization algorithm.

The BFGS algorithm is a quasi-Newton method that
requires the function to be differentiable.
"""

nelder_mead = "nelderMead"
"""
Nelder-Mead optimization algorithm.

Nelder-Mead is a direct search method that
does not require functions to be differentiable.
"""

_all_optimizations = [bfgs, nelder_mead]
