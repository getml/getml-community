# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of kernel functions to be used by the
hyperparameter optimizations.
"""

exp = "exp"
"""
An exponential kernel yielding non-differentiable
sample paths.
"""

gauss = "gauss"
"""
A Gaussian kernel yielding analytic
(infinitely--differentiable) sample paths.
"""

matern32 = "matern32"
"""
A Matérn 3/2 kernel yielding once-differentiable
sample paths.
"""

matern52 = "matern52"
"""
A Matérn 5/2 kernel yielding twice-differentiable
sample paths.
"""


_all_kernels = [matern32, matern52, exp, gauss]
