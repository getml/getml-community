# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Loss functions used by the feature learning algorithms.

The getML Python API contains two different loss
functions. We recommend using
[`SQUARELOSS`][getml.feature_learning.loss_functions.SQUARELOSS] for regression problems and
[`CROSSENTROPYLOSS`][getml.feature_learning.loss_functions.CROSSENTROPYLOSS] for classification
problems.

Please note that these loss functions will only be used by the feature
learning algorithms and not by the [`predictors`][getml.predictors].

"""

from typing import Final, Literal

CrossEntropyLossType = Literal["CrossEntropyLoss"]
"""Type of the cross entropy loss function."""
CROSSENTROPYLOSS: Final[CrossEntropyLossType] = "CrossEntropyLoss"
"""
The cross entropy between two probability distributions
$p(x)$ and $q(x)$ is a combination of the information
contained in $p(x)$ and the additional information stored in
$q(x)$ with respect to $p(x)$. In technical terms: it
is the entropy of $p(x)$ plus the Kullback-Leibler
divergence - a distance in probability space - from $q(x)$
to $p(x)$.

$$
H(p,q) = H(p) + D_{KL}(p||q)
$$

For discrete probability distributions the cross entropy loss can
be calculated by

$$
H(p,q) = - \\sum_{x \\in X} p(x) \\log q(x)
$$

and for continuous probability distributions by

$$
H(p,q) = - \\int_{X} p(x) \\log q(x) dx
$$

with $X$ being the support of the samples and $p(x)$
and $q(x)$ being two discrete or continuous probability
distributions over $X$.

Note:
    Recommended loss function for classification problems.
"""

SquareLossType = Literal["SquareLoss"]
"""Type of the square loss function."""
SQUARELOSS: Final[SquareLossType] = "SquareLoss"
"""
The Square loss (aka mean squared error (MSE)) measures the loss by calculating the average of all squared
deviations of the predictions $\\hat{y}$ from the observed
(given) outcomes $y$. Depending on the context this measure
is also known as mean squared error (MSE) or mean squared
deviation (MSD).

$$
L(y,\\hat{y}) = \\frac{1}{n} \\sum_{i=1}^{n} (y_i -\\hat{y}_i)^2 
$$

with $n$ being the number of samples, $y$ the observed
outcome, and $\\hat{y}$ the estimate.

Note:
    Recommended loss function for regression problems.
"""

_all_loss_functions = [CROSSENTROPYLOSS, SQUARELOSS]

_classification_loss = [CROSSENTROPYLOSS]

# For backward compatibility
CrossEntropyLoss = CROSSENTROPYLOSS
SquareLoss = SQUARELOSS
