# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Loss functions used by the feature learning algorithms.

The getML Python API contains two different loss
functions. We recommend using
:class:`~getml.feature_learning.loss_functions.SquareLoss` for regression problems and
:class:`~getml.feature_learning.loss_functions.CrossEntropyLoss` for classification
problems.

Please note that these loss functions will only be used by the feature
learning algorithms and not by the :mod:`~getml.predictors`.
"""

CrossEntropyLoss = "CrossEntropyLoss"
"""
Cross entropy loss

The cross entropy between two probability distributions
:math:`p(x)` and :math:`q(x)` is a combination of the information
contained in :math:`p(x)` and the additional information stored in
:math:`q(x)` with respect to :math:`p(x)`. In technical terms: it
is the entropy of :math:`p(x)` plus the Kullback-Leibler
divergence - a distance in probability space - from :math:`q(x)`
to :math:`p(x)`.

.. math::

    H(p,q) = H(p) + D_{KL}(p||q)

For discrete probability distributions the cross entropy loss can
be calculated by

.. math::

    H(p,q) = - \\sum_{x \\in X} p(x) \\log q(x)

and for continuous probability distributions by

.. math::

    H(p,q) = - \\int_{X} p(x) \\log q(x) dx

with :math:`X` being the support of the samples and :math:`p(x)`
and :math:`q(x)` being two discrete or continuous probability
distributions over :math:`X`.

Note:
    Recommended loss function for classification problems.
"""

SquareLoss = "SquareLoss"
"""
Square loss (aka mean squared error (MSE))

Measures the loss by calculating the average of all squared
deviations of the predictions :math:`\\hat{y}` from the observed
(given) outcomes :math:`y`. Depending on the context this measure
is also known as mean squared error (MSE) or mean squared
deviation (MSD).  deviation (MSD).

.. math::

    L(y,\\hat{y}) = \\frac{1}{n} \\sum_{i=1}^{n} (y_i -\\hat{y}_i)^2

with :math:`n` being the number of samples, :math:`y` the observed
outcome, and :math:`\\hat{y}` the estimate.

Note:
    Recommended loss function for regression problems.
"""

_all_loss_functions = [CrossEntropyLoss, SquareLoss]

_classification_loss = [CrossEntropyLoss]
