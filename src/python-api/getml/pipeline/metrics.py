# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Signifies different scoring methods.
"""

from typing import List

# --------------------------------------------------------------------

_all_metrics: List[str] = [
    "auc",
    "accuracy",
    "cross_entropy",
    "mae",
    "rmse",
    "rsquared",
]

# --------------------------------------------------------------------
# Scores for classification problems.

auc = _all_metrics[0]
"""Area under the curve - refers to the area under the receiver
operating characteristic (ROC) curve.

Used for classification problems.

When handling a classification problem, the ROC curve maps the
relationship between two conflicting goals:

On the hand, we want a high *true positive rate*. The true positive
rate, sometimes referred to as *recall*, measures the share of
true positive predictions over all positives:

$$
TPR = \\frac{number \\; of \\; true \\; positives}{number \\; of \\; all \\; positives}
$$

In other words, we want our classification algorithm to "catch" as
many positives as possible.

On the other hand, we also want a low *false positive rate* (FPR). The
false positive rate measures the share of false positives over all
negatives.

$$
FPR = \\frac{number \\; of \\; false \\; positives}{number \\; of \\; all \\; negatives}
$$

In other words, we want as few "false alarms" as possible.

However, unless we have a perfect classifier, these two goals
conflict with each other.

The ROC curve maps the TPR against the FPR. We now measure the area
under said curve (AUC). A higher AUC implies that the trade-off between
TPR and FPR is more beneficial. A perfect model would have an AUC of
1. An AUC of 0.5 implies that the model has no predictive value.

"""

accuracy = _all_metrics[1]
"""Accuracy - measures the share of accurate predictions as of total
samples in the testing set.

Used for classification problems.

$$
accuracy = \\frac{number \\; of \\; correct \\; predictions}{number \\; of \\; all \\; predictions}
$$

The number of correct predictions depends on the threshold used:
For instance, we could interpret all predictions for which the probability
is greater than 0.5 as a positive and all others as a negative.
But we do not have to use a threshold of 0.5 - we might as well
use any other threshold. Which threshold we choose will impact
the calculated accuracy.

When calculating the accuracy, the value returned is the
accuracy returned by the *best threshold*.

Even though accuracy is the most intuitive way to measure a classification
algorithm, it can also be very misleading when the samples are very skewed.
For instance, if only 2% of the samples are positive, a predictor that
always predicts negative outcomes will have an accuracy of 98%. This sounds
very good to the layman, but the predictor in this example
actually has no predictive value.
"""

cross_entropy = _all_metrics[2]
"""Cross entropy, also referred to as log-loss, is a measure of the likelihood
of the classification model.

Used for classification problems.

Mathematically speaking, cross-entropy for a binary classification problem
is defined as follows:

$$
cross \\; entropy = - \\frac{1}{N} \\sum_{i}^{N} (y_i \\log p_i + (1 - y_i) \\log(1 - p_i),
$$

where $p_i$ is the probability of a positive outcome as predicted
by the classification algorithm and $y_i$ is the target value,
which is 1 for a positive outcome and 0 otherwise.

There are several ways to justify the use of cross entropy to evaluate
classification algorithms. But the most intuitive way is to think of
it as a measure of *likelihood*. When we have a classification algorithm
that gives us probabilities, we would like to know how likely it is
that we observe a particular state of the world given the probabilities.

We can calculate this likelihood as follows:

$$
likelihood = \\prod_{i}^{N} (p_i^{y_i} * (1 - p_i)^{1 - y_i}).
$$

(Recall that $y_i$ can only be 0 or 1.)

If we take the logarithm of the likelihood as defined above, divide by
$N$ and then multiply by `-1` (because we want lower to mean
better and 0 to mean perfect), the outcome will be cross entropy.

"""

# --------------------------------------------------------------------
# Scores for regression problems.

mae = _all_metrics[3]
"""Mean Absolute Error - measure of distance between two
numerical targets.

Used for regression problems.

$$
MAE = \\frac{\\sum_{i=1}^n | \\mathbf{y}_i - \\mathbf{\\hat{y}}_i |}{n},
$$

where $\\mathbf{y}_i$ and $\\mathbf{\\hat{y}}_i$ are the target
values or prediction respectively for a particular data sample
$i$ (both multidimensional in case of using multiple targets)
while $n$ is the number of samples we consider during the
scoring.
"""

rmse = _all_metrics[4]
"""Root Mean Squared Error - measure of distance between two
numerical targets.

Used for regression problems.

$$
RMSE = \\sqrt{\\frac{\\sum_{i=1}^n ( \\mathbf{y}_i - \\mathbf{\\hat{y}}_i )^2}{n}},
$$

where $\\mathbf{y}_i$ and $\\mathbf{\\hat{y}}_i$ are the target
values or prediction respectively for a particular data sample
$i$ (both multidimensional in case of using multiple targets)
while $n$ is the number of samples we consider during the
scoring.
"""


rsquared = _all_metrics[5]
"""$R^{2}$ - squared correlation coefficient between predictions and targets.

Used for regression problems.

$R^{2}$ is defined as follows:

$$
R^{2} = \\frac{(\\sum_{i=1}^n ( y_i - \\bar{y_i} ) *  ( \\hat{y_i} - \\bar{\\hat{y_i}} ))^2 }{\\sum_{i=1}^n ( y_i - \\bar{y_i} )^2 \\sum_{i=1}^n ( \\hat{y_i} - \\bar{\\hat{y_i}} )^2 },
$$

where $y_i$ are the true values, $\\hat{y_i}$ are
the predictions and $\\bar{...}$ denotes the mean operator.

An $R^{2}$ of 1 implies perfect correlation between the predictions
and the targets and an $R^{2}$ of 0 implies no correlation
at all.
"""


# --------------------------------------------------------------------

_classification_metrics: List[str] = [auc, accuracy, cross_entropy]

# --------------------------------------------------------------------

_minimizing_metrics: List[str] = [cross_entropy, mae, rmse]

# --------------------------------------------------------------------
