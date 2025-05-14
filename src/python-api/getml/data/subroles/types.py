# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import Literal

ExcludeCategoryTrimmer = Literal["exclude category trimmer"]
ExcludeFastProp = Literal["exclude fastprop"]
ExcludeFeatureLearners = Literal["exclude feature learners"]
ExcludeImputation = Literal["exclude imputation"]
ExcludeMapping = Literal["exclude mapping"]
ExcludeMultirel = Literal["exclude multirel"]
ExcludePredictors = Literal["exclude predictors"]
ExcludePreprocessors = Literal["exclude preprocessors"]
ExcludeRelboost = Literal["exclude relboost"]
ExcludeRelMT = Literal["exclude relmt"]
ExcludeSeasonal = Literal["exclude seasonal"]
ExcludeTextFieldSplitter = Literal["exclude text field splitter"]

ExcludeLike = Literal[
    ExcludeCategoryTrimmer,
    ExcludeFastProp,
    ExcludeFeatureLearners,
    ExcludeImputation,
    ExcludeMapping,
    ExcludeMultirel,
    ExcludePredictors,
    ExcludePreprocessors,
    ExcludeRelboost,
    ExcludeRelMT,
    ExcludeSeasonal,
    ExcludeTextFieldSplitter,
]

IncludeEmail = Literal["include email"]
IncludeSubstring = Literal["include substring"]

IncludeLike = Literal[IncludeEmail, IncludeSubstring]


OnlyEmail = Literal["only email"]
OnlySubstring = Literal["only substring"]

OnlyLike = Literal[OnlyEmail, OnlySubstring]

Subrole = Literal[ExcludeLike, IncludeLike, OnlyLike]
