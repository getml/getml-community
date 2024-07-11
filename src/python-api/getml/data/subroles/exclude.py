# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import Final, get_args

from getml.data.subroles.types import (
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
)

"""
Columns marked with a subrole in this submodule will not be used
for the specified purpose.

Example:
    ```python
    # The Relboost feature learning algorithm will
    # ignore this column.
    my_data_frame.set_subroles(
        "my_column", getml.data.subroles.exclude.relboost)
    ```
"""
category_trimmer: Final[ExcludeCategoryTrimmer] = get_args(ExcludeCategoryTrimmer)[0]
"""
The [`CategoryTrimmer`][getml.preprocessors.CategoryTrimmer] preprocessor
will ignore this column.
"""

fastprop: Final[ExcludeFastProp] = get_args(ExcludeFastProp)[0]
"""
[`FastProp`][getml.feature_learning.FastProp] will ignore
this column.
"""

feature_learners: Final[ExcludeFeatureLearners] = get_args(ExcludeFeatureLearners)[0]
"""
All feature learners ([`feature_learning`][getml.feature_learning])
will ignore this column.
"""

imputation: Final[ExcludeImputation] = get_args(ExcludeImputation)[0]
"""
The [`Imputation`][getml.preprocessors.Imputation] preprocessor
will ignore this column.
"""

mapping: Final[ExcludeMapping] = get_args(ExcludeMapping)[0]
"""
The [`Mapping`][getml.preprocessors.Mapping] preprocessor
will ignore this column.
"""

multirel: Final[ExcludeMultirel] = get_args(ExcludeMultirel)[0]
"""
[`Multirel`][getml.feature_learning.Multirel] will ignore
this column.
"""

predictors: Final[ExcludePredictors] = get_args(ExcludePredictors)[0]
"""
All [`predictors`][getml.predictors] will ignore this column.
"""

preprocessors: Final[ExcludePreprocessors] = get_args(ExcludePreprocessors)[0]
"""
All [`preprocessors`][getml.preprocessors] will ignore this column.
"""

relboost: Final[ExcludeRelboost] = get_args(ExcludeRelboost)[0]
"""
[`Relboost`][getml.feature_learning.Relboost] will ignore
this column.
"""

relmt: Final[ExcludeRelMT] = get_args(ExcludeRelMT)[0]
"""
[`RelMT`][getml.feature_learning.RelMT] will ignore
this column.
"""

seasonal: Final[ExcludeSeasonal] = get_args(ExcludeSeasonal)[0]
"""
The [`Seasonal`][getml.preprocessors.Seasonal] preprocessor
will ignore this column.
"""

text_field_splitter: Final[ExcludeTextFieldSplitter] = get_args(
    ExcludeTextFieldSplitter
)[0]
"""
The [`TextFieldSplitter`][getml.preprocessors.TextFieldSplitter] will ignore this column.
"""

__all__ = (
    "category_trimmer",
    "fastprop",
    "feature_learners",
    "imputation",
    "mapping",
    "multirel",
    "predictors",
    "preprocessors",
    "relboost",
    "relmt",
    "seasonal",
    "text_field_splitter",
)
