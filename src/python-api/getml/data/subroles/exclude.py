# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import Final

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

??? example
    ```python
    # The Relboost feature learning algorithm will
    # ignore this column.
    my_data_frame.set_subroles(
        "my_column", getml.data.subroles.exclude.relboost)
    ```
"""
category_trimmer: Final[ExcludeCategoryTrimmer] = "exclude category trimmer"
"""
The [`CategoryTrimmer`][getml.preprocessors.CategoryTrimmer] preprocessor
will ignore this column.
"""

fastprop: Final[ExcludeFastProp] = "exclude fastprop"
"""
[`FastProp`][getml.feature_learning.FastProp] will ignore
this column.
"""

feature_learners: Final[ExcludeFeatureLearners] = "exclude feature learners"
"""
All feature learners ([`feature_learning`][getml.feature_learning])
will ignore this column.
"""

imputation: Final[ExcludeImputation] = "exclude imputation"
"""
The [`Imputation`][getml.preprocessors.Imputation] preprocessor
will ignore this column.
"""

mapping: Final[ExcludeMapping] = "exclude mapping"
"""
The [`Mapping`][getml.preprocessors.Mapping] preprocessor
will ignore this column.
"""

multirel: Final[ExcludeMultirel] = "exclude multirel"
"""
[`Multirel`][getml.feature_learning.Multirel] will ignore
this column.
"""

predictors: Final[ExcludePredictors] = "exclude predictors"
"""
All [`predictors`][getml.predictors] will ignore this column.
"""

preprocessors: Final[ExcludePreprocessors] = "exclude preprocessors"
"""
All [`preprocessors`][getml.preprocessors] will ignore this column.
"""

relboost: Final[ExcludeRelboost] = "exclude relboost"
"""
[`Relboost`][getml.feature_learning.Relboost] will ignore
this column.
"""

relmt: Final[ExcludeRelMT] = "exclude relmt"
"""
[`RelMT`][getml.feature_learning.RelMT] will ignore
this column.
"""

seasonal: Final[ExcludeSeasonal] = "exclude seasonal"
"""
The [`Seasonal`][getml.preprocessors.Seasonal] preprocessor
will ignore this column.
"""

text_field_splitter: Final[ExcludeTextFieldSplitter] = "exclude text field splitter"
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
