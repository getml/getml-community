# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


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

category_trimmer = "exclude category trimmer"
"""
The [`CategoryTrimmer`][getml.preprocessors.CategoryTrimmer] preprocessor
will ignore this column.
"""

fastprop = "exclude fastprop"
"""
[`FastProp`][getml.feature_learning.FastProp] will ignore
this column.
"""


feature_learners = "exclude feature learners"
"""
All feature learners ([`feature_learning`][getml.feature_learning])
will ignore this column.
"""

imputation = "exclude imputation"
"""
The [`Imputation`][getml.preprocessors.Imputation] preprocessor
will ignore this column.
"""

mapping = "exclude mapping"
"""
The [`Mapping`][getml.preprocessors.Mapping] preprocessor
will ignore this column.
"""

multirel = "exclude multirel"
"""
[`Multirel`][getml.feature_learning.Multirel] will ignore
this column.
"""

predictors = "exclude predictors"
"""
All [`predictors`][getml.predictors] will ignore this column.
"""

preprocessors = "exclude preprocessors"
"""
All [`preprocessors`][getml.preprocessors] will ignore this column.
"""

relboost = "exclude relboost"
"""
[`Relboost`][getml.feature_learning.Relboost] will ignore
this column.
"""

relmt = "exclude relmt"
"""
[`RelMT`][getml.feature_learning.RelMT] will ignore
this column.
"""

seasonal = "exclude seasonal"
"""
The [`Seasonal`][getml.preprocessors.Seasonal] preprocessor
will ignore this column.
"""

text_field_splitter = "exclude text field splitter"
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

_all_exclude = [
    category_trimmer,
    fastprop,
    feature_learners,
    imputation,
    mapping,
    multirel,
    predictors,
    preprocessors,
    relboost,
    relmt,
    seasonal,
    text_field_splitter,
]
