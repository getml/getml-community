// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_SUBROLE_HPP_
#define HELPERS_SUBROLE_HPP_

// ----------------------------------------------------------------------------

namespace helpers {
// ----------------------------------------------------------------------------

enum class Subrole {
  comparison_only,
  email,
  email_only,
  exclude_category_trimmer,
  exclude_fastprop,
  exclude_feature_learners,
  exclude_imputation,
  exclude_mapping,
  exclude_multirel,
  exclude_predictors,
  exclude_preprocessors,
  exclude_relboost,
  exclude_relmt,
  exclude_seasonal,
  exclude_text_field_splitter,
  substring,
  substring_only
};

// ----------------------------------------------------------------------------

}  // namespace helpers

// ----------------------------------------------------------------------------

#endif  // HELPERS_SUBROLE_HPP_
