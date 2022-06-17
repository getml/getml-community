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
