#include "engine/pipelines/FittedPipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

// ----------------------------------------------------------------------------

std::vector<std::string> FittedPipeline::autofeature_names() const {
  std::vector<std::string> autofeatures;

  for (size_t i = 0; i < predictor_impl_->autofeatures().size(); ++i) {
    const auto& index = predictor_impl_->autofeatures().at(i);

    for (const auto ix : index) {
      autofeatures.push_back("feature_" + std::to_string(i + 1) + "_" +
                             std::to_string(ix + 1));
    }
  }

  return autofeatures;
}

// ----------------------------------------------------------------------------

std::tuple<std::vector<std::string>, std::vector<std::string>,
           std::vector<std::string>>
FittedPipeline::feature_names() const {
  assert_true(feature_learners_.size() ==
              predictor_impl_->autofeatures().size());

  const auto autofeatures = autofeature_names();

  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::SQLite3Generator().make_staging_table_colname(
        _colname);
  };

  const auto numerical = helpers::Macros::modify_colnames(
      predictor_impl_->numerical_colnames(), make_staging_table_colname);

  const auto categorical = helpers::Macros::modify_colnames(
      predictor_impl_->categorical_colnames(), make_staging_table_colname);

  return std::make_tuple(autofeatures, numerical, categorical);
}

// ----------------------------------------------------------------------------

bool FittedPipeline::is_classification() const {
  const auto is_cl = [](const auto& elem) { return elem->is_classification(); };

  bool all_classifiers =
      std::all_of(feature_learners_.begin(), feature_learners_.end(), is_cl);

  for (const auto& fs : feature_selectors_) {
    all_classifiers =
        all_classifiers && std::all_of(fs.begin(), fs.end(), is_cl);
  }

  for (const auto& p : predictors_) {
    all_classifiers = all_classifiers && std::all_of(p.begin(), p.end(), is_cl);
  }

  bool all_regressors =
      std::none_of(feature_learners_.begin(), feature_learners_.end(), is_cl);

  for (const auto& fs : feature_selectors_) {
    all_regressors =
        all_regressors && std::none_of(fs.begin(), fs.end(), is_cl);
  }

  for (const auto& p : predictors_) {
    all_regressors = all_regressors && std::none_of(p.begin(), p.end(), is_cl);
  }

  if (!all_classifiers && !all_regressors) {
    throw std::runtime_error(
        "You are mixing classification and regression algorithms. "
        "Please make sure that all of your feature learners, feature "
        "selectors and predictors are either all regression algorithms "
        "or all classifications algorithms.");
  }

  if (all_classifiers == all_regressors) {
    throw std::runtime_error(
        "The pipelines needs at least one feature learner, feature "
        "selector or predictor.");
  }

  return all_classifiers;
}
// ----------------------------------------------------------------------------

size_t FittedPipeline::num_features() const {
  const auto [autofeatures, manual1, manual2] = feature_names();
  return autofeatures.size() + manual1.size() + manual2.size();
}

// ----------------------------------------------------------------------------

size_t FittedPipeline::num_predictors_per_set() const {
  if (predictors_.size() == 0) {
    return 0;
  }
  const auto n_expected = predictors_.at(0).size();
#ifndef NDEBUG
  for (const auto& pset : predictors_) {
    assert_true(pset.size() == n_expected);
  }
#endif  // NDEBUG
  return n_expected;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
