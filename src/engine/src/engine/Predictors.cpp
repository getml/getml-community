#include "engine/pipelines/Predictors.hpp"

// ----------------------------------------------------------------------------

#include "transpilation/transpilation.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

// ----------------------------------------------------------------------------

std::vector<std::string> Predictors::autofeature_names() const {
  std::vector<std::string> autofeatures;

  for (size_t i = 0; i < impl_->autofeatures().size(); ++i) {
    const auto& index = impl_->autofeatures().at(i);

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
Predictors::feature_names() const {
  const auto autofeatures = autofeature_names();

  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::HumanReadableSQLGenerator()
        .make_staging_table_colname(_colname);
  };

  const auto numerical = helpers::Macros::modify_colnames(
      impl_->numerical_colnames(), make_staging_table_colname);

  const auto categorical = helpers::Macros::modify_colnames(
      impl_->categorical_colnames(), make_staging_table_colname);

  return std::make_tuple(autofeatures, numerical, categorical);
}

// ----------------------------------------------------------------------------

size_t Predictors::num_features() const {
  const auto [autofeatures, manual1, manual2] = feature_names();
  return autofeatures.size() + manual1.size() + manual2.size();
}

// ----------------------------------------------------------------------------

size_t Predictors::num_predictors_per_set() const {
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
