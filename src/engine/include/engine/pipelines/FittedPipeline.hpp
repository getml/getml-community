#ifndef ENGINE_PIPELINES_FITTEDPIPELINE_HPP_
#define ENGINE_PIPELINES_FITTEDPIPELINE_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "fct/fct.hpp"
#include "predictors/predictors.hpp"

// ----------------------------------------------------------------------------

#include "engine/featurelearners/featurelearners.hpp"
#include "engine/preprocessors/preprocessors.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct FittedPipeline {
  /// Returns the names of the autofeatures.
  std::vector<std::string> autofeature_names() const;

  /// Returns the names of the autofeatures, the numerical manual features and
  /// the categorical manual features.
  std::tuple<std::vector<std::string>, std::vector<std::string>,
             std::vector<std::string>>
  feature_names() const;

  /// Whether this is a classification pipeline.
  bool is_classification() const;

  /// Calculates the number of automated and manual features used.
  size_t num_features() const {
    const auto [autofeatures, manual1, manual2] = feature_names();
    return autofeatures.size() + manual1.size() + manual2.size();
  }

  /// Calculates the number of predictors per set.
  size_t num_predictors_per_set() const {
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

  /// The fingerprints of the data frames used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> df_fingerprints_;

  /// The feature learners used in this pipeline.
  const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
      feature_learners_;

  /// The feature selectors used in this pipeline (every target has its own
  /// set of feature selectors).
  const std::vector<std::vector<fct::Ref<const predictors::Predictor>>>
      feature_selectors_;

  /// Pimpl for the feature selectors.
  const fct::Ref<const predictors::PredictorImpl> feature_selector_impl_;

  /// The fingerprints of the feature learners used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> fl_fingerprints_;

  /// The fingerprints of the feature selectors used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> fs_fingerprints_;

  /// The schema of the peripheral tables as they are inserted into the
  /// feature learners.
  const fct::Ref<const std::vector<helpers::Schema>>
      modified_peripheral_schema_;

  /// The schema of the population as it is inserted into the feature
  /// learners.
  const fct::Ref<const helpers::Schema> modified_population_schema_;

  /// The schema of the peripheral tables as they are originally passed.
  const fct::Ref<const std::vector<helpers::Schema>> peripheral_schema_;

  /// The schema of the population as originally passed.
  const fct::Ref<const helpers::Schema> population_schema_;

  /// The predictors used in this pipeline (every target has its own set of
  /// predictors).
  const std::vector<std::vector<fct::Ref<const predictors::Predictor>>>
      predictors_;

  /// Pimpl for the predictors.
  const fct::Ref<const predictors::PredictorImpl> predictor_impl_;

  /// The preprocessors used in this pipeline.
  const std::vector<fct::Ref<const preprocessors::Preprocessor>> preprocessors_;

  /// The fingerprints of the preprocessor used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> preprocessor_fingerprints_;

  /// The names of the target columns
  const std::vector<std::string> targets_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITTEDPIPELINE_HPP_

