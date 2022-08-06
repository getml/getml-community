// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

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

#include "engine/pipelines/Fingerprints.hpp"
#include "engine/pipelines/Predictors.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

struct FittedPipeline {
  /// Returns the names of the autofeatures.
  std::vector<std::string> autofeature_names() const {
    return predictors_.autofeature_names();
  }

  /// Returns the names of the autofeatures, the numerical manual features and
  /// the categorical manual features.
  std::tuple<std::vector<std::string>, std::vector<std::string>,
             std::vector<std::string>>
  feature_names() const {
    return predictors_.feature_names();
  }

  /// Calculates the number of automated and manual features used.
  size_t num_features() const { return predictors_.num_features(); }

  /// Calculates the number of predictors per set.
  size_t num_predictors_per_set() const {
    return predictors_.num_predictors_per_set();
  }

  /// The names of the target columns
  const auto& targets() const { return modified_population_schema_->targets_; }

  /// Whether this is a classification pipeline.
  bool is_classification() const;

  /// The feature learners used in this pipeline.
  const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
      feature_learners_;

  /// The feature selectors used in this pipeline.
  const Predictors feature_selectors_;

  /// The fingerprints used for this pipeline
  const Fingerprints fingerprints_;

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

  /// The predictors used in this pipeline.
  const Predictors predictors_;

  /// The preprocessors used in this pipeline.
  const std::vector<fct::Ref<const preprocessors::Preprocessor>> preprocessors_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITTEDPIPELINE_HPP_

