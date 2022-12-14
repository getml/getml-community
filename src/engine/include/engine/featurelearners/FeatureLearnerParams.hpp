// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/Int.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace featurelearners {

struct FeatureLearnerParams {
  /// The command used to initialize the feature learner.
  const Poco::JSON::Object cmd_;

  /// The dependencies used for the fingerprint.
  const std::vector<Poco::JSON::Object::Ptr> dependencies_;

  /// The names of the peripheral tables.
  const std::shared_ptr<const std::vector<std::string>> peripheral_;

  /// The schema of the peripheral tables.
  const std::shared_ptr<const std::vector<helpers::Schema>> peripheral_schema_;

  /// The placeholder representing the data model.
  const std::shared_ptr<const helpers::Placeholder> placeholder_;

  /// The schema of the population.
  const std::shared_ptr<const helpers::Schema> population_schema_;

  /// Indicates which target to use.
  const Int target_num_;
};

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

