// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_FINGERPRINTS_HPP_
#define ENGINE_PIPELINES_FINGERPRINTS_HPP_

#include <Poco/JSON/Object.h>

#include <vector>

namespace engine {
namespace pipelines {

struct Fingerprints {
  /// The fingerprints of the data frames used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> df_fingerprints_;

  /// The fingerprints of the feature learners used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> fl_fingerprints_;

  /// The fingerprints of the feature selectors used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> fs_fingerprints_;

  /// The fingerprints of the preprocessor used for fitting.
  const std::vector<Poco::JSON::Object::Ptr> preprocessor_fingerprints_;
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FINGERPRINTS_HPP_

