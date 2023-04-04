// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_SAVE_HPP_
#define ENGINE_PIPELINES_SAVE_HPP_

#include <string>
#include <vector>

#include "engine/pipelines/SaveParams.hpp"

namespace engine {
namespace pipelines {

class Save {
 public:
  /// Saves the pipeline.
  static void save(const SaveParams& _params);

 private:
  /// Saves the feature learners.
  static void save_feature_learners(const SaveParams& _params,
                                    const Poco::TemporaryFile& _tfile);

  /// Saves the pipeline itself to a JSON file.
  static void save_pipeline_json(const SaveParams& _params,
                                 const Poco::TemporaryFile& _tfile);

  /// Saves the feature selectors or predictors.
  static void save_predictors(
      const std::vector<std::vector<fct::Ref<const predictors::Predictor>>>&
          _predictors,
      const std::string& _purpose, const Poco::TemporaryFile& _tfile);

  /// Saves the preprocessors.
  static void save_preprocessors(const SaveParams& _params,
                                 const Poco::TemporaryFile& _tfile);

 private:
  /// Moves the files from their temporary location to their final destination.
  static void move_tfile(const std::string& _path, const std::string& _name,
                         Poco::TemporaryFile* _tfile);
};

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQL_HPP_
