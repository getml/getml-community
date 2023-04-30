// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/transform.hpp"

#include <stdexcept>

#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/make_placeholder.hpp"
#include "engine/pipelines/modify_data_frames.hpp"
#include "engine/pipelines/score.hpp"
#include "engine/pipelines/staging.hpp"
#include "json/json.hpp"
#include "metrics/Scores.hpp"
#include "transpilation/SQLDialectParser.hpp"

namespace engine {
namespace pipelines {
namespace transform {

/// Applies the preprocessors.
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
apply_preprocessors(const FeaturesOnlyParams& _params,
                    const containers::DataFrame& _population_df,
                    const std::vector<containers::DataFrame>& _peripheral_dfs);

/// Gets the numerical columns from _population_df and
/// returns a combination of the autofeatures and the
/// numerical columns.
containers::NumericalFeatures get_numerical_features(
    const containers::NumericalFeatures& _autofeatures,
    const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl);

/// Generates the autofeatures.
containers::NumericalFeatures generate_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl);

/// Makes or retrieves the autofeatures as part of make_features(...).
containers::NumericalFeatures make_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl);

/// Retrieves the features from a cached data frame.
std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
retrieve_features_from_cache(const containers::DataFrame& _df);

/// Selects the autofeatures based on the feature selectors.
containers::NumericalFeatures select_autofeatures(
    const containers::NumericalFeatures& _autofeatures,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl);
// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
apply_preprocessors(const FeaturesOnlyParams& _params,
                    const containers::DataFrame& _population_df,
                    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  auto population_df = _population_df;

  auto peripheral_dfs = _peripheral_dfs;

  const auto [placeholder, peripheral_names] =
      _params.get<"pipeline_">().make_placeholder();

  const auto& logger = _params.get<"transform_params_">().get<"logger_">();

  const auto& socket = _params.get<"transform_params_">().get<"socket_">();

  const auto socket_logger =
      logger
          ? std::make_shared<communication::SocketLogger>(logger, true, socket)
          : std::shared_ptr<communication::SocketLogger>();

  if (socket_logger) {
    socket_logger->log("Preprocessing...");
  }

  for (size_t i = 0; i < _params.get<"preprocessors_">().size(); ++i) {
    const auto progress = (i * 100) / _params.get<"preprocessors_">().size();

    if (socket_logger) {
      socket_logger->log("Progress: " + std::to_string(progress) + "%.");
    }

    auto& p = _params.get<"preprocessors_">().at(i);

    const auto params = preprocessors::TransformParams{
        .cmd_ = _params.get<"transform_params_">().get<"cmd_">(),
        .categories_ = _params.get<"transform_params_">().get<"categories_">(),
        .logger_ = socket_logger,
        .logging_begin_ = (i * 100) / _params.get<"preprocessors_">().size(),
        .logging_end_ =
            ((i + 1) * 100) / _params.get<"preprocessors_">().size(),
        .peripheral_dfs_ = peripheral_dfs,
        .peripheral_names_ = *peripheral_names,
        .placeholder_ = *placeholder,
        .population_df_ = population_df};

    std::tie(population_df, peripheral_dfs) = p->transform(params);
  }

  if (socket_logger) {
    socket_logger->log("Progress: 100%.");
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures generate_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl) {
  auto autofeatures = containers::NumericalFeatures();

  for (size_t i = 0; i < _feature_learners.size(); ++i) {
    const auto& fe = _feature_learners.at(i);

    const auto socket_logger =
        std::make_shared<const communication::SocketLogger>(
            _params.get<"logger_">(), fe->silent(), _params.get<"socket_">());

    const auto& index = _params.get<"predictor_impl_">()->autofeatures().at(i);

    const auto params = featurelearners::TransformParams{
        .cmd_ = _params.get<"cmd_">(),
        .index_ = index,
        .logger_ = socket_logger,
        .peripheral_dfs_ = _params.get<"peripheral_dfs_">(),
        .population_df_ = _params.get<"population_df_">(),
        .prefix_ = std::to_string(i + 1) + "_",
        .temp_dir_ = _params.get<"categories_">()->temp_dir()};

    auto new_features = fe->transform(params);

    autofeatures.insert(autofeatures.end(), new_features.begin(),
                        new_features.end());
  }

  return autofeatures;
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures generate_predictions(
    const FittedPipeline& _fitted,
    const containers::CategoricalFeatures& _categorical_features,
    const containers::NumericalFeatures& _numerical_features) {
  const auto predictor =
      [&_fitted](const size_t _i,
                 const size_t _j) -> fct::Ref<const predictors::Predictor> {
    assert_true(_i < _fitted.predictors_.predictors_.size());
    assert_true(_j < _fitted.predictors_[_i].size());
    return _fitted.predictors_.predictors_.at(_i).at(_j);
  };

  const auto nrows = [&_numerical_features,
                      &_categorical_features]() -> size_t {
    if (_numerical_features.size() > 0) {
      return _numerical_features[0].size();
    }
    if (_categorical_features.size() > 0) {
      return _categorical_features[0].size();
    }
    assert_true(false && "No features");
    return 0;
  };

  auto predictions = containers::NumericalFeatures();

  for (size_t i = 0; i < _fitted.predictors_.size(); ++i) {
    const auto num_predictors_per_set = _fitted.predictors_.at(i).size();

    const auto divisor = static_cast<Float>(num_predictors_per_set);

    auto mean_prediction =
        helpers::Feature<Float>(std::make_shared<std::vector<Float>>(
            nrows()));  // TODO: Use actual pool

    for (size_t j = 0; j < num_predictors_per_set; ++j) {
      const auto new_prediction =
          predictor(i, j)->predict(_categorical_features, _numerical_features);

      assert_true(new_prediction.size() == mean_prediction.size());

      std::transform(mean_prediction.begin(), mean_prediction.end(),
                     new_prediction.begin(), mean_prediction.begin(),
                     std::plus<Float>());
    }

    for (auto& val : mean_prediction) {
      val /= divisor;
    }

    predictions.push_back(mean_prediction);
  }

  return predictions;
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures get_categorical_features(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl) {
  auto categorical_features = containers::CategoricalFeatures();

  if (!_pipeline.include_categorical()) {
    return categorical_features;
  }

  for (const auto& col : _predictor_impl.categorical_colnames()) {
    categorical_features.push_back(
        helpers::Feature<Int>(_population_df.categorical(col).data_ptr()));
  }

  return categorical_features;
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures get_numerical_features(
    const containers::NumericalFeatures& _autofeatures,
    const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl) {
  const auto is_null = [](const Float _val) {
    return (std::isnan(_val) || std::isinf(_val));
  };

  const auto contains_null = [is_null](const auto& _col) -> bool {
    return std::any_of(_col.begin(), _col.end(), is_null);
  };

  auto numerical_features = _autofeatures;

  for (const auto& col : _predictor_impl.numerical_colnames()) {
    numerical_features.push_back(
        helpers::Feature<Float>(_population_df.numerical(col).data_ptr()));
    if (contains_null(numerical_features.back())) {
      throw std::runtime_error("Column '" + col +
                               "' contains values that are nan or infinite!");
    }
  }
  return numerical_features;
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures make_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl) {
  if (_params.get<"autofeatures_">() &&
      _params.get<"autofeatures_">()->size() ==
          _params.get<"predictor_impl_">()->num_autofeatures()) {
    return *_params.get<"autofeatures_">();
  }

  if (_params.get<"autofeatures_">() &&
      _params.get<"autofeatures_">()->size() != 0) {
    return select_autofeatures(*_params.get<"autofeatures_">(),
                               _feature_learners, _predictor_impl);
  }

  return generate_autofeatures(_params, _feature_learners, _predictor_impl);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
make_features(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl,
    const std::vector<commands::Fingerprint>& _fs_fingerprints) {
  const auto df = _params.get<"data_frame_tracker_">().retrieve(
      _fs_fingerprints, _params.get<"original_population_df_">(),
      _params.get<"original_peripheral_dfs_">());

  if (df) {
    return retrieve_features_from_cache(*df);
  }

  const auto autofeatures =
      make_autofeatures(_params, _feature_learners, _predictor_impl);

  const auto numerical_features =
      get_numerical_features(autofeatures, _params.get<"population_df_">(),
                             *_params.get<"predictor_impl_">());

  const auto categorical_features =
      get_categorical_features(_pipeline, _params.get<"population_df_">(),
                               *_params.get<"predictor_impl_">());

  return std::make_tuple(numerical_features, categorical_features,
                         autofeatures);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
retrieve_features_from_cache(const containers::DataFrame& _df) {
  containers::NumericalFeatures autofeatures;

  containers::NumericalFeatures numerical_features;

  for (size_t i = 0; i < _df.num_numericals(); ++i) {
    const auto col = _df.numerical(i);

    numerical_features.push_back(helpers::Feature<Float>(col.data_ptr()));

    if (col.name().substr(0, 8) == "feature_") {
      autofeatures.push_back(numerical_features.back());
    }
  }

  containers::CategoricalFeatures categorical_features;

  for (size_t i = 0; i < _df.num_categoricals(); ++i) {
    const auto col = _df.categorical(i);
    categorical_features.push_back(col.data_ptr());
  }

  return std::make_tuple(numerical_features, categorical_features,
                         autofeatures);
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures select_autofeatures(
    const containers::NumericalFeatures& _autofeatures,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl) {
  assert_true(_feature_learners.size() ==
              _predictor_impl.autofeatures().size());

  containers::NumericalFeatures selected;

  size_t offset = 0;

  for (size_t i = 0; i < _feature_learners.size(); ++i) {
    for (const size_t ix : _predictor_impl.autofeatures().at(i)) {
      assert_true(offset + ix < _autofeatures.size());

      selected.push_back(_autofeatures.at(offset + ix));
    }

    offset += _feature_learners.at(i)->num_features();
  }

  assert_true(offset == _autofeatures.size());

  return selected;
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           std::shared_ptr<const metrics::Scores>>
transform(const TransformParams& _params, const Pipeline& _pipeline,
          const FittedPipeline& _fitted) {
  const bool score = _params.get<"cmd_">().get<"score_">();

  const bool predict = _params.get<"cmd_">().get<"predict_">();

  if ((score || predict) && _fitted.num_predictors_per_set() == 0) {
    throw std::runtime_error(
        "You cannot call .predict(...) or .score(...) on a pipeline "
        "that doesn't have any predictors.");
  }

  const auto features_only_params = FeaturesOnlyParams{
      .dependencies_ = _fitted.fingerprints_.get<"fs_fingerprints_">(),
      .feature_learners_ = _fitted.feature_learners_,
      .fs_fingerprints_ = _fitted.fingerprints_.get<"fs_fingerprints_">(),
      .pipeline_ = _pipeline,
      .preprocessors_ = _fitted.preprocessors_,
      .predictor_impl_ = _fitted.predictors_.impl_,
      .transform_params_ = _params};

  const auto [numerical_features, categorical_features, population_df] =
      transform_features_only(features_only_params);

  if (!score && !predict) {
    return std::make_tuple(numerical_features, categorical_features, nullptr);
  }
  const auto scores =
      score ? score::calculate_feature_stats(_pipeline, _fitted,
                                             numerical_features, population_df)
            : nullptr;

  const auto transformed_categorical_features =
      _fitted.predictors_.impl_->transform_encodings(categorical_features);

  const auto predictions = generate_predictions(
      _fitted, transformed_categorical_features, numerical_features);

  return std::make_tuple(predictions, containers::CategoricalFeatures(),
                         scores);
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
stage_data_frames(const Pipeline& _pipeline,
                  const containers::DataFrame& _population_df,
                  const std::vector<containers::DataFrame>& _peripheral_dfs,
                  const std::shared_ptr<const communication::Logger>& _logger,
                  const std::optional<std::string>& _temp_dir,
                  Poco::Net::StreamSocket* _socket) {
  const auto socket_logger =
      std::make_shared<communication::SocketLogger>(_logger, true, _socket);

  socket_logger->log("staging...");

  const auto data_model = _pipeline.obj().get<"data_model_">();

  const auto peripheral_names = _pipeline.parse_peripheral();

  auto population_df = _population_df;

  auto peripheral_dfs = _peripheral_dfs;

  modify_data_frames::add_time_stamps(*data_model, *peripheral_names,
                                      &population_df, &peripheral_dfs);

  modify_data_frames::add_join_keys(*data_model, *peripheral_names, _temp_dir,
                                    &population_df, &peripheral_dfs);

  const auto placeholder =
      make_placeholder::make_placeholder(*data_model, "t1");

  const auto joined_peripheral_names =
      make_placeholder::make_peripheral(*placeholder);

  staging::join_tables(*peripheral_names, placeholder->name(),
                       joined_peripheral_names, &population_df,
                       &peripheral_dfs);

  socket_logger->log("Progress: 100%.");

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::DataFrame>
transform_features_only(const FeaturesOnlyParams& _params) {
  auto [population_df, peripheral_dfs] = stage_data_frames(
      _params.get<"pipeline_">(),
      _params.get<"transform_params_">().get<"original_population_df_">(),
      _params.get<"transform_params_">().get<"original_peripheral_dfs_">(),
      _params.get<"transform_params_">().get<"logger_">(),
      _params.get<"transform_params_">().get<"categories_">()->temp_dir(),
      _params.get<"transform_params_">().get<"socket_">());

  std::tie(population_df, peripheral_dfs) =
      apply_preprocessors(_params, population_df, peripheral_dfs);

  const auto make_features_params = MakeFeaturesParams(
      _params.get<"transform_params_">() *
      fct::make_field<"dependencies_">(_params.get<"dependencies_">()) *
      fct::make_field<"peripheral_dfs_">(peripheral_dfs) *
      fct::make_field<"population_df_">(population_df) *
      fct::make_field<"predictor_impl_">(_params.get<"predictor_impl_">()) *
      fct::make_field<"autofeatures_", containers::NumericalFeatures*>(
          nullptr));

  const auto [numerical_features, categorical_features, _] = make_features(
      make_features_params, _params.get<"pipeline_">(),
      _params.get<"feature_learners_">(), *_params.get<"predictor_impl_">(),
      *_params.get<"fs_fingerprints_">());

  return std::make_tuple(numerical_features, categorical_features,
                         population_df);
}

}  // namespace transform
}  // namespace pipelines
}  // namespace engine
