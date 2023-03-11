// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Transform.hpp"

#include <stdexcept>

#include "engine/pipelines/DataFrameModifier.hpp"
#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/PlaceholderMaker.hpp"
#include "engine/pipelines/Score.hpp"
#include "engine/pipelines/Staging.hpp"
#include "json/json.hpp"
#include "metrics/Scores.hpp"
#include "transpilation/SQLDialectParser.hpp"

namespace engine {
namespace pipelines {

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Transform::apply_preprocessors(
    const FeaturesOnlyParams& _params,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  auto population_df = _population_df;

  auto peripheral_dfs = _peripheral_dfs;

  const auto [placeholder, peripheral_names] =
      _params.pipeline_.make_placeholder();

  assert_true(_params.transform_params_.logger_);

  const auto socket_logger = std::make_shared<communication::SocketLogger>(
      _params.transform_params_.logger_, true,
      _params.transform_params_.socket_);

  socket_logger->log("Preprocessing...");

  for (size_t i = 0; i < _params.preprocessors_.size(); ++i) {
    const auto progress = (i * 100) / _params.preprocessors_.size();

    socket_logger->log("Progress: " + std::to_string(progress) + "%.");

    auto& p = _params.preprocessors_.at(i);

    const auto params = preprocessors::TransformParams{
        .cmd_ = _params.transform_params_.cmd_,
        .categories_ = _params.transform_params_.categories_,
        .logger_ = socket_logger,
        .logging_begin_ = (i * 100) / _params.preprocessors_.size(),
        .logging_end_ = ((i + 1) * 100) / _params.preprocessors_.size(),
        .peripheral_dfs_ = peripheral_dfs,
        .peripheral_names_ = *peripheral_names,
        .placeholder_ = *placeholder,
        .population_df_ = population_df};

    std::tie(population_df, peripheral_dfs) = p->transform(params);
  }

  socket_logger->log("Progress: 100%.");

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures Transform::generate_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl) {
  auto autofeatures = containers::NumericalFeatures();

  for (size_t i = 0; i < _feature_learners.size(); ++i) {
    const auto& fe = _feature_learners.at(i);

    const auto socket_logger =
        std::make_shared<const communication::SocketLogger>(
            _params.logger_, fe->silent(), _params.socket_);

    const auto& index = _params.predictor_impl_->autofeatures().at(i);

    const auto params = featurelearners::TransformParams{
        .cmd_ = _params.cmd_,
        .index_ = index,
        .logger_ = socket_logger,
        .peripheral_dfs_ = _params.peripheral_dfs_,
        .population_df_ = _params.population_df_,
        .prefix_ = std::to_string(i + 1) + "_",
        .temp_dir_ = _params.categories_->temp_dir()};

    auto new_features = fe->transform(params);

    autofeatures.insert(autofeatures.end(), new_features.begin(),
                        new_features.end());
  }

  return autofeatures;
}

// ----------------------------------------------------------------------------

containers::NumericalFeatures Transform::generate_predictions(
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

containers::CategoricalFeatures Transform::get_categorical_features(
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

containers::NumericalFeatures Transform::get_numerical_features(
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

containers::NumericalFeatures Transform::make_autofeatures(
    const MakeFeaturesParams& _params,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl) {
  if (_params.autofeatures_ &&
      _params.autofeatures_->size() ==
          _params.predictor_impl_->num_autofeatures()) {
    return *_params.autofeatures_;
  }

  if (_params.autofeatures_ && _params.autofeatures_->size() != 0) {
    return select_autofeatures(*_params.autofeatures_, _feature_learners,
                               _predictor_impl);
  }

  return generate_autofeatures(_params, _feature_learners, _predictor_impl);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
Transform::make_features(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const predictors::PredictorImpl& _predictor_impl,
    const std::vector<typename commands::PredictorFingerprint::DependencyType>&
        _fs_fingerprints) {
  // TODO
  /*const auto df = _params.data_frame_tracker_.retrieve(
    _fs_fingerprints, _params.original_population_df_,
    _params.original_peripheral_dfs_);

if (df) {
  return retrieve_features_from_cache(*df);
}*/

  const auto autofeatures =
      make_autofeatures(_params, _feature_learners, _predictor_impl);

  const auto numerical_features = get_numerical_features(
      autofeatures, _params.population_df_, *_params.predictor_impl_);

  const auto categorical_features = get_categorical_features(
      _pipeline, _params.population_df_, *_params.predictor_impl_);

  return std::make_tuple(numerical_features, categorical_features,
                         autofeatures);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::NumericalFeatures>
Transform::retrieve_features_from_cache(const containers::DataFrame& _df) {
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

containers::NumericalFeatures Transform::select_autofeatures(
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
Transform::transform(const TransformParams& _params, const Pipeline& _pipeline,
                     const FittedPipeline& _fitted) {
  // TODO
  const bool score = false; /*_params.cmd_.has("score_") &&
                   JSON::get_value<bool>(_params.cmd_, "score_");*/

  const bool predict = false; /* _params.cmd_.has("predict_") &&
                         JSON::get_value<bool>(_params.cmd_, "predict_");*/

  if ((score || predict) && _fitted.num_predictors_per_set() == 0) {
    throw std::runtime_error(
        "You cannot call .predict(...) or .score(...) on a pipeline "
        "that doesn't have any predictors.");
  }

  const auto features_only_params = FeaturesOnlyParams{
      .dependencies_ = {},  // TODO _fitted.fingerprints_.fs_fingerprints_,
      .feature_learners_ = _fitted.feature_learners_,
      .pipeline_ = _pipeline,
      .preprocessors_ = _fitted.preprocessors_,
      .predictor_impl_ = _fitted.predictors_.impl_,
      .transform_params_ = _params};

  const auto [numerical_features, categorical_features, population_df] =
      transform_features_only(features_only_params);

  // TODO

  // if (!score && !predict) {
  return std::make_tuple(numerical_features, categorical_features, nullptr);
  //}
  /*
    const auto scores = score ? Score::calculate_feature_stats(
                                    _pipeline, _fitted, numerical_features,
                                    _params.cmd_, population_df)
                              : nullptr;

    const auto transformed_categorical_features =
        _fitted.predictors_.impl_->transform_encodings(categorical_features);

    const auto predictions = generate_predictions(
        _fitted, transformed_categorical_features, numerical_features);

    return std::make_tuple(predictions, containers::CategoricalFeatures(),
                           scores);*/
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Transform::stage_data_frames(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::optional<std::string>& _temp_dir,
    Poco::Net::StreamSocket* _socket) {
  const auto socket_logger =
      std::make_shared<communication::SocketLogger>(_logger, true, _socket);

  socket_logger->log("Staging...");

  const auto data_model = _pipeline.obj().get<"data_model_">();

  const auto peripheral_names = _pipeline.parse_peripheral();

  auto population_df = _population_df;

  auto peripheral_dfs = _peripheral_dfs;

  DataFrameModifier::add_time_stamps(*data_model, *peripheral_names,
                                     &population_df, &peripheral_dfs);

  DataFrameModifier::add_join_keys(*data_model, *peripheral_names, _temp_dir,
                                   &population_df, &peripheral_dfs);

  const auto placeholder =
      PlaceholderMaker::make_placeholder(*data_model, "t1");

  const auto joined_peripheral_names =
      PlaceholderMaker::make_peripheral(*placeholder);

  Staging::join_tables(*peripheral_names, placeholder->name(),
                       joined_peripheral_names, &population_df,
                       &peripheral_dfs);

  socket_logger->log("Progress: 100%.");

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------------------------------

std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
           containers::DataFrame>
Transform::transform_features_only(const FeaturesOnlyParams& _params) {
  auto [population_df, peripheral_dfs] = stage_data_frames(
      _params.pipeline_, _params.transform_params_.original_population_df_,
      _params.transform_params_.original_peripheral_dfs_,
      _params.transform_params_.logger_,
      _params.transform_params_.categories_->temp_dir(),
      _params.transform_params_.socket_);

  std::tie(population_df, peripheral_dfs) =
      apply_preprocessors(_params, population_df, peripheral_dfs);

  const auto make_features_params = MakeFeaturesParams{
      .categories_ = _params.transform_params_.categories_,
      .cmd_ = _params.transform_params_.cmd_,
      .data_frame_tracker_ = _params.transform_params_.data_frame_tracker_,
      .dependencies_ = _params.dependencies_,
      .logger_ = _params.transform_params_.logger_,
      .peripheral_dfs_ = peripheral_dfs,
      .population_df_ = population_df,
      .predictor_impl_ = _params.predictor_impl_,
      .autofeatures_ = nullptr,
      .socket_ = _params.transform_params_.socket_};

  const auto [numerical_features, categorical_features, _] = make_features(
      make_features_params, _params.pipeline_, _params.feature_learners_,
      *_params.predictor_impl_, _params.fs_fingerprints_);

  return std::make_tuple(numerical_features, categorical_features,
                         population_df);
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine
