// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Fit.hpp"

#include "engine/commands/FeatureLearner.hpp"
#include "engine/featurelearners/AbstractFeatureLearner.hpp"
#include "engine/pipelines/Score.hpp"
#include "engine/pipelines/Transform.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/Field.hpp"
#include "fct/collect.hpp"
#include "helpers/StringReplacer.hpp"
#include "json/json.hpp"
#include "predictors/Predictor.hpp"

namespace engine {
namespace pipelines {

// ------------------------------------------------------------------------

std::string Fit::beautify_purpose(const std::string& _purpose) {
  const auto purpose = helpers::StringReplacer::replace_all(_purpose, "_", " ");
  const auto len = purpose.find_last_not_of("s ") + 1;
  return purpose.substr(0, len);
}

// ------------------------------------------------------------------------

std::vector<size_t> Fit::calculate_importance_index(
    const Predictors& _feature_selectors) {
  const auto sum_importances = calculate_sum_importances(_feature_selectors);

  const auto make_pair =
      [&sum_importances](const size_t _i) -> std::pair<size_t, Float> {
    return std::make_pair(_i, sum_importances.at(_i));
  };

  const auto iota = fct::iota<size_t>(0, sum_importances.size());

  auto pairs = fct::collect::vector<std::pair<size_t, Float>>(
      iota | VIEWS::transform(make_pair));

  std::sort(
      pairs.begin(), pairs.end(),
      [](const std::pair<size_t, Float>& p1,
         const std::pair<size_t, Float>& p2) { return p1.second > p2.second; });

  return fct::collect::vector<size_t>(pairs | VIEWS::keys);
}

// ------------------------------------------------------------------------

std::vector<Float> Fit::calculate_sum_importances(
    const Predictors& _feature_selectors) {
  const auto importances = Score::feature_importances(_feature_selectors);

  assert_true(importances.size() == _feature_selectors.size());

  auto sum_importances = importances.at(0);

  for (size_t i = 1; i < importances.size(); ++i) {
    assert_true(sum_importances.size() == importances[i].size());

    std::transform(importances.at(i).begin(), importances.at(i).end(),
                   sum_importances.begin(), sum_importances.begin(),
                   std::plus<Float>());
  }

  return sum_importances;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Fit::extract_df_fingerprints(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  const auto placeholder = JSON::get_object(_pipeline.obj(), "data_model_");

  // TODO: Insert data frames.
  /*std::vector<Poco::JSON::Object::Ptr> df_fingerprints = {
      placeholder, _population_df.fingerprint()};

  for (const auto& df : _peripheral_dfs) {
    df_fingerprints.push_back(df.fingerprint());
  }*/

  return {placeholder};
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Fit::extract_fl_fingerprints(
    const std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  if (_feature_learners.size() == 0) {
    return _dependencies;
  }

  return {};

  // TODO
  /*const auto get_fingerprint = [](const auto& _fl) -> Poco::JSON::Object::Ptr
  { return _fl->fingerprint();
  };

  return fct::collect::vector<Poco::JSON::Object::Ptr>(
      _feature_learners | VIEWS::transform(get_fingerprint));*/
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Fit::extract_predictor_fingerprints(
    const std::vector<std::vector<fct::Ref<predictors::Predictor>>>&
        _predictors,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  if (_predictors.size() == 0 || _predictors.at(0).size() == 0) {
    return _dependencies;
  }

  const auto get_fingerprint = [](const auto& _p) -> Poco::JSON::Object::Ptr {
    return _p->fingerprint();
  };

  const auto get_fingerprints = [get_fingerprint](const auto& _ps) {
    return _ps | VIEWS::transform(get_fingerprint);
  };

  return fct::join::vector<Poco::JSON::Object::Ptr>(
      _predictors | VIEWS::transform(get_fingerprints));
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Fit::extract_preprocessor_fingerprints(
    const std::vector<fct::Ref<preprocessors::Preprocessor>>& _preprocessors,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  if (_preprocessors.size() == 0) {
    return _dependencies;
  }

  return _dependencies;

  // TODO
  /*const auto get_fingerprint = [](const auto& _fl) -> Poco::JSON::Object::Ptr
  { return _fl->fingerprint();
  };

  return fct::collect::vector<Poco::JSON::Object::Ptr>(
      _preprocessors | VIEWS::transform(get_fingerprint));*/
}

// ----------------------------------------------------------------------

std::pair<fct::Ref<const helpers::Schema>,
          fct::Ref<const std::vector<helpers::Schema>>>
Fit::extract_schemata(const containers::DataFrame& _population_df,
                      const std::vector<containers::DataFrame>& _peripheral_dfs,
                      const bool _separate_discrete) {
  const auto extract_schema =
      [_separate_discrete](
          const containers::DataFrame& _df) -> helpers::Schema {
    return _df.to_schema(_separate_discrete);
  };

  const auto population_schema =
      fct::Ref<const helpers::Schema>::make(extract_schema(_population_df));

  const auto peripheral_schema =
      fct::Ref<const std::vector<helpers::Schema>>::make(
          fct::collect::vector<helpers::Schema>(
              _peripheral_dfs | VIEWS::transform(extract_schema)));

  return std::make_pair(population_schema, peripheral_schema);
}

// ----------------------------------------------------------------------

std::pair<fct::Ref<const FittedPipeline>, fct::Ref<const metrics::Scores>>
Fit::fit(const Pipeline& _pipeline, const FitParams& _params) {
  const auto fit_preprocessors_params = FitPreprocessorsParams{
      .categories_ = _params.categories_,
      .cmd_ = _params.cmd_,
      .logger_ = _params.logger_,
      .peripheral_dfs_ = _params.peripheral_dfs_,
      .population_df_ = _params.population_df_,
      .preprocessor_tracker_ = _params.preprocessor_tracker_,
      .socket_ = _params.socket_};

  const auto preprocessed =
      fit_preprocessors_only(_pipeline, fit_preprocessors_params);

  const auto [population_schema, peripheral_schema] =
      extract_schemata(_params.population_df_, _params.peripheral_dfs_, false);

  const auto [modified_population_schema, modified_peripheral_schema] =
      extract_schemata(preprocessed.population_df_,
                       preprocessed.peripheral_dfs_, true);

  const auto [placeholder, peripheral] = _pipeline.make_placeholder();

  const featurelearners::FeatureLearnerParams feature_learner_params =
      fct::make_field<"dependencies_">(
          json::Parser<fct::Ref<const std::vector<
              typename commands::FeatureLearnerFingerprint::DependencyType>>>::
              from_json(
                  preprocessed
                      .preprocessor_fingerprints_)) *  // TODO: Remove from_json
      fct::make_field<"peripheral_">(peripheral) *
      fct::make_field<"peripheral_schema_">(modified_peripheral_schema) *
      fct::make_field<"placeholder_">(placeholder) *
      fct::make_field<"population_schema_">(modified_population_schema) *
      fct::make_field<"target_num_">(
          featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS);

  const auto [feature_learners, fl_fingerprints] = fit_feature_learners(
      _pipeline, _params, preprocessed.population_df_,
      preprocessed.peripheral_dfs_, feature_learner_params);

  const auto feature_selector_impl = make_feature_selector_impl(
      _pipeline, feature_learners, preprocessed.population_df_);

  containers::NumericalFeatures autofeatures;

  const auto fit_feature_selectors_params =
      FitPredictorsParams{.autofeatures_ = &autofeatures,
                          .dependencies_ = fl_fingerprints,
                          .feature_learners_ = feature_learners,
                          .fit_params_ = _params,
                          .impl_ = feature_selector_impl,
                          .peripheral_dfs_ = preprocessed.peripheral_dfs_,
                          .pipeline_ = _pipeline,
                          .population_df_ = preprocessed.population_df_,
                          .preprocessors_ = preprocessed.preprocessors_,
                          .purpose_ = "feature_selectors_"};

  const auto [feature_selectors, fs_fingerprints] =
      fit_predictors(fit_feature_selectors_params);

  const auto predictor_impl = make_predictor_impl(_pipeline, feature_selectors,
                                                  preprocessed.population_df_);

  // TODO: This is a temporary fix.
  const auto validation_fingerprint =
      /*_params.validation_df_ ? std::vector<Poco::JSON::Object::Ptr>(
                                   {_params.validation_df_->fingerprint()})
                             :*/
      std::vector<Poco::JSON::Object::Ptr>();

  const auto dependencies = fct::join::vector<Poco::JSON::Object::Ptr>(
      {fs_fingerprints, validation_fingerprint});

  const auto fit_predictors_params =
      FitPredictorsParams{.autofeatures_ = &autofeatures,
                          .dependencies_ = dependencies,
                          .feature_learners_ = feature_learners,
                          .fit_params_ = _params,
                          .impl_ = predictor_impl,
                          .peripheral_dfs_ = preprocessed.peripheral_dfs_,
                          .pipeline_ = _pipeline,
                          .population_df_ = preprocessed.population_df_,
                          .preprocessors_ = preprocessed.preprocessors_,
                          .purpose_ = "predictors_"};

  const auto [predictors, _] = fit_predictors(fit_predictors_params);

  const bool score = predictors.size() > 0 && predictors.at(0).size() > 0;

  const auto score_params =
      score ? std::make_optional<MakeFeaturesParams>(MakeFeaturesParams{
                  .categories_ = _params.categories_,
                  .cmd_ = _params.cmd_,
                  .data_frame_tracker_ = _params.data_frame_tracker_,
                  .dependencies_ = fs_fingerprints,
                  .logger_ = _params.logger_,
                  .original_peripheral_dfs_ = _params.peripheral_dfs_,
                  .original_population_df_ = _params.population_df_,
                  .peripheral_dfs_ = preprocessed.peripheral_dfs_,
                  .population_df_ = preprocessed.population_df_,
                  .predictor_impl_ = predictor_impl,
                  .autofeatures_ = &autofeatures,
                  .socket_ = _params.socket_})
            : std::nullopt;

  const auto fingerprints =
      Fingerprints{.df_fingerprints_ = std::move(preprocessed.df_fingerprints_),
                   .fl_fingerprints_ = std::move(fl_fingerprints),
                   .fs_fingerprints_ = std::move(fs_fingerprints),
                   .preprocessor_fingerprints_ =
                       std::move(preprocessed.preprocessor_fingerprints_)};

  const auto fitted_pipeline =
      fct::Ref<const FittedPipeline>::make(FittedPipeline{
          .feature_learners_ = std::move(feature_learners),
          .feature_selectors_ = std::move(feature_selectors),
          .fingerprints_ = std::move(fingerprints),
          .modified_peripheral_schema_ = std::move(modified_peripheral_schema),
          .modified_population_schema_ = std::move(modified_population_schema),
          .peripheral_schema_ = std::move(peripheral_schema),
          .population_schema_ = std::move(population_schema),
          .predictors_ = std::move(predictors),
          .preprocessors_ = std::move(preprocessed.preprocessors_)});

  const auto scores = make_scores(score_params, _pipeline, *fitted_pipeline);

  return std::make_pair(std::move(fitted_pipeline), std::move(scores));
}

// ----------------------------------------------------------------------------

std::pair<std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>,
          std::vector<Poco::JSON::Object::Ptr>>
Fit::fit_feature_learners(
    const Pipeline& _pipeline, const FitParams& _params,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const featurelearners::FeatureLearnerParams& _feature_learner_params) {
  auto feature_learners = init_feature_learners(
      _pipeline, _feature_learner_params, _population_df.num_targets());

  if (feature_learners.size() == 0) {
    return std::make_pair(
        to_const(feature_learners),
        std::vector<Poco::JSON::Object::Ptr>(
            {}) /*TODO _feature_learner_params.get<"dependencies_">()*/);
  }

  const auto [modified_population_schema, modified_peripheral_schema] =
      extract_schemata(_population_df, _peripheral_dfs, true);

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);

    const auto socket_logger =
        std::make_shared<const communication::SocketLogger>(
            _params.logger_, fe->silent(), _params.socket_);

    const auto fingerprint = fe->fingerprint();

    const auto retrieved_fe = _params.fe_tracker_->retrieve(fingerprint);

    if (retrieved_fe) {
      socket_logger->log(
          "Retrieving features (because a similar feature "
          "learner has already been fitted)...");

      socket_logger->log("Progress: 100%.");

      fe = fct::Ref<featurelearners::AbstractFeatureLearner>(retrieved_fe);

      continue;
    }

    const auto params = featurelearners::FitParams{
        .cmd_ = _params.cmd_,
        .logger_ = socket_logger,
        .peripheral_dfs_ = _peripheral_dfs,
        .population_df_ = _population_df,
        .prefix_ = std::to_string(i + 1) + "_",
        .temp_dir_ = _params.categories_->temp_dir()};

    fe->fit(params);

    _params.fe_tracker_->add(fe);
  }

  auto fl_fingerprints = extract_fl_fingerprints(
      feature_learners,
      std::vector<Poco::JSON::Object::Ptr>(
          {}) /*TODO _feature_learner_params.get<"dependencies_">()*/);

  return std::make_pair(to_const(feature_learners), std::move(fl_fingerprints));
}

// ----------------------------------------------------------------------------

std::pair<Predictors, std::vector<Poco::JSON::Object::Ptr>> Fit::fit_predictors(
    const FitPredictorsParams& _params) {
  auto predictors = init_predictors(_params.pipeline_, _params.purpose_,
                                    _params.impl_, _params.dependencies_,
                                    _params.population_df_.num_targets());

  const auto [retrieved_predictors, all_retrieved] =
      retrieve_predictors(_params.fit_params_.pred_tracker_, predictors);

  if (all_retrieved) {
    const auto fingerprints = extract_predictor_fingerprints(
        to_ref(retrieved_predictors), _params.dependencies_);
    const auto predictors_struct =
        Predictors{.impl_ = _params.impl_,
                   .predictors_ = to_const(to_ref(retrieved_predictors))};
    return std::make_pair(predictors_struct, fingerprints);
  }

  const auto make_features_params = MakeFeaturesParams{
      .categories_ = _params.fit_params_.categories_,
      .cmd_ = _params.fit_params_.cmd_,
      .data_frame_tracker_ = _params.fit_params_.data_frame_tracker_,
      .dependencies_ = _params.dependencies_,
      .logger_ = _params.fit_params_.logger_,
      .peripheral_dfs_ = _params.peripheral_dfs_,
      .population_df_ = _params.population_df_,
      .predictor_impl_ = _params.impl_,
      .autofeatures_ = _params.autofeatures_,
      .socket_ = _params.fit_params_.socket_};

  auto [numerical_features, categorical_features, autofeatures] =
      Transform::make_features(make_features_params, _params.pipeline_,
                               _params.feature_learners_, *_params.impl_,
                               _params.fit_params_.fs_fingerprints_);

  *_params.autofeatures_ = autofeatures;

  categorical_features =
      _params.impl_->transform_encodings(categorical_features);

  auto [numerical_features_valid, categorical_features_valid] =
      make_features_validation(_params);

  if (categorical_features_valid) {
    *categorical_features_valid =
        _params.impl_->transform_encodings(*categorical_features_valid);
  }

  assert_true(_params.fit_params_.population_df_.num_targets() ==
              predictors.size());

  assert_true(predictors.size() == retrieved_predictors.size());

  for (size_t t = 0; t < _params.fit_params_.population_df_.num_targets();
       ++t) {
    const auto target_col = helpers::Feature<Float>(
        _params.fit_params_.population_df_.target(t).data_ptr());

    const auto target_col_valid =
        numerical_features_valid
            ? std::make_optional<decltype(target_col)>(
                  _params.fit_params_.validation_df_.value()
                      .target(t)
                      .to_vector_ptr())
            : std::optional<decltype(target_col)>();

    assert_true(predictors.at(t).size() == retrieved_predictors.at(t).size());

    for (size_t i = 0; i < predictors.at(t).size(); ++i) {
      auto& p = predictors.at(t).at(i);

      const auto socket_logger =
          std::make_shared<const communication::SocketLogger>(
              _params.fit_params_.logger_, p->silent(),
              _params.fit_params_.socket_);

      if (retrieved_predictors.at(t).at(i)) {
        socket_logger->log("Retrieving predictor...");
        socket_logger->log("Progress: 100%.");
        p = fct::Ref(retrieved_predictors.at(t).at(i));
        continue;
      }

      socket_logger->log(p->type() + ": Training as " +
                         beautify_purpose(_params.purpose_) + "...");

      p->fit(socket_logger, categorical_features, numerical_features,
             target_col, categorical_features_valid, numerical_features_valid,
             target_col_valid);

      _params.fit_params_.pred_tracker_->add(p);
    }
  }

  auto fingerprints =
      extract_predictor_fingerprints(predictors, _params.dependencies_);

  const auto predictors_struct =
      Predictors{.impl_ = _params.impl_, .predictors_ = to_const(predictors)};

  return std::make_pair(std::move(predictors_struct), std::move(fingerprints));
}

// ----------------------------------------------------------------------

Preprocessed Fit::fit_preprocessors_only(
    const Pipeline& _pipeline, const FitPreprocessorsParams& _params) {
  const auto targets = get_targets(_params.population_df_);

  const auto df_fingerprints = extract_df_fingerprints(
      _pipeline, _params.population_df_, _params.peripheral_dfs_);

  auto [population_df, peripheral_dfs] = Transform::stage_data_frames(
      _pipeline, _params.population_df_, _params.peripheral_dfs_,
      _params.logger_, _params.categories_->temp_dir(), _params.socket_);

  const auto [preprocessors, preprocessor_fingerprints] =
      fit_transform_preprocessors(_pipeline, _params, df_fingerprints,
                                  &population_df, &peripheral_dfs);

  return Preprocessed{.df_fingerprints_ = df_fingerprints,
                      .peripheral_dfs_ = peripheral_dfs,
                      .population_df_ = population_df,
                      .preprocessors_ = preprocessors,
                      .preprocessor_fingerprints_ = preprocessor_fingerprints};
}

// ----------------------------------------------------------------------------

std::pair<std::vector<fct::Ref<const preprocessors::Preprocessor>>,
          std::vector<Poco::JSON::Object::Ptr>>
Fit::fit_transform_preprocessors(
    const Pipeline& _pipeline, const FitPreprocessorsParams& _params,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  auto preprocessors = init_preprocessors(_pipeline, _dependencies);

  if (preprocessors.size() == 0) {
    return std::make_pair(to_const(preprocessors), _dependencies);
  }

  const auto [placeholder, peripheral_names] = _pipeline.make_placeholder();

  const auto socket_logger =
      _params.logger_ ? std::make_shared<const communication::SocketLogger>(
                            _params.logger_, true, _params.socket_)
                      : std::shared_ptr<const communication::SocketLogger>();

  if (socket_logger) {
    socket_logger->log("Preprocessing...");
  }

  for (size_t i = 0; i < preprocessors.size(); ++i) {
    if (socket_logger) {
      const auto progress = (i * 100) / preprocessors.size();
      socket_logger->log("Progress: " + std::to_string(progress) + "%.");
    }

    auto& p = preprocessors.at(i);

    // TODO
    /*const auto fingerprint = p->fingerprint();

    const auto retrieved_preprocessor =
        _params.preprocessor_tracker_->retrieve(fingerprint);

    if (retrieved_preprocessor) {
      const auto params = preprocessors::TransformParams{
          .cmd_ = _params.cmd_,
          .categories_ = _params.categories_,
          .logger_ = socket_logger,
          .logging_begin_ = (i * 100) / preprocessors.size(),
          .logging_end_ = ((i + 1) * 100) / preprocessors.size(),
          .peripheral_dfs_ = *_peripheral_dfs,
          .peripheral_names_ = *peripheral_names,
          .placeholder_ = *placeholder,
          .population_df_ = *_population_df};

      p = fct::Ref<preprocessors::Preprocessor>(retrieved_preprocessor);

      std::tie(*_population_df, *_peripheral_dfs) = p->transform(params);

      continue;
    }*/

    const auto params = preprocessors::FitParams{
        .cmd_ = _params.cmd_,
        .categories_ = _params.categories_,
        .logger_ = socket_logger,
        .logging_begin_ = (i * 100) / preprocessors.size(),
        .logging_end_ = ((i + 1) * 100) / preprocessors.size(),
        .peripheral_dfs_ = *_peripheral_dfs,
        .peripheral_names_ = *peripheral_names,
        .placeholder_ = *placeholder,
        .population_df_ = *_population_df};

    std::tie(*_population_df, *_peripheral_dfs) = p->fit_transform(params);

    _params.preprocessor_tracker_->add(p);
  }

  if (socket_logger) {
    socket_logger->log("Progress: 100%.");
  }

  auto preprocessor_fingerprints =
      extract_preprocessor_fingerprints(preprocessors, _dependencies);

  return std::make_pair(to_const(preprocessors),
                        std::move(preprocessor_fingerprints));
}

// ----------------------------------------------------------------------

std::vector<std::string> Fit::get_targets(
    const containers::DataFrame& _population_df) {
  const auto get_target = [](const auto& _col) -> std::string {
    return _col.name();
  };
  return fct::collect::vector<std::string>(_population_df.targets() |
                                           VIEWS::transform(get_target));
}

// ------------------------------------------------------------------------

std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>
Fit::init_feature_learners(
    const Pipeline& _pipeline,
    const featurelearners::FeatureLearnerParams& _feature_learner_params,
    const size_t _num_targets) {
  if (_num_targets == 0) {
    throw std::runtime_error("You must provide at least one target.");
  }

  const auto make_fl_for_one_target =
      [](const featurelearners::FeatureLearnerParams& _params,
         const commands::FeatureLearner& _hyperparameters,
         const Int _target_num)
      -> fct::Ref<featurelearners::AbstractFeatureLearner> {
    const auto new_params =
        _params.replace(fct::make_field<"target_num_">(_target_num));

    return featurelearners::FeatureLearnerParser::parse(new_params,
                                                        _hyperparameters);
  };

  const auto make_fl_for_all_targets =
      [_num_targets, make_fl_for_one_target](
          const featurelearners::FeatureLearnerParams& _params,
          const commands::FeatureLearner& _hyperparameters) {
        const auto make = std::bind(make_fl_for_one_target, _params,
                                    _hyperparameters, std::placeholders::_1);

        const auto iota = fct::iota<Int>(0, _num_targets);

        return fct::collect::vector<
            fct::Ref<featurelearners::AbstractFeatureLearner>>(
            iota | VIEWS::transform(make));
      };

  const auto to_fl = [&_feature_learner_params, make_fl_for_all_targets](
                         const commands::FeatureLearner& _hyperparameters)
      -> std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>> {
    const auto new_params =
        _feature_learner_params.replace(fct::make_field<"target_num_">(
            featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS));

    const auto new_feature_learner =
        featurelearners::FeatureLearnerParser::parse(new_params,
                                                     _hyperparameters);

    if (new_feature_learner->supports_multiple_targets()) {
      return {new_feature_learner};
    }

    return make_fl_for_all_targets(new_params, _hyperparameters);
  };

  // TODO: Get rid for from_json
  const auto obj_vector =
      json::Parser<std::vector<commands::FeatureLearner>>::from_json(
          JSON::get_array(_pipeline.obj(), "feature_learners_"));

  return fct::join::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>(
      obj_vector | VIEWS::transform(to_fl));
}

// ----------------------------------------------------------------------

std::vector<std::vector<fct::Ref<predictors::Predictor>>> Fit::init_predictors(
    const Pipeline& _pipeline, const std::string& _elem,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
    const size_t _num_targets) {
  const auto arr = JSON::get_array(_pipeline.obj(), _elem);

  std::vector<std::vector<fct::Ref<predictors::Predictor>>> predictors;

  for (size_t t = 0; t < _num_targets; ++t) {
    std::vector<fct::Ref<predictors::Predictor>> predictors_for_target;

    auto target_num = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

    target_num->set("target_num_", t);

    auto dependencies = _dependencies;

    dependencies.push_back(target_num);

    for (size_t i = 0; i < arr->size(); ++i) {
      const auto ptr = arr->getObject(i);

      if (!ptr) {
        throw std::runtime_error("Element " + std::to_string(i) + " in " +
                                 _elem +
                                 " is not a proper JSON "
                                 "object.");
      }

      auto new_predictor = predictors::PredictorParser::parse(
          *ptr, _predictor_impl, dependencies);

      predictors_for_target.emplace_back(std::move(new_predictor));
    }

    predictors.emplace_back(std::move(predictors_for_target));
  }

  return predictors;
}

// ----------------------------------------------------------------------

std::vector<fct::Ref<preprocessors::Preprocessor>> Fit::init_preprocessors(
    const Pipeline& _pipeline,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
  if (!_pipeline.obj().has("preprocessors_")) {
    return std::vector<fct::Ref<preprocessors::Preprocessor>>();
  }

  const auto arr =
      jsonutils::JSON::get_object_array(_pipeline.obj(), "preprocessors_");

  const auto parse = [&arr, &_dependencies](const size_t _i) {
    const auto ptr = arr->getObject(_i);
    assert_true(ptr);
    return preprocessors::PreprocessorParser::parse(*ptr, _dependencies);
  };

  const auto iota = fct::iota<size_t>(0, arr->size());

  auto vec = fct::collect::vector<fct::Ref<preprocessors::Preprocessor>>(
      iota | VIEWS::transform(parse));

  const auto mapping_to_end =
      [](const fct::Ref<preprocessors::Preprocessor>& _ptr) -> bool {
    return _ptr->type() != preprocessors::Preprocessor::MAPPING;
  };

  std::stable_partition(vec.begin(), vec.end(), mapping_to_end);

  // We need to take into consideration that preprocessors can also depend on
  // each other.
  // TODO
  /*auto dependencies = _dependencies;

  for (auto& p : vec) {
    const auto copy = p->clone(dependencies);
    dependencies.push_back(p->fingerprint());
    p = fct::Ref<preprocessors::Preprocessor>(copy);
  }*/

  return vec;
}

// ------------------------------------------------------------------------

fct::Ref<const predictors::PredictorImpl> Fit::make_feature_selector_impl(
    const Pipeline& _pipeline,
    const std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const containers::DataFrame& _population_df) {
  const auto blacklist = std::vector<helpers::Subrole>(
      {helpers::Subrole::exclude_predictors, helpers::Subrole::email_only,
       helpers::Subrole::substring_only});

  const auto is_not_comparison_only = [](const auto& _col) -> bool {
    return (_col.unit().find("comparison only") == std::string::npos);
  };

  const auto is_not_on_blacklist = [&blacklist](const auto& _col) -> bool {
    return !helpers::SubroleParser::contains_any(_col.subroles(), blacklist);
  };

  const auto is_null = [](const Float _val) {
    return (std::isnan(_val) || std::isinf(_val));
  };

  const auto does_not_contain_null = [is_null](const auto& _col) -> bool {
    return std::none_of(_col.begin(), _col.end(), is_null);
  };

  const auto get_name = [](const auto& _col) -> std::string {
    return _col.name();
  };

  const auto categorical_colnames =
      _pipeline.include_categorical()
          ? fct::collect::vector<std::string>(
                _population_df.categoricals() |
                VIEWS::filter(is_not_comparison_only) |
                VIEWS::filter(is_not_on_blacklist) | VIEWS::transform(get_name))
          : std::vector<std::string>();

  const auto numerical_colnames = fct::collect::vector<std::string>(
      _population_df.numericals() | VIEWS::filter(is_not_comparison_only) |
      VIEWS::filter(is_not_on_blacklist) |
      VIEWS::filter(does_not_contain_null) | VIEWS::transform(get_name));

  const auto get_num_features = [](const auto& _fl) -> size_t {
    return _fl->num_features();
  };

  const auto num_autofeatures = fct::collect::vector<size_t>(
      _feature_learners | VIEWS::transform(get_num_features));

  const auto fs_impl = fct::Ref<predictors::PredictorImpl>::make(
      num_autofeatures, categorical_colnames, numerical_colnames);

  const auto categorical_features =
      Transform::get_categorical_features(_pipeline, _population_df, *fs_impl);

  fs_impl->fit_encodings(categorical_features);

  return fs_impl;
}

// ----------------------------------------------------------------------------

std::pair<std::optional<containers::NumericalFeatures>,
          std::optional<containers::CategoricalFeatures>>
Fit::make_features_validation(const FitPredictorsParams& _params) {
  if (!_params.fit_params_.validation_df_ ||
      _params.purpose_ != TransformParams::PREDICTOR) {
    return std::make_pair(std::optional<containers::NumericalFeatures>(),
                          std::optional<containers::CategoricalFeatures>());
  }

  const auto transform_params = TransformParams{
      .categories_ = _params.fit_params_.categories_,
      .cmd_ = _params.fit_params_.cmd_,
      .data_frames_ = _params.fit_params_.data_frames_,
      .data_frame_tracker_ = _params.fit_params_.data_frame_tracker_,
      .logger_ = _params.fit_params_.logger_,
      .original_peripheral_dfs_ = _params.fit_params_.peripheral_dfs_,
      .original_population_df_ =
          *_params.fit_params_
               .validation_df_,  // NOTE: We want to take the validation_df here
      .socket_ = _params.fit_params_.socket_};

  const auto features_only_params =
      FeaturesOnlyParams{.dependencies_ = _params.dependencies_,
                         .feature_learners_ = _params.feature_learners_,
                         .pipeline_ = _params.pipeline_,
                         .preprocessors_ = _params.preprocessors_,
                         .predictor_impl_ = _params.impl_,
                         .transform_params_ = transform_params};

  const auto [numerical_features, categorical_features, _] =
      Transform::transform_features_only(features_only_params);

  return std::make_pair(
      std::make_optional<containers::NumericalFeatures>(numerical_features),
      std::make_optional<containers::CategoricalFeatures>(
          categorical_features));
}

// ------------------------------------------------------------------------

fct::Ref<const predictors::PredictorImpl> Fit::make_predictor_impl(
    const Pipeline& _pipeline, const Predictors& _feature_selectors,
    const containers::DataFrame& _population_df) {
  const auto predictor_impl =
      fct::Ref<predictors::PredictorImpl>::make(*_feature_selectors.impl_);

  if (_feature_selectors.size() == 0 || _feature_selectors.at(0).size() == 0) {
    return predictor_impl;
  }

  const auto share_selected_features =
      JSON::get_value<Float>(_pipeline.obj(), "share_selected_features_");

  if (share_selected_features <= 0.0) {
    return predictor_impl;
  }

  const auto index = calculate_importance_index(_feature_selectors);

  const auto n_selected =
      std::max(static_cast<size_t>(1),
               static_cast<size_t>(index.size() * share_selected_features));

  predictor_impl->select_features(n_selected, index);

  auto categorical_features = Transform::get_categorical_features(
      _pipeline, _population_df, *predictor_impl);

  predictor_impl->fit_encodings(categorical_features);

  return predictor_impl;
}

// ----------------------------------------------------------------------------

fct::Ref<const metrics::Scores> Fit::make_scores(
    const std::optional<MakeFeaturesParams>& _score_params,
    const Pipeline& _pipeline, const FittedPipeline& _fitted) {
  auto scores = fct::Ref<metrics::Scores>::make(_pipeline.scores());

  scores->from_json_obj(Score::column_importances_as_obj(_pipeline, _fitted));

  scores->from_json_obj(Score::feature_importances_as_obj(_fitted));

  scores->from_json_obj(Score::feature_names_as_obj(_fitted));

  if (!_score_params) {
    return scores;
  }

  return score_after_fitting(*_score_params, _pipeline.with_scores(scores),
                             _fitted);
}

// ----------------------------------------------------------------------------

std::pair<std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>,
          bool>
Fit::retrieve_predictors(
    const fct::Ref<dependency::PredTracker>& _pred_tracker,
    const std::vector<std::vector<fct::Ref<predictors::Predictor>>>&
        _predictors) {
  bool all_retrieved = true;

  auto retrieved_predictors =
      std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>();

  for (auto& vec : _predictors) {
    std::vector<std::shared_ptr<predictors::Predictor>> r;

    for (auto& p : vec) {
      const auto ptr = _pred_tracker->retrieve(p->fingerprint());

      if (!ptr) {
        all_retrieved = false;
      }

      r.push_back(ptr);
    }

    retrieved_predictors.push_back(r);
  }

  return std::make_pair(retrieved_predictors, all_retrieved);
}

// ----------------------------------------------------------------------------

fct::Ref<const metrics::Scores> Fit::score_after_fitting(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const FittedPipeline& _fitted) {
  auto [numerical_features, categorical_features, _] = Transform::make_features(
      _params, _pipeline, _fitted.feature_learners_, *_fitted.predictors_.impl_,
      _fitted.fingerprints_.fs_fingerprints_);

  categorical_features =
      _fitted.predictors_.impl_->transform_encodings(categorical_features);

  const auto yhat = Transform::generate_predictions(
      _fitted, categorical_features, numerical_features);

  const auto& name =
      fct::get<"name_">(_params.cmd_.get<"population_df_">().val_);

  return std::get<0>(
      Score::score(_pipeline, _fitted, _params.population_df_, name, yhat));
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine
