// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/fit.hpp"

#include "commands/FeatureLearner.hpp"
#include "commands/Fingerprint.hpp"
#include "communication/communication.hpp"
#include "engine/dependency/dependency.hpp"
#include "engine/pipelines/FeatureLearnerParser.hpp"
#include "engine/pipelines/FitPredictorsParams.hpp"
#include "engine/pipelines/MakeFeaturesParams.hpp"
#include "engine/pipelines/TransformParams.hpp"
#include "engine/pipelines/score.hpp"
#include "engine/pipelines/transform.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/collect.hpp"
#include "featurelearners/AbstractFeatureLearner.hpp"
#include "helpers/StringReplacer.hpp"
#include "json/json.hpp"
#include "predictors/Predictor.hpp"
#include "rfl/Field.hpp"
#include "rfl/as.hpp"
#include "rfl/make_named_tuple.hpp"
#include "rfl/replace.hpp"
#include "rfl/to_named_tuple.hpp"

namespace engine {
namespace pipelines {
namespace fit {

/// Prints the purpose in a more readable format.
std::string beautify_purpose(const std::string& _purpose);

/// Calculates an index ranking the features by importance.
std::vector<size_t> calculate_importance_index(
    const Predictors& _feature_selectors);

/// Calculates the sum of importances over all indices.
std::vector<Float> calculate_sum_importances(
    const Predictors& _feature_selectors);

/// Extracts the fingerprints from the predictors.
rfl::Ref<const std::vector<commands::Fingerprint>>
extract_predictor_fingerprints(
    const std::vector<std::vector<rfl::Ref<predictors::Predictor>>>&
        _predictors,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies);

/// Fits the feature learners. Returns the fitted feature learners and their
/// fingerprints.
std::pair<std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>,
          rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_feature_learners(
    const Pipeline& _pipeline, const FitParams& _params,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const featurelearners::FeatureLearnerParams& _feature_learner_params);

/// Fits the predictors. Returns the fitted predictors and their
/// fingerprints.
std::pair<Predictors, rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_predictors(const FitPredictorsParams& _params);

/// Fits the preprocessors and applies them to the training set.
std::pair<std::vector<rfl::Ref<const preprocessors::Preprocessor>>,
          rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_transform_preprocessors(
    const Pipeline& _pipeline, const FitPreprocessorsParams& _params,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs);

/// Retrieves the targets from the population dataframe.
std::vector<std::string> get_targets(
    const containers::DataFrame& _population_df);

/// Generates the impl for the feature selectors.
rfl::Ref<const predictors::PredictorImpl> make_feature_selector_impl(
    const Pipeline& _pipeline,
    const std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const containers::DataFrame& _population_df);

/// Generates the features for the validation.
std::pair<std::optional<containers::NumericalFeatures>,
          std::optional<containers::CategoricalFeatures>>
make_features_validation(const FitPredictorsParams& _params);

/// Generates the impl for the predictors.
rfl::Ref<const predictors::PredictorImpl> make_predictor_impl(
    const Pipeline& _pipeline, const Predictors& _feature_selectors,
    const containers::DataFrame& _population_df);

/// Generates the metrics::Scores object which is also returned by fit.
rfl::Ref<const metrics::Scores> make_scores(
    const std::optional<MakeFeaturesParams>& _score_params,
    const Pipeline& _pipeline, const FittedPipeline& _fitted);

/// Retrieves the predictors from the tracker.
std::pair<std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>,
          bool>
retrieve_predictors(
    const rfl::Ref<dependency::PredTracker>& _pred_tracker,
    const std::vector<std::vector<rfl::Ref<predictors::Predictor>>>&
        _predictors);

/// Scores the pipeline in-sample after it has been successfully fitted.
rfl::Ref<const metrics::Scores> score_after_fitting(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const FittedPipeline& _fitted);

// ------------------------------------------------------------------------

std::string beautify_purpose(const std::string& _purpose) {
  const auto purpose = helpers::StringReplacer::replace_all(_purpose, "_", " ");
  const auto len = purpose.find_last_not_of("s ") + 1;
  return purpose.substr(0, len);
}

// ------------------------------------------------------------------------

std::vector<size_t> calculate_importance_index(
    const Predictors& _feature_selectors) {
  const auto sum_importances = calculate_sum_importances(_feature_selectors);

  const auto make_pair =
      [&sum_importances](const size_t _i) -> std::pair<size_t, Float> {
    return std::make_pair(_i, sum_importances.at(_i));
  };

  const auto iota = fct::iota<size_t>(0, sum_importances.size());

  auto pairs = fct::collect::vector(iota | VIEWS::transform(make_pair));

  std::sort(
      pairs.begin(), pairs.end(),
      [](const std::pair<size_t, Float>& p1,
         const std::pair<size_t, Float>& p2) { return p1.second > p2.second; });

  return fct::collect::vector(pairs | VIEWS::keys);
}

// ------------------------------------------------------------------------

std::vector<Float> calculate_sum_importances(
    const Predictors& _feature_selectors) {
  const auto importances = score::feature_importances(_feature_selectors);

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

rfl::Ref<const std::vector<commands::Fingerprint>> extract_df_fingerprints(
    const Pipeline& _pipeline, const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) {
  const auto get_fingerprint = [](const auto& _df) {
    return _df.fingerprint();
  };

  const auto placeholder = std::vector<commands::Fingerprint>(
      {commands::Fingerprint(_pipeline.obj().data_model())});

  const auto population =
      std::vector<commands::Fingerprint>({_population_df.fingerprint()});

  const auto peripheral =
      fct::collect::vector(_peripheral_dfs | VIEWS::transform(get_fingerprint));

  return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
      fct::join::vector<commands::Fingerprint>(
          {placeholder, population, peripheral}));
}

// ----------------------------------------------------------------------

rfl::Ref<const std::vector<commands::Fingerprint>> extract_fl_fingerprints(
    const std::vector<rfl::Ref<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies) {
  if (_feature_learners.size() == 0) {
    return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
        fct::collect::vector(*_dependencies));
  }

  const auto get_fingerprint = [](const auto& _fl) {
    return _fl->fingerprint();
  };

  return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
      fct::collect::vector(_feature_learners |
                           VIEWS::transform(get_fingerprint)));
}

// ----------------------------------------------------------------------

rfl::Ref<const std::vector<commands::Fingerprint>>
extract_predictor_fingerprints(
    const std::vector<std::vector<rfl::Ref<predictors::Predictor>>>&
        _predictors,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies) {
  if (_predictors.size() == 0 || _predictors.at(0).size() == 0) {
    return _dependencies;
  }

  const auto get_fingerprint = [](const auto& _p) { return _p->fingerprint(); };

  const auto get_fingerprints = [get_fingerprint](const auto& _ps) {
    return _ps | VIEWS::transform(get_fingerprint);
  };

  return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
      fct::join::vector<commands::Fingerprint>(
          _predictors | VIEWS::transform(get_fingerprints)));
}

// ----------------------------------------------------------------------

rfl::Ref<const std::vector<commands::Fingerprint>>
extract_preprocessor_fingerprints(
    const std::vector<rfl::Ref<preprocessors::Preprocessor>>& _preprocessors,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies) {
  if (_preprocessors.size() == 0) {
    return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
        fct::collect::vector(*_dependencies));
  }

  const auto get_fingerprint = [](const auto& _fl) {
    return _fl->fingerprint();
  };

  return rfl::Ref<const std::vector<commands::Fingerprint>>::make(
      fct::collect::vector(_preprocessors | VIEWS::transform(get_fingerprint)));
}

// ----------------------------------------------------------------------

std::pair<rfl::Ref<const helpers::Schema>,
          rfl::Ref<const std::vector<helpers::Schema>>>
extract_schemata(const containers::DataFrame& _population_df,
                 const std::vector<containers::DataFrame>& _peripheral_dfs,
                 const bool _separate_discrete) {
  const auto extract_schema =
      [_separate_discrete](
          const containers::DataFrame& _df) -> helpers::Schema {
    return _df.to_schema(_separate_discrete);
  };

  const auto population_schema =
      rfl::Ref<const helpers::Schema>::make(extract_schema(_population_df));

  const auto peripheral_schema =
      rfl::Ref<const std::vector<helpers::Schema>>::make(fct::collect::vector(
          _peripheral_dfs | VIEWS::transform(extract_schema)));

  return std::make_pair(population_schema, peripheral_schema);
}

// ----------------------------------------------------------------------

std::pair<rfl::Ref<const FittedPipeline>, rfl::Ref<const metrics::Scores>> fit(
    const Pipeline& _pipeline, const FitParams& _params) {
  const auto fit_preprocessors_params =
      rfl::as<FitPreprocessorsParams>(_params);

  const auto preprocessed =
      fit_preprocessors_only(_pipeline, fit_preprocessors_params);

  const auto [population_schema, peripheral_schema] = extract_schemata(
      _params.population_df(), _params.peripheral_dfs(), false);

  const auto [modified_population_schema, modified_peripheral_schema] =
      extract_schemata(preprocessed.population_df_,
                       preprocessed.peripheral_dfs_, true);

  const auto [placeholder, peripheral] = _pipeline.make_placeholder();

  const auto feature_learner_params = featurelearners::FeatureLearnerParams(
      rfl::make_field<"dependencies_">(preprocessed.preprocessor_fingerprints_),
      rfl::make_field<"peripheral_">(peripheral),
      rfl::make_field<"peripheral_schema_">(modified_peripheral_schema),
      rfl::make_field<"placeholder_">(placeholder),
      rfl::make_field<"population_schema_">(modified_population_schema),
      rfl::make_field<"target_num_">(
          featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS));

  const auto [feature_learners, fl_fingerprints] = fit_feature_learners(
      _pipeline, _params, preprocessed.population_df_,
      preprocessed.peripheral_dfs_, feature_learner_params);

  const auto feature_selector_impl = make_feature_selector_impl(
      _pipeline, feature_learners, preprocessed.population_df_);

  containers::NumericalFeatures autofeatures;

  const auto fit_feature_selectors_params = FitPredictorsParams(
      rfl::make_field<"autofeatures_", containers::NumericalFeatures*>(
          &autofeatures),
      rfl::make_field<"dependencies_">(fl_fingerprints),
      rfl::make_field<"feature_learners_">(feature_learners),
      rfl::make_field<"fit_params_">(_params),
      rfl::make_field<"impl_">(feature_selector_impl),
      rfl::make_field<"peripheral_dfs_">(preprocessed.peripheral_dfs_),
      rfl::make_field<"pipeline_">(_pipeline),
      rfl::make_field<"population_df_">(preprocessed.population_df_),
      rfl::make_field<"preprocessors_">(preprocessed.preprocessors_),
      rfl::make_field<"preprocessor_fingerprints_">(
          preprocessed.preprocessor_fingerprints_),
      rfl::make_field<"purpose_">(Purpose::make<"feature_selectors_">()));

  const auto [feature_selectors, fs_fingerprints] =
      fit_predictors(fit_feature_selectors_params);

  const auto predictor_impl = make_predictor_impl(_pipeline, feature_selectors,
                                                  preprocessed.population_df_);

  const auto validation_fingerprint =
      _params.validation_df() ? std::vector<commands::Fingerprint>(
                                    {_params.validation_df()->fingerprint()})
                              : std::vector<commands::Fingerprint>();

  const auto dependencies =
      rfl::Ref<const std::vector<commands::Fingerprint>>::make(
          fct::join::vector<commands::Fingerprint>(
              {fct::collect::vector(*fs_fingerprints),
               validation_fingerprint}));

  const auto fit_predictors_params =
      rfl::replace(fit_feature_selectors_params,
                   rfl::make_field<"dependencies_">(dependencies),
                   rfl::make_field<"impl_">(predictor_impl),
                   rfl::make_field<"purpose_">(Purpose::make<"predictors_">()));

  const auto [predictors, _] = fit_predictors(fit_predictors_params);

  const bool score = predictors.size() > 0 && predictors.at(0).size() > 0;

  const auto score_params =
      score ? std::make_optional<MakeFeaturesParams>(
                  rfl::from_named_tuple<MakeFeaturesParams>(
                      rfl::to_named_tuple(
                          rfl::replace(_params,
                                       rfl::make_field<"peripheral_dfs_">(
                                           preprocessed.peripheral_dfs_),
                                       rfl::make_field<"population_df_">(
                                           preprocessed.population_df_))) *
                      rfl::make_field<"dependencies_">(fs_fingerprints) *
                      rfl::make_field<"original_peripheral_dfs_">(
                          _params.peripheral_dfs()) *
                      rfl::make_field<"original_population_df_">(
                          _params.population_df()) *
                      rfl::make_field<"predictor_impl_">(predictor_impl) *
                      rfl::make_field<"autofeatures_",
                                      containers::NumericalFeatures*>(
                          &autofeatures)))
            : std::nullopt;

  const auto fingerprints = Fingerprints(
      rfl::make_field<"df_fingerprints_">(preprocessed.df_fingerprints_),
      rfl::make_field<"preprocessor_fingerprints_">(
          preprocessed.preprocessor_fingerprints_),
      rfl::make_field<"fl_fingerprints_">(fl_fingerprints),
      rfl::make_field<"fs_fingerprints_">(fs_fingerprints));

  const auto fitted_pipeline =
      rfl::Ref<const FittedPipeline>::make(FittedPipeline{
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

std::pair<std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>,
          rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_feature_learners(
    const Pipeline& _pipeline, const FitParams& _params,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const featurelearners::FeatureLearnerParams& _feature_learner_params) {
  auto feature_learners = init_feature_learners(
      _pipeline, _feature_learner_params, _population_df.num_targets());

  if (feature_learners.size() == 0) {
    return std::make_pair(
        to_const(feature_learners),
        rfl::Ref<const std::vector<commands::Fingerprint>>::make(
            fct::collect::vector(
                *_feature_learner_params.get<"dependencies_">())));
  }

  const auto [modified_population_schema, modified_peripheral_schema] =
      extract_schemata(_population_df, _peripheral_dfs, true);

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);

    const auto socket_logger =
        std::make_shared<const communication::SocketLogger>(
            _params.logger(), fe->silent(), _params.socket());

    const auto fingerprint = fe->fingerprint();

    const auto retrieved_fe = _params.fe_tracker()->retrieve(fingerprint);

    if (retrieved_fe) {
      socket_logger->log(
          "Retrieving features (because a similar feature "
          "learner has already been fitted)...");

      socket_logger->log("Progress: 100%.");

      fe = rfl::Ref<featurelearners::AbstractFeatureLearner>(retrieved_fe);

      continue;
    }

    const auto params = featurelearners::FitParams(
        rfl::make_field<"cmd_">(_params.cmd()),
        rfl::make_field<"peripheral_dfs_">(_peripheral_dfs),
        rfl::make_field<"population_df_">(_population_df),
        rfl::make_field<"prefix_">(std::to_string(i + 1) + "_"),
        rfl::make_field<"socket_logger_">(socket_logger),
        rfl::make_field<"temp_dir_">(_params.categories()->temp_dir()));

    fe->fit(params);

    _params.fe_tracker()->add(fe);
  }

  const auto fl_fingerprints = extract_fl_fingerprints(
      feature_learners, _feature_learner_params.get<"dependencies_">());

  return std::make_pair(to_const(feature_learners), fl_fingerprints);
}

// ----------------------------------------------------------------------------

std::pair<Predictors, rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_predictors(const FitPredictorsParams& _params) {
  auto predictors = init_predictors(_params.pipeline(), _params.purpose(),
                                    _params.impl(), *_params.dependencies(),
                                    _params.population_df().num_targets());

  const auto [retrieved_predictors, all_retrieved] =
      retrieve_predictors(_params.fit_params().pred_tracker(), predictors);

  if (all_retrieved) {
    const auto fingerprints = extract_predictor_fingerprints(
        to_ref(retrieved_predictors), _params.dependencies());
    const auto predictors_struct =
        Predictors{.impl_ = _params.impl(),
                   .predictors_ = to_const(to_ref(retrieved_predictors))};
    return std::make_pair(predictors_struct, fingerprints);
  }

  const auto& fit_params = _params.fit_params();

  const auto make_features_params =
      MakeFeaturesParams{.categories = fit_params.categories(),
                         .cmd = fit_params.cmd(),
                         .data_frame_tracker = fit_params.data_frame_tracker(),
                         .dependencies = _params.dependencies(),
                         .logger = fit_params.logger(),
                         .original_peripheral_dfs = fit_params.peripheral_dfs(),
                         .original_population_df = fit_params.population_df(),
                         .peripheral_dfs = _params.peripheral_dfs(),
                         .population_df = _params.population_df(),
                         .predictor_impl = _params.impl(),
                         .autofeatures = _params.autofeatures(),
                         .socket = fit_params.socket()};

  auto [numerical_features, categorical_features, autofeatures] =
      transform::make_features(make_features_params, _params.pipeline(),
                               _params.feature_learners(), *_params.impl(),
                               *_params.fit_params().fs_fingerprints());

  *_params.autofeatures() = autofeatures;

  categorical_features =
      _params.impl()->transform_encodings(categorical_features);

  auto [numerical_features_valid, categorical_features_valid] =
      make_features_validation(_params);

  if (categorical_features_valid) {
    *categorical_features_valid =
        _params.impl()->transform_encodings(*categorical_features_valid);
  }

  assert_true(_params.fit_params().population_df().num_targets() ==
              predictors.size());

  assert_true(predictors.size() == retrieved_predictors.size());

  for (size_t t = 0; t < _params.fit_params().population_df().num_targets();
       ++t) {
    const auto target_col = helpers::Feature<Float>(
        _params.fit_params().population_df().target(t).data_ptr());

    const auto target_col_valid =
        numerical_features_valid
            ? std::make_optional<decltype(target_col)>(_params.fit_params()
                                                           .validation_df()
                                                           .value()
                                                           .target(t)
                                                           .to_vector_ptr())
            : std::optional<decltype(target_col)>();

    assert_true(predictors.at(t).size() == retrieved_predictors.at(t).size());

    for (size_t i = 0; i < predictors.at(t).size(); ++i) {
      auto& p = predictors.at(t).at(i);

      const auto socket_logger =
          std::make_shared<const communication::SocketLogger>(
              _params.fit_params().logger(), p->silent(),
              _params.fit_params().socket());

      if (retrieved_predictors.at(t).at(i)) {
        socket_logger->log("Retrieving predictor...");
        socket_logger->log("Progress: 100%.");
        p = rfl::Ref(retrieved_predictors.at(t).at(i));
        continue;
      }

      socket_logger->log(p->type() + ": Training as " +
                         beautify_purpose(_params.purpose().name()) + "...");

      p->fit(socket_logger, categorical_features, numerical_features,
             target_col, categorical_features_valid, numerical_features_valid,
             target_col_valid);

      _params.fit_params().pred_tracker()->add(p);
    }
  }

  const auto fingerprints =
      extract_predictor_fingerprints(predictors, _params.dependencies());

  const auto predictors_struct =
      Predictors{.impl_ = _params.impl(), .predictors_ = to_const(predictors)};

  return std::make_pair(std::move(predictors_struct), fingerprints);
}

// ----------------------------------------------------------------------

Preprocessed fit_preprocessors_only(const Pipeline& _pipeline,
                                    const FitPreprocessorsParams& _params) {
  const auto targets = get_targets(_params.population_df());

  const auto df_fingerprints = extract_df_fingerprints(
      _pipeline, _params.population_df(), _params.peripheral_dfs());

  auto [population_df, peripheral_dfs] = transform::stage_data_frames(
      _pipeline, _params.population_df(), _params.peripheral_dfs(),
      _params.logger(), _params.categories()->temp_dir(), _params.socket());

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

std::pair<std::vector<rfl::Ref<const preprocessors::Preprocessor>>,
          rfl::Ref<const std::vector<commands::Fingerprint>>>
fit_transform_preprocessors(
    const Pipeline& _pipeline, const FitPreprocessorsParams& _params,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs) {
  auto preprocessors = init_preprocessors(_pipeline, _dependencies);

  if (preprocessors.size() == 0) {
    return std::make_pair(
        to_const(preprocessors),
        rfl::Ref<const std::vector<commands::Fingerprint>>::make(
            fct::collect::vector(*_dependencies)));
  }

  const auto [placeholder, peripheral_names] = _pipeline.make_placeholder();

  const auto socket_logger =
      _params.logger() ? std::make_shared<const communication::SocketLogger>(
                             _params.logger(), true, _params.socket())
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

    const auto fingerprint = p->fingerprint();

    const auto retrieved_preprocessor =
        _params.preprocessor_tracker()->retrieve(fingerprint);

    const auto params = preprocessors::Params{
        .categories = _params.categories(),
        .cmd = _params.cmd(),
        .logger = socket_logger,
        .logging_begin = (i * 100) / preprocessors.size(),
        .logging_end = ((i + 1) * 100) / preprocessors.size(),
        .peripheral_dfs = *_peripheral_dfs,
        .peripheral_names = *peripheral_names,
        .placeholder = *placeholder,
        .population_df = *_population_df};

    if (retrieved_preprocessor) {
      p = rfl::Ref<preprocessors::Preprocessor>(retrieved_preprocessor);
      std::tie(*_population_df, *_peripheral_dfs) = p->transform(params);
      continue;
    }

    std::tie(*_population_df, *_peripheral_dfs) = p->fit_transform(params);

    _params.preprocessor_tracker()->add(p);
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

std::vector<std::string> get_targets(
    const containers::DataFrame& _population_df) {
  const auto get_target = [](const auto& _col) -> std::string {
    return _col.name();
  };
  return fct::collect::vector(_population_df.targets() |
                              VIEWS::transform(get_target));
}

// ------------------------------------------------------------------------

std::vector<rfl::Ref<featurelearners::AbstractFeatureLearner>>
init_feature_learners(
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
      -> rfl::Ref<featurelearners::AbstractFeatureLearner> {
    const auto new_params =
        _params.replace(rfl::make_field<"target_num_">(_target_num));

    return FeatureLearnerParser::parse(new_params, _hyperparameters);
  };

  const auto make_fl_for_all_targets =
      [_num_targets, make_fl_for_one_target](
          const featurelearners::FeatureLearnerParams& _params,
          const commands::FeatureLearner& _hyperparameters) {
        const auto make = std::bind(make_fl_for_one_target, _params,
                                    _hyperparameters, std::placeholders::_1);

        const auto iota = fct::iota<Int>(0, _num_targets);

        return fct::collect::vector(iota | VIEWS::transform(make));
      };

  const auto to_fl = [&_feature_learner_params, make_fl_for_all_targets](
                         const commands::FeatureLearner& _hyperparameters)
      -> std::vector<rfl::Ref<featurelearners::AbstractFeatureLearner>> {
    const auto new_params =
        _feature_learner_params.replace(rfl::make_field<"target_num_">(
            featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS));

    const auto new_feature_learner =
        FeatureLearnerParser::parse(new_params, _hyperparameters);

    if (new_feature_learner->supports_multiple_targets()) {
      return {new_feature_learner};
    }

    return make_fl_for_all_targets(new_params, _hyperparameters);
  };

  const auto obj_vector = _pipeline.obj().feature_learners();

  return fct::join::vector<rfl::Ref<featurelearners::AbstractFeatureLearner>>(
      obj_vector | VIEWS::transform(to_fl));
}

// ----------------------------------------------------------------------

std::vector<std::vector<rfl::Ref<predictors::Predictor>>> init_predictors(
    const Pipeline& _pipeline, const Purpose _purpose,
    const rfl::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const std::vector<commands::Fingerprint>& _dependencies,
    const size_t _num_targets) {
  const auto commands = _purpose.value() == Purpose::value_of<"predictors_">()
                            ? _pipeline.obj().predictors()
                            : _pipeline.obj().feature_selectors();

  std::vector<std::vector<rfl::Ref<predictors::Predictor>>> predictors;

  for (size_t t = 0; t < _num_targets; ++t) {
    std::vector<rfl::Ref<predictors::Predictor>> predictors_for_target;

    using TargetNumber = typename commands::Fingerprint::TargetNumber;

    const auto target_num = TargetNumber(rfl::make_field<"target_num_">(t));

    auto dependencies = _dependencies;

    dependencies.push_back(commands::Fingerprint(target_num));

    for (const auto& cmd : commands) {
      auto new_predictor = predictors::PredictorParser::parse(
          cmd, _predictor_impl, dependencies);
      predictors_for_target.emplace_back(std::move(new_predictor));
    }

    predictors.emplace_back(std::move(predictors_for_target));
  }

  return predictors;
}

// ----------------------------------------------------------------------

std::vector<rfl::Ref<preprocessors::Preprocessor>> init_preprocessors(
    const Pipeline& _pipeline,
    const rfl::Ref<const std::vector<commands::Fingerprint>>& _dependencies) {
  auto dependencies = fct::collect::vector(*_dependencies);

  const auto parse = [&dependencies](const auto& _cmd) {
    return preprocessors::PreprocessorParser::parse(_cmd.val_, dependencies);
  };

  const auto commands = _pipeline.obj().preprocessors();

  auto vec = fct::collect::vector(commands | VIEWS::transform(parse));

  const auto mapping_to_end = [](const auto& _p) -> bool {
    return _p->type() != preprocessors::Preprocessor::MAPPING;
  };

  std::stable_partition(vec.begin(), vec.end(), mapping_to_end);

  // We need to take into consideration that preprocessors can also depend on
  // each other.
  for (auto& p : vec) {
    const auto copy = p->clone(dependencies);
    dependencies.push_back(p->fingerprint());
    p = rfl::Ref<preprocessors::Preprocessor>(copy);
  }

  return vec;
}

// ------------------------------------------------------------------------

rfl::Ref<const predictors::PredictorImpl> make_feature_selector_impl(
    const Pipeline& _pipeline,
    const std::vector<rfl::Ref<const featurelearners::AbstractFeatureLearner>>&
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
          ? fct::collect::vector(_population_df.categoricals() |
                                 VIEWS::filter(is_not_comparison_only) |
                                 VIEWS::filter(is_not_on_blacklist) |
                                 VIEWS::transform(get_name))
          : std::vector<std::string>();

  const auto numerical_colnames = fct::collect::vector(
      _population_df.numericals() | VIEWS::filter(is_not_comparison_only) |
      VIEWS::filter(is_not_on_blacklist) |
      VIEWS::filter(does_not_contain_null) | VIEWS::transform(get_name));

  const auto get_num_features = [](const auto& _fl) -> size_t {
    return _fl->num_features();
  };

  const auto num_autofeatures = fct::collect::vector(
      _feature_learners | VIEWS::transform(get_num_features));

  const auto fs_impl = rfl::Ref<predictors::PredictorImpl>::make(
      num_autofeatures, categorical_colnames, numerical_colnames);

  const auto categorical_features =
      transform::get_categorical_features(_pipeline, _population_df, *fs_impl);

  fs_impl->fit_encodings(categorical_features);

  return fs_impl;
}

// ----------------------------------------------------------------------------

std::pair<std::optional<containers::NumericalFeatures>,
          std::optional<containers::CategoricalFeatures>>
make_features_validation(const FitPredictorsParams& _params) {
  if (!_params.fit_params().validation_df() ||
      _params.purpose().value() == Purpose::value_of<"feature_selectors_">()) {
    return std::make_pair(std::optional<containers::NumericalFeatures>(),
                          std::optional<containers::CategoricalFeatures>());
  }

  const auto transform_params = TransformParams{
      .categories = _params.fit_params().categories(),
      .cmd = TransformParams::Cmd{.data_frames_or_views =
                                      _params.fit_params().cmd(),
                                  .predict = false,
                                  .score = false},
      .data_frames = _params.fit_params().data_frames(),
      .data_frame_tracker = _params.fit_params().data_frame_tracker(),
      .logger = _params.fit_params().logger(),
      .original_peripheral_dfs = _params.fit_params().peripheral_dfs(),
      .original_population_df =
          *_params.fit_params().validation_df(),  // NOTE: We want to take the
                                                  // validation_df here
      .socket = _params.fit_params().socket()};

  const auto features_only_params = FeaturesOnlyParams{
      .dependencies = _params.dependencies(),
      .feature_learners = _params.feature_learners(),
      .fs_fingerprints = _params.fit_params().fs_fingerprints(),
      .pipeline = _params.pipeline(),
      .preprocessors = _params.preprocessors(),
      .predictor_impl = _params.impl(),
      .transform_params = transform_params};

  const auto [numerical_features, categorical_features, _] =
      transform::transform_features_only(features_only_params);

  return std::make_pair(
      std::make_optional<containers::NumericalFeatures>(numerical_features),
      std::make_optional<containers::CategoricalFeatures>(
          categorical_features));
}

// ------------------------------------------------------------------------

rfl::Ref<const predictors::PredictorImpl> make_predictor_impl(
    const Pipeline& _pipeline, const Predictors& _feature_selectors,
    const containers::DataFrame& _population_df) {
  const auto predictor_impl =
      rfl::Ref<predictors::PredictorImpl>::make(*_feature_selectors.impl_);

  if (_feature_selectors.size() == 0 || _feature_selectors.at(0).size() == 0) {
    return predictor_impl;
  }

  const auto share_selected_features =
      _pipeline.obj().share_selected_features();

  if (share_selected_features <= 0.0) {
    return predictor_impl;
  }

  const auto index = calculate_importance_index(_feature_selectors);

  const auto n_selected =
      std::max(static_cast<size_t>(1),
               static_cast<size_t>(index.size() * share_selected_features));

  predictor_impl->select_features(n_selected, index);

  auto categorical_features = transform::get_categorical_features(
      _pipeline, _population_df, *predictor_impl);

  predictor_impl->fit_encodings(categorical_features);

  return predictor_impl;
}

// ----------------------------------------------------------------------------

rfl::Ref<const metrics::Scores> make_scores(
    const std::optional<MakeFeaturesParams>& _score_params,
    const Pipeline& _pipeline, const FittedPipeline& _fitted) {
  auto scores = rfl::Ref<metrics::Scores>::make(_pipeline.scores());

  const auto [c_desc, c_importances] =
      score::column_importances(_pipeline, _fitted);

  const auto feature_importances =
      score::feature_importances(_fitted.predictors_);

  const auto [n1, n2, n3] = _fitted.feature_names();

  const auto feature_names = fct::join::vector<std::string>({n1, n2, n3});

  scores->update(
      rfl::make_field<"column_descriptions_">(c_desc),
      rfl::make_field<"column_importances_">(score::transpose(c_importances)),
      rfl::make_field<"feature_importances_">(
          score::transpose(feature_importances)),
      rfl::make_field<"feature_names_">(feature_names));

  if (!_score_params) {
    return scores;
  }

  return score_after_fitting(*_score_params, _pipeline.with_scores(scores),
                             _fitted);
}

// ----------------------------------------------------------------------------

std::pair<std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>,
          bool>
retrieve_predictors(
    const rfl::Ref<dependency::PredTracker>& _pred_tracker,
    const std::vector<std::vector<rfl::Ref<predictors::Predictor>>>&
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

rfl::Ref<const metrics::Scores> score_after_fitting(
    const MakeFeaturesParams& _params, const Pipeline& _pipeline,
    const FittedPipeline& _fitted) {
  auto [numerical_features, categorical_features, _] = transform::make_features(
      _params, _pipeline, _fitted.feature_learners_, *_fitted.predictors_.impl_,
      *_fitted.fingerprints_.fs_fingerprints());

  categorical_features =
      _fitted.predictors_.impl_->transform_encodings(categorical_features);

  const auto yhat = transform::generate_predictions(
      _fitted, categorical_features, numerical_features);

  const auto get_name = [](const auto& _c) -> const auto& { return _c.name(); };

  const auto& name = rfl::visit(get_name, _params.cmd().population_df().val_);

  return score::score(_pipeline, _fitted, _params.population_df(), name, yhat);
}

}  // namespace fit
}  // namespace pipelines
}  // namespace engine
