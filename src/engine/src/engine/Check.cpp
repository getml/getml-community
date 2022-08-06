// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/pipelines/Check.hpp"

// ----------------------------------------------------------------------------

#include "fct/collect.hpp"
#include "jsonutils/jsonutils.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/Fit.hpp"
#include "engine/pipelines/FitPreprocessorsParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

void Check::check(const Pipeline& _pipeline, const CheckParams& _params) {
  // TODO: We are forced to generate the modified tables, even when we can
  // retrieve the check, but we really only need the modified schemata at this
  // point. Fix this.
  const auto fit_preprocessors_params = FitPreprocessorsParams{
      .categories_ = _params.categories_,
      .cmd_ = _params.cmd_,
      .logger_ = _params.logger_,
      .peripheral_dfs_ = _params.peripheral_dfs_,
      .population_df_ = _params.population_df_,
      .preprocessor_tracker_ = _params.preprocessor_tracker_,
      .socket_ = _params.socket_};

  const auto preprocessed =
      Fit::fit_preprocessors_only(_pipeline, fit_preprocessors_params);

  const auto [modified_population_schema, modified_peripheral_schema] =
      Fit::extract_schemata(preprocessed.population_df_,
                            preprocessed.peripheral_dfs_, true);

  const auto [placeholder, peripheral_names] = _pipeline.make_placeholder();

  const auto feature_learner_params = featurelearners::FeatureLearnerParams{
      .cmd_ = Poco::JSON::Object(),
      .dependencies_ = preprocessed.preprocessor_fingerprints_,
      .peripheral_ = peripheral_names.ptr(),  // TODO
      .peripheral_schema_ = modified_peripheral_schema.ptr(),
      .placeholder_ = placeholder.ptr(),  // TODO
      .population_schema_ = modified_population_schema.ptr(),
      .target_num_ = featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS};

  const auto [feature_learners, fl_fingerprints] =
      init_feature_learners(_pipeline, feature_learner_params, _params);

  const auto warning_fingerprint = make_warning_fingerprint(fl_fingerprints);

  const auto retrieved =
      _params.warning_tracker_->retrieve(warning_fingerprint);

  if (retrieved) {
    communication::Sender::send_string("Success!", _params.socket_);
    retrieved->send(_params.socket_);
    return;
  }

  const auto socket_logger =
      _params.logger_ ? std::make_shared<const communication::SocketLogger>(
                            _params.logger_, true, _params.socket_)
                      : std::shared_ptr<const communication::SocketLogger>();

  // TODO: Use fct::Ref
  const auto to_ptr = [](const auto& _fl) { return _fl.ptr(); };

  const auto fl_shared_ptr = fct::collect::vector<
      std::shared_ptr<featurelearners::AbstractFeatureLearner>>(
      feature_learners | VIEWS::transform(to_ptr));

  const auto warner = preprocessors::DataModelChecker::check(
      placeholder.ptr(), peripheral_names.ptr(), preprocessed.population_df_,
      preprocessed.peripheral_dfs_, fl_shared_ptr, socket_logger);

  communication::Sender::send_string("Success!", _params.socket_);

  const auto warnings = warner.to_warnings_obj(warning_fingerprint);

  warnings->send(_params.socket_);

  _params.warning_tracker_->add(warnings);
}

// ----------------------------------------------------------------------------

std::pair<std::vector<fct::Ref<featurelearners::AbstractFeatureLearner>>,
          std::vector<Poco::JSON::Object::Ptr>>
Check::init_feature_learners(
    const Pipeline& _pipeline,
    const featurelearners::FeatureLearnerParams& _feature_learner_params,
    const CheckParams& _params) {
  const auto df_fingerprints = Fit::extract_df_fingerprints(
      _pipeline, _params.population_df_, _params.peripheral_dfs_);

  const auto preprocessors =
      Fit::init_preprocessors(_pipeline, df_fingerprints);

  const auto preprocessor_fingerprints =
      Fit::extract_preprocessor_fingerprints(preprocessors, df_fingerprints);

  const auto feature_learners = Fit::init_feature_learners(
      _pipeline, _feature_learner_params, _params.population_df_.num_targets());

  return std::make_pair(feature_learners,
                        Fit::extract_fl_fingerprints(
                            feature_learners, preprocessor_fingerprints));
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Check::make_warning_fingerprint(
    const std::vector<Poco::JSON::Object::Ptr>& _fl_fingerprints) {
  auto arr = jsonutils::JSON::vector_to_array_ptr(_fl_fingerprints);
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());
  obj->set("fl_fingerprints_", arr);
  return obj;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
