#include "engine/pipelines/Load.hpp"

#include "engine/pipelines/Fit.hpp"
#include "engine/pipelines/FittedPipeline.hpp"

namespace engine {
namespace pipelines {

Pipeline Load::load(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const std::shared_ptr<dependency::PreprocessorTracker>
        _preprocessor_tracker) {
  assert_true(_fe_tracker);
  assert_true(_pred_tracker);
  assert_true(_preprocessor_tracker);

  const auto obj = load_json_obj(_path + "obj.json");

  const auto scores = fct::Ref<const metrics::Scores>::make(
      load_json_obj(_path + "scores.json"));

  const auto pipeline_json = load_pipeline_json(_path);

  const auto pipeline = Pipeline(obj).with_scores(scores).with_creation_time(
      pipeline_json.creation_time_);

  const auto [feature_selector_impl, predictor_impl] = load_impls(_path);

  const auto preprocessors = load_preprocessors(
      _path, _preprocessor_tracker, obj, pipeline_json.fingerprints_);

  const auto feature_learners =
      load_feature_learners(_path, _fe_tracker, pipeline_json, pipeline);

  const auto feature_selectors = load_feature_selectors(
      _path, _pred_tracker, feature_selector_impl, pipeline_json, pipeline);

  const auto predictors = load_predictors(_path, _pred_tracker, predictor_impl,
                                          pipeline_json, pipeline);

  const auto fitted =
      fct::Ref<const pipelines::FittedPipeline>::make(pipelines::FittedPipeline{
          .feature_learners_ = feature_learners,
          .feature_selectors_ = feature_selectors,
          .fingerprints_ = pipeline_json.fingerprints_,
          .modified_peripheral_schema_ =
              pipeline_json.modified_peripheral_schema_,
          .modified_population_schema_ =
              pipeline_json.modified_population_schema_,
          .peripheral_schema_ = pipeline_json.peripheral_schema_,
          .population_schema_ = pipeline_json.population_schema_,
          .predictors_ = predictors,
          .preprocessors_ = preprocessors});

  return pipeline.with_allow_http(pipeline_json.allow_http_)
      .with_fitted(fitted);
}

// ------------------------------------------------------------------------

std::vector<fct::Ref<const featurelearners::AbstractFeatureLearner>>
Load::load_feature_learners(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  assert_true(_fe_tracker);

  const auto [placeholder, peripheral] = _pipeline.make_placeholder();

  const auto feature_learner_params = featurelearners::FeatureLearnerParams{
      .cmd_ = Poco::JSON::Object(),
      .dependencies_ = _pipeline_json.fingerprints_.preprocessor_fingerprints_,
      .peripheral_ = peripheral.ptr(),  // TODO
      .peripheral_schema_ = _pipeline_json.modified_peripheral_schema_.ptr(),
      .placeholder_ = placeholder.ptr(),  // TODO
      .population_schema_ = _pipeline_json.modified_population_schema_.ptr(),
      .target_num_ = featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS};

  const auto feature_learners = Fit::init_feature_learners(
      _pipeline, feature_learner_params, _pipeline_json.targets_.size());

  for (size_t i = 0; i < feature_learners.size(); ++i) {
    auto& fe = feature_learners.at(i);
    fe->load(_path + "feature-learner-" + std::to_string(i) + ".json");
    _fe_tracker->add(fe);
  }

  return Fit::to_const(feature_learners);
}

// ------------------------------------------------------------------------

Predictors Load::load_feature_selectors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _feature_selector_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto feature_selectors = Fit::init_predictors(
      _pipeline, "feature_selectors_", _feature_selector_impl,
      _pipeline_json.fingerprints_.fl_fingerprints_,
      _pipeline_json.targets_.size());

  for (size_t i = 0; i < feature_selectors.size(); ++i) {
    for (size_t j = 0; j < feature_selectors.at(i).size(); ++j) {
      const auto& p = feature_selectors.at(i).at(j);
      p->load(_path + "feature-selector-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _feature_selector_impl,
                    .predictors_ = Fit::to_const(feature_selectors)};
}

// ------------------------------------------------------------------------

Fingerprints Load::load_fingerprints(const Poco::JSON::Object& _pipeline_json) {
  const auto df_fingerprints = JSON::array_to_obj_vector(
      JSON::get_array(_pipeline_json, "df_fingerprints_"));

  const auto fl_fingerprints = JSON::array_to_obj_vector(
      JSON::get_array(_pipeline_json, "fl_fingerprints_"));

  const auto fs_fingerprints = JSON::array_to_obj_vector(
      JSON::get_array(_pipeline_json, "fs_fingerprints_"));

  const auto preprocessor_fingerprints = JSON::array_to_obj_vector(
      JSON::get_array(_pipeline_json, "preprocessor_fingerprints_"));

  return Fingerprints{
      .df_fingerprints_ = df_fingerprints,
      .fl_fingerprints_ = fl_fingerprints,
      .fs_fingerprints_ = fs_fingerprints,
      .preprocessor_fingerprints_ = preprocessor_fingerprints,
  };
}

// ------------------------------------------------------------------------

std::pair<fct::Ref<const predictors::PredictorImpl>,
          fct::Ref<const predictors::PredictorImpl>>
Load::load_impls(const std::string& _path) {
  const auto feature_selector_impl =
      fct::Ref<const predictors::PredictorImpl>::make(
          load_json_obj(_path + "feature-selector-impl.json"));

  const auto predictor_impl = fct::Ref<predictors::PredictorImpl>::make(
      load_json_obj(_path + "predictor-impl.json"));

  return std::make_pair(feature_selector_impl, predictor_impl);
}

// ------------------------------------------------------------------------

Poco::JSON::Object Load::load_json_obj(const std::string& _fname) {
  std::ifstream input(_fname);

  std::stringstream json;

  std::string line;

  if (input.is_open()) {
    while (std::getline(input, line)) {
      json << line;
    }

    input.close();
  } else {
    throw std::runtime_error("File '" + _fname + "' not found!");
  }

  const auto ptr =
      Poco::JSON::Parser().parse(json.str()).extract<Poco::JSON::Object::Ptr>();

  if (!ptr) {
    throw std::runtime_error("JSON file did not contain an object!");
  }

  return *ptr;
}

// ----------------------------------------------------------------------------

PipelineJSON Load::load_pipeline_json(const std::string& _path) {
  const auto to_schema =
      [](const Poco::JSON::Object::Ptr& _obj) -> helpers::Schema {
    assert_true(_obj);
    return helpers::Schema::from_json(*_obj);
  };

  const auto pipeline_json = load_json_obj(_path + "pipeline.json");

  const auto get_peripheral_schema = [&pipeline_json,
                                      to_schema](const std::string& _name) {
    auto arr = JSON::get_array(pipeline_json, _name);
    const auto vec = JSON::array_to_obj_vector(arr);
    return fct::Ref<const std::vector<helpers::Schema>>::make(
        fct::collect::vector<helpers::Schema>(vec |
                                              VIEWS::transform(to_schema)));
  };

  const bool allow_http = JSON::get_value<bool>(pipeline_json, "allow_http_");

  const auto creation_time =
      JSON::get_value<std::string>(pipeline_json, "creation_time_");

  const auto modified_peripheral_schema =
      get_peripheral_schema("modified_peripheral_schema_");

  const auto modified_population_schema =
      fct::Ref<const helpers::Schema>::make(helpers::Schema::from_json(
          *JSON::get_object(pipeline_json, "modified_population_schema_")));

  const auto peripheral_schema = get_peripheral_schema("peripheral_schema_");

  const auto population_schema =
      fct::Ref<const helpers::Schema>::make(helpers::Schema::from_json(
          *JSON::get_object(pipeline_json, "population_schema_")));

  const auto targets = JSON::array_to_vector<std::string>(
      JSON::get_array(pipeline_json, "targets_"));

  const auto fingerprints = load_fingerprints(pipeline_json);

  return PipelineJSON{.allow_http_ = allow_http,
                      .creation_time_ = creation_time,
                      .fingerprints_ = fingerprints,
                      .modified_peripheral_schema_ = modified_peripheral_schema,
                      .modified_population_schema_ = modified_population_schema,
                      .peripheral_schema_ = peripheral_schema,
                      .population_schema_ = population_schema,
                      .targets_ = targets};
}

// ------------------------------------------------------------------------

Predictors Load::load_predictors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const fct::Ref<const predictors::PredictorImpl>& _predictor_impl,
    const PipelineJSON& _pipeline_json, const Pipeline& _pipeline) {
  const auto predictors =
      Fit::init_predictors(_pipeline, "predictors_", _predictor_impl,
                           _pipeline_json.fingerprints_.fs_fingerprints_,
                           _pipeline_json.targets_.size());

  for (size_t i = 0; i < predictors.size(); ++i) {
    for (size_t j = 0; j < predictors.at(i).size(); ++j) {
      const auto& p = predictors.at(i).at(j);
      p->load(_path + "predictor-" + std::to_string(i) + "-" +
              std::to_string(j));
      _pred_tracker->add(p);
    }
  }

  return Predictors{.impl_ = _predictor_impl,
                    .predictors_ = Fit::to_const(predictors)};
}

// ------------------------------------------------------------------------

std::vector<fct::Ref<const preprocessors::Preprocessor>>
Load::load_preprocessors(const std::string& _path,
                         const std::shared_ptr<dependency::PreprocessorTracker>
                             _preprocessor_tracker,
                         const Poco::JSON::Object& _obj,
                         const Fingerprints& _fingerprints) {
  assert_true(_preprocessor_tracker);

  if (!_obj.has("preprocessors_")) {
    return {};
  }

  auto preprocessors =
      std::vector<fct::Ref<const preprocessors::Preprocessor>>();

  const auto arr = jsonutils::JSON::get_object_array(_obj, "preprocessors_");

  for (size_t i = 0; i < arr->size(); ++i) {
    const auto json_obj =
        load_json_obj(_path + "preprocessor-" + std::to_string(i) + ".json");

    const auto p = preprocessors::PreprocessorParser::parse(
        json_obj, _fingerprints.df_fingerprints_);

    preprocessors.push_back(p);

    _preprocessor_tracker->add(p);
  }

  return preprocessors;
}

// ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

