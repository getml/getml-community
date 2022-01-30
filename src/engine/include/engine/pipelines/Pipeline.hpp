#ifndef ENGINE_PIPELINES_PIPELINE_HPP_
#define ENGINE_PIPELINES_PIPELINE_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/TemporaryFile.h>

// ----------------------------------------------------------------------------

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/PipelineImpl.hpp"
#include "engine/pipelines/TransformParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {
// ----------------------------------------------------------------------------

class Pipeline {
  // --------------------------------------------------------

 public:
  Pipeline(const Poco::JSON::Object& _obj);

  Pipeline(const std::string& _path,
           const std::shared_ptr<dependency::FETracker> _fe_tracker,
           const std::shared_ptr<dependency::PredTracker> _pred_tracker,
           const std::shared_ptr<dependency::PreprocessorTracker>
               _preprocessor_tracker);

  Pipeline(const Pipeline& _other);

  Pipeline(Pipeline&& _other) noexcept;

  ~Pipeline();

  // --------------------------------------------------------

 public:
  /// Returns the feature names.
  std::tuple<std::vector<std::string>, std::vector<std::string>,
             std::vector<std::string>>
  feature_names() const;

  /// Checks the validity of the data model.
  void check(const CheckParams& _params) const;

  /// Fit the pipeline.
  void fit(const FitParams& _params);

  /// Copy assignment operator.
  Pipeline& operator=(const Pipeline& _other);

  /// Move assignment operator.
  Pipeline& operator=(Pipeline&& _other) noexcept;

  /// Save the pipeline to disk.
  void save(const helpers::StringIterator& _categories,
            const std::string& _temp_dir, const std::string& _path,
            const std::string& _name) const;

  /// Score the pipeline.
  Poco::JSON::Object score(const containers::DataFrame& _population_df,
                           const std::string& _population_name,
                           const containers::NumericalFeatures& _yhat);

  /// Generate features and predictions.
  std::pair<containers::NumericalFeatures, containers::CategoricalFeatures>
  transform(const TransformParams& _params);

  /// Expresses the Pipeline in a form the monitor can understand.
  Poco::JSON::Object to_monitor(const helpers::StringIterator& _categories,
                                const std::string& _name) const;

  /// Express features as SQL code
  std::string to_sql(const helpers::StringIterator& _categories,
                     const bool _targets, const bool _full_pipeline,
                     const std::string& _dialect) const;

  // --------------------------------------------------------

 public:
  /// Trivial accessor
  bool& allow_http() { return impl_.allow_http_; }

  /// Trivial (const) accessor
  bool allow_http() const { return impl_.allow_http_; }

  /// Returns the pipeline's complete dependency graph
  std::vector<Poco::JSON::Object::Ptr> dependencies() const {
    return fs_fingerprints();
  }

  /// Trivial (const) accessor
  const Poco::JSON::Object& obj() const { return impl_.obj_; }

  /// Whether the pipeline contains any premium_only feature learners
  bool premium_only() const {
    return std::any_of(
        feature_learners_.begin(), feature_learners_.end(),
        [](const std::shared_ptr<featurelearners::AbstractFeatureLearner>& fe) {
          assert_true(fe);
          return fe->premium_only();
        });
  }

  /// Trivial (const) accessor
  const metrics::Scores& scores() const { return impl_.scores_; }

  /// Writes a JSON object to disc.
  void save_json_obj(const Poco::JSON::Object& _obj,
                     const std::string& _path) const {
    std::ofstream fs(_path, std::ofstream::out);
    Poco::JSON::Stringifier::stringify(_obj, fs);
    fs.close();
  }

  /// Trivial (const) accessor
  const std::vector<std::string>& targets() const { return impl_.targets_; }

  // --------------------------------------------------------

 private:
  /// Add all numerical columns in the population table that
  /// haven't been explicitly marked "comparison only".
  void add_population_cols(const Poco::JSON::Object& _cmd,
                           const containers::DataFrame& _population_df,
                           const predictors::PredictorImpl& _predictor_impl,
                           containers::NumericalFeatures* _features) const;

  /// Applies the fitted preprocessors.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  apply_preprocessors(
      const Poco::JSON::Object& _cmd,
      const std::shared_ptr<const communication::Logger>& _logger,
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs,
      const std::shared_ptr<const containers::Encoding>& _categories,
      Poco::Net::StreamSocket* _socket) const;

  /// Returns the feature names.
  std::vector<std::string> autofeature_names() const;

  /// Calculates an index ordering the features by importance.
  std::vector<size_t> calculate_importance_index() const;

  /// Calculates the sum of the feature importances for all targets.
  std::vector<Float> calculate_sum_importances() const;

  /// Calculates the feature statistics to be displayed in the monitor.
  void calculate_feature_stats(const containers::NumericalFeatures _features,
                               const size_t _nrows, const size_t _ncols,
                               const Poco::JSON::Object& _cmd,
                               const containers::DataFrame& _population_df);

  /// Calculates the column importances.
  std::pair<std::vector<helpers::ColumnDescription>,
            std::vector<std::vector<Float>>>
  column_importances() const;

  /// Returns a JSON object containing all column importances.
  Poco::JSON::Object column_importances_as_obj() const;

  /// Calculates the column importances for the autofeatures.
  void column_importances_auto(
      const std::vector<std::vector<Float>>& _f_importances,
      std::vector<helpers::ImportanceMaker>* _importance_makers) const;

  /// Calculates the column importances for the manual features.
  void column_importances_manual(
      const std::vector<std::vector<Float>>& _f_importances,
      std::vector<helpers::ImportanceMaker>* _importance_makers) const;

  /// Extracts column names from the column importances.
  void extract_coldesc(
      const std::map<helpers::ColumnDescription, Float>& _column_importances,
      std::vector<helpers::ColumnDescription>* _coldesc) const;

  /// Extracts the fingerprints of the data frames.
  std::vector<Poco::JSON::Object::Ptr> extract_df_fingerprints(
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs) const;

  /// Extracts the fingerprints of the feature learners.
  std::vector<Poco::JSON::Object::Ptr> extract_fl_fingerprints(
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
          _feature_learners) const;

  /// Extracts fingerprints from feature selectors or predictors.
  std::vector<Poco::JSON::Object::Ptr> extract_fingerprints(
      const std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>&
          _predictors) const;

  /// Extracts the fingerprints of the feature selectors.
  std::vector<Poco::JSON::Object::Ptr> extract_fs_fingerprints() const;

  /// Extracts the importance values from the column importances.
  void extract_importance_values(
      const std::map<helpers::ColumnDescription, Float>& _column_importances,
      std::vector<std::vector<Float>>* _all_column_importances) const;

  /// Extracts the fingerprints of the preprocessors.
  std::vector<Poco::JSON::Object::Ptr> extract_preprocessor_fingerprints(
      const std::vector<std::shared_ptr<preprocessors::Preprocessor>>&
          _preprocessors) const;

  /// Extracts the schemata as they are inserted into the feature learners.
  std::pair<std::shared_ptr<const helpers::Schema>,
            std::shared_ptr<const std::vector<helpers::Schema>>>
  extract_modified_schemata(
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs) const;

  /// Extracts the schemata from the data frame used for training.
  std::pair<std::shared_ptr<const helpers::Schema>,
            std::shared_ptr<const std::vector<helpers::Schema>>>
  extract_schemata(
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs) const;

  /// Fits the feature learning algorithms.
  void fit_feature_learners(
      const FitParams& _params, const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs);

  /// Fits the predictors.
  void fit_predictors(const TransformParams& _params);

  /// Fits the preprocessors. Returns a map of transformed data frames.
  std::vector<std::shared_ptr<preprocessors::Preprocessor>>
  fit_transform_preprocessors(
      const Poco::JSON::Object& _cmd,
      const std::shared_ptr<const communication::Logger>& _logger,
      const std::shared_ptr<containers::Encoding>& _categories,
      const std::shared_ptr<dependency::PreprocessorTracker>&
          _preprocessor_tracker,
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
      containers::DataFrame* _population_df,
      std::vector<containers::DataFrame>* _peripheral_dfs,
      Poco::Net::StreamSocket* _socket) const;

  /// Expresses the feature learners as SQL code.
  std::vector<std::string> feature_learners_to_sql(
      const helpers::StringIterator& _categories, const bool _targets,
      const bool _subfeatures,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const;

  /// Calculates the feature importances vis-a-vis each target.
  std::vector<std::vector<Float>> feature_importances(
      const std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>&
          _predictors) const;

  /// Returns a JSON object containing all feature importances.
  Poco::JSON::Object feature_importances_as_obj() const;

  /// Returns a JSON object containing all feature names.
  Poco::JSON::Object feature_names_as_obj() const;

  /// Fills up the values of the importance makers.
  void fill_zeros(std::vector<helpers::ImportanceMaker>* _f_importances) const;

  /// Generate the autofeatures.
  containers::NumericalFeatures generate_autofeatures(
      const TransformParams& _params) const;

  /// Generates the predictions based on the features.
  containers::NumericalFeatures generate_predictions(
      const containers::CategoricalFeatures& _categorical_features,
      const containers::NumericalFeatures& _numerical_features) const;

  /// Gets the categorical columns in the population table that are to be
  /// included in the predictor.
  containers::CategoricalFeatures get_categorical_features(
      const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df,
      const predictors::PredictorImpl& _predictor_impl) const;

  /// Gets all of the numerical features needed from the autofeatures and the
  /// columns in the population table.
  containers::NumericalFeatures get_numerical_features(
      const containers::NumericalFeatures& _autofeatures,
      const Poco::JSON::Object& _cmd,
      const containers::DataFrame& _population_df,
      const predictors::PredictorImpl& _predictor_impl) const;

  /// Get the targets from the population table.
  std::vector<std::string> get_targets(
      const containers::DataFrame& _population_df) const;

  /// Prepares the feature learners from the JSON object.
  std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
  init_feature_learners(
      const size_t _num_targets,
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies) const;

  /// Prepares the preprocessors.
  std::vector<std::shared_ptr<preprocessors::Preprocessor>> init_preprocessors(
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies) const;

  /// Whether the pipeline is used for classification problems
  bool is_classification() const;

  /// Prepares the predictors.
  std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
  init_predictors(
      const std::string& _elem, const size_t _num_targets,
      const std::shared_ptr<const predictors::PredictorImpl>& _predictor_impl,
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies) const;

  /// Loads a new Pipeline from disc.
  Pipeline load(const std::string& _path,
                const std::shared_ptr<dependency::FETracker> _fe_tracker,
                const std::shared_ptr<dependency::PredTracker> _pred_tracker,
                const std::shared_ptr<dependency::PreprocessorTracker>
                    _preprocessor_tracker) const;

  /// Loads the feature learners.
  void load_feature_learners(
      const std::string& _path,
      const std::shared_ptr<dependency::FETracker> _fe_tracker,
      Pipeline* _pipeline) const;

  /// Loads the feature selectors.
  void load_feature_selectors(
      const std::string& _path,
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      Pipeline* _pipeline) const;

  /// Loads the fingerprints.
  void load_fingerprints(const Poco::JSON::Object& _pipeline_json,
                         Pipeline* _pipeline) const;

  /// Loads the impls for the predictors and feature selectors.
  void load_impls(const std::string& _path, Pipeline* _pipeline) const;

  /// Loads a JSON object from disc.
  Poco::JSON::Object load_json_obj(const std::string& _fname) const;

  /// Loads the pipeline.json.
  void load_pipeline_json(const std::string& _path, Pipeline* _pipeline) const;

  /// Loads the predictors.
  void load_predictors(
      const std::string& _path,
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      Pipeline* _pipeline) const;

  /// Loads the preprocessors.
  void load_preprocessors(const std::string& _path,
                          const std::shared_ptr<dependency::PreprocessorTracker>
                              _preprocessor_tracker,
                          Pipeline* _pipeline) const;

  /// Makes or retrieves the autofeatures as part of make_features(...).
  containers::NumericalFeatures make_autofeatures(
      const TransformParams& _params) const;

  /// Generates a build history from the dependencies as the fingerprints of
  /// the inserted data frames.
  std::vector<Poco::JSON::Object::Ptr> make_build_history(
      const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
      const std::vector<Poco::JSON::Object::Ptr>& _df_fingerprints) const;

  /// Generates the features we can insert into the feature selectors or
  /// predictors.
  std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
             containers::NumericalFeatures>
  make_features(const TransformParams& _params) const;

  /// Generates the features for the validation set.
  std::pair<std::optional<containers::NumericalFeatures>,
            std::optional<containers::CategoricalFeatures>>
  make_features_validation(const TransformParams& _params);

  /// Generates the names of the features.
  std::vector<std::string> make_feature_names() const;

  /// Figures out which columns from the population table we would like to
  /// add.
  void make_feature_selector_impl(const Poco::JSON::Object& _cmd,
                                  const containers::DataFrame& _population_df);

  /// Calculate the importance factors, which are needed to generate the
  /// column importances.
  std::vector<Float> make_importance_factors(
      const size_t _num_features, const std::vector<size_t>& _autofeatures,
      const std::vector<Float>::const_iterator _begin,
      const std::vector<Float>::const_iterator _end) const;

  /// Generates the placeholder and the peripheral names, integrating the
  /// many-to-one joins and all other modifications.
  std::pair<std::shared_ptr<const helpers::Placeholder>,
            std::shared_ptr<const std::vector<std::string>>>
  make_placeholder() const;

  /// Contains only the selected features.
  void make_predictor_impl(const Poco::JSON::Object& _cmd,
                           const containers::DataFrame& _population_df);

  /// Generates the schemata needed for the SQL generation of the staging
  /// tables.
  std::pair<containers::Schema, std::vector<containers::Schema>>
  make_staging_schemata() const;

  /// Implements such things as memory, horizon, many-to-joins etc.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  modify_data_frames(
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs,
      const std::shared_ptr<const communication::Logger>& _logger,
      const std::optional<std::string>& _temp_dir,
      Poco::Net::StreamSocket* _socket) const;

  /// Moves the temporary folder to its final destination at the end of
  /// .save(...).
  void move_tfile(const std::string& _path, const std::string& _name,
                  Poco::TemporaryFile* _tfile) const;

  /// Parses the population name.
  std::shared_ptr<std::string> parse_population() const;

  /// Parses the peripheral names.
  std::shared_ptr<std::vector<std::string>> parse_peripheral() const;

  /// Expresses the preprocessors as SQL code.
  std::vector<std::string> preprocessors_to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const;

  /// Retrieves the features from a data frame.
  std::tuple<containers::NumericalFeatures, containers::CategoricalFeatures,
             containers::NumericalFeatures>
  retrieve_features(const containers::DataFrame& _df) const;

  /// Retrieves the predictors from the pred_tracker, if possible.
  /// Returns true if all predictors could be retrieved.
  bool retrieve_predictors(
      const std::shared_ptr<dependency::PredTracker> _pred_tracker,
      std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>*
          _predictors) const;

  /// Saves the feature learners.
  void save_feature_learners(const Poco::TemporaryFile& _tfile) const;

  /// Saves the feature selectors.
  void save_feature_selectors(const Poco::TemporaryFile& _tfile) const;

  /// Saves the pipeline meta-information into the pipeline.json file.
  void save_pipeline_json(const Poco::TemporaryFile& _tfile) const;

  /// Saves the predictors.
  void save_predictors(const Poco::TemporaryFile& _tfile) const;

  /// Saves the preprocessors.
  void save_preprocessors(const Poco::TemporaryFile& _tfile) const;

  /// Conducts in-sample scoring after the pipeline has been fitted.
  void score_after_fitting(const TransformParams& _params);

  /// Selects the autofeatures that are needed for the prediction.
  containers::NumericalFeatures select_autofeatures(
      const containers::NumericalFeatures& _autofeatures,
      const predictors::PredictorImpl& _predictor_impl) const;

  /// Expresses the staging scripts as SQL.
  std::vector<std::string> staging_to_sql(
      const bool _targets,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const;

  /// Returns a transposed version Poco::JSON::Array::Ptr of the original
  /// vector-of-vectors.
  Poco::JSON::Array::Ptr transpose(
      const std::vector<std::vector<Float>>& _original) const;

  // --------------------------------------------------------

 private:
  /// Helper class used to clone the feature learners, selectors
  /// and predictors.
  template <class T>
  std::vector<std::shared_ptr<T>> clone(
      const std::vector<std::shared_ptr<T>>& _vec) const {
    auto out = std::vector<std::shared_ptr<T>>();
    for (const auto& elem : _vec) {
      assert_true(elem);
      out.push_back(elem->clone());
    }
    return out;
  }

  /// Helper class used to clone the feature learners, selectors
  /// and predictors.
  template <class T>
  std::vector<std::vector<std::shared_ptr<T>>> clone(
      const std::vector<std::vector<std::shared_ptr<T>>>& _vec) const {
    auto out = std::vector<std::vector<std::shared_ptr<T>>>();
    for (const auto& elem : _vec) {
      out.push_back(clone(elem));
    }
    return out;
  }

  /// Trivial accessor
  std::string& creation_time() { return impl_.creation_time_; }

  /// Trivial (const) accessor
  const std::string& creation_time() const { return impl_.creation_time_; }

  /// Trivial accessor
  std::vector<Poco::JSON::Object::Ptr>& df_fingerprints() {
    return impl_.df_fingerprints_;
  }

  /// Trivial (const) accessor
  const std::vector<Poco::JSON::Object::Ptr>& df_fingerprints() const {
    return impl_.df_fingerprints_;
  }

  /// Trivial (private) accessor
  const predictors::PredictorImpl& feature_selector_impl() const {
    throw_unless(impl_.feature_selector_impl_, "Pipeline has not been fitted.");
    return *impl_.feature_selector_impl_;
  }

  /// Trivial accessor
  std::vector<Poco::JSON::Object::Ptr>& fl_fingerprints() {
    return impl_.fl_fingerprints_;
  }

  /// Trivial (const) accessor
  const std::vector<Poco::JSON::Object::Ptr>& fl_fingerprints() const {
    return impl_.fl_fingerprints_;
  }

  /// Trivial accessor
  std::vector<Poco::JSON::Object::Ptr>& fs_fingerprints() {
    return impl_.fs_fingerprints_;
  }

  /// Trivial (const) accessor
  const std::vector<Poco::JSON::Object::Ptr>& fs_fingerprints() const {
    return impl_.fs_fingerprints_;
  }

  /// Generates the warning fingerprint
  Poco::JSON::Object::Ptr make_warning_fingerprint(
      const std::vector<Poco::JSON::Object::Ptr>& _fl_fingerprints) const {
    auto arr = jsonutils::JSON::vector_to_array_ptr(_fl_fingerprints);
    auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());
    obj->set("fl_fingerprints_", arr);
    return obj;
  }

  /// Trivial (private) accessor
  std::shared_ptr<const std::vector<helpers::Schema>>&
  modified_peripheral_schema() {
    return impl_.modified_peripheral_schema_;
  }

  /// Trivial (const) accessor
  const std::shared_ptr<const std::vector<helpers::Schema>>&
  modified_peripheral_schema() const {
    assert_true(impl_.modified_peripheral_schema_);
    return impl_.modified_peripheral_schema_;
  }

  /// Trivial (private) accessor
  std::shared_ptr<const helpers::Schema>& modified_population_schema() {
    return impl_.modified_population_schema_;
  }

  /// Trivial (const) accessor
  const std::shared_ptr<const helpers::Schema>& modified_population_schema()
      const {
    assert_true(impl_.modified_population_schema_);
    return impl_.modified_population_schema_;
  }

  /// Calculates the number of automated and manual features used.
  size_t num_features() const {
    const auto [autofeatures, manual1, manual2] = feature_names();
    return autofeatures.size() + manual1.size() + manual2.size();
  }

  /// Calculates the number of predictors per set.
  size_t num_predictors_per_set() const {
    if (predictors_.size() == 0) {
      return 0;
    }

    const auto n_expected = predictors_.at(0).size();

#ifndef NDEBUG
    for (const auto& pset : predictors_) {
      assert_true(pset.size() == n_expected);
    }
#endif  // NDEBUG

    return n_expected;
  }

  /// Trivial (private) accessor
  size_t num_predictor_sets() const { return predictors_.size(); }

  /// Trivial (const) accessor
  size_t num_targets() const { return impl_.targets_.size(); }

  /// Trivial (const) accessor
  Poco::JSON::Object& obj() { return impl_.obj_; }

  /// Trivial (private) accessor
  std::shared_ptr<const std::vector<helpers::Schema>>& peripheral_schema() {
    return impl_.peripheral_schema_;
  }

  /// Trivial (const) accessor
  const std::shared_ptr<const std::vector<helpers::Schema>>& peripheral_schema()
      const {
    assert_true(impl_.peripheral_schema_);
    return impl_.peripheral_schema_;
  }

  /// Trivial (private) accessor
  std::shared_ptr<const helpers::Schema>& population_schema() {
    return impl_.population_schema_;
  }

  /// Trivial (const) accessor
  const std::shared_ptr<const helpers::Schema>& population_schema() const {
    assert_true(impl_.population_schema_);
    return impl_.population_schema_;
  }

  /// Trivial (private) accessor
  predictors::Predictor* predictor(size_t _i, size_t _j) {
    assert_true(_i < predictors_.size());
    assert_true(_j < predictors_.at(_i).size());
    assert_true(predictors_.at(_i).at(_j));
    return predictors_.at(_i).at(_j).get();
  }

  /// Trivial (private) accessor
  const predictors::Predictor* predictor(size_t _i, size_t _j) const {
    assert_true(_i < predictors_.size());
    assert_true(_j < predictors_.at(_i).size());
    assert_true(predictors_.at(_i).at(_j));
    return predictors_.at(_i).at(_j).get();
  }

  /// Trivial (private) accessor
  const predictors::PredictorImpl& predictor_impl() const {
    throw_unless(impl_.predictor_impl_, "Pipeline has not been fitted.");
    return *impl_.predictor_impl_;
  }

  /// Trivial accessor
  std::vector<Poco::JSON::Object::Ptr>& preprocessor_fingerprints() {
    return impl_.preprocessor_fingerprints_;
  }

  /// Trivial (const) accessor
  const std::vector<Poco::JSON::Object::Ptr>& preprocessor_fingerprints()
      const {
    return impl_.preprocessor_fingerprints_;
  }

  /// Trivial accessor
  metrics::Scores& scores() { return impl_.scores_; }

  /// Trivial accessor
  std::vector<std::string>& targets() { return impl_.targets_; }

  // --------------------------------------------------------

 private:
  /// The feature learners used in this pipeline.
  std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
      feature_learners_;

  /// The feature selectors used in this pipeline (every target has its own
  /// set of feature selectors).
  std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
      feature_selectors_;

  /// Impl for the pipeline
  PipelineImpl impl_;

  /// The predictors used in this pipeline (every target has its own set of
  /// predictors).
  std::vector<std::vector<std::shared_ptr<predictors::Predictor>>> predictors_;

  /// The preprocessors used in this pipeline.
  std::vector<std::shared_ptr<preprocessors::Preprocessor>> preprocessors_;

  // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINE_HPP_
