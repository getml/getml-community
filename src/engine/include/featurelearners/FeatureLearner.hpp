// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FEATURELEARNERS_FEATURELEARNER_HPP_
#define FEATURELEARNERS_FEATURELEARNER_HPP_

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "fastprop/algorithm/algorithm.hpp"
#include "fastprop/subfeatures/subfeatures.hpp"
#include "fct/Field.hpp"
#include "fct/define_named_tuple.hpp"
#include "fct/get.hpp"
#include "featurelearners/AbstractFeatureLearner.hpp"
#include "featurelearners/FeatureLearnerParams.hpp"
#include "featurelearners/FitParams.hpp"
#include "featurelearners/Float.hpp"
#include "featurelearners/Int.hpp"
#include "featurelearners/TransformParams.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/FeatureContainer.hpp"
#include "helpers/Loader.hpp"
#include "helpers/RowIndexContainer.hpp"
#include "helpers/Saver.hpp"
#include "helpers/StringIterator.hpp"
#include "helpers/VocabularyContainer.hpp"
#include "helpers/WordIndexContainer.hpp"

namespace featurelearners {

template <class FeatureLearnerType>
class FeatureLearner : public AbstractFeatureLearner {
 public:
  using NamedTupleType = fct::define_named_tuple_t<
      typename FeatureLearnerType::NamedTupleType,
      fct::Field<
          "fast_prop_container_",
          std::shared_ptr<const fastprop::subfeatures::FastPropContainer>>,
      fct::Field<"target_num_", Int>,
      fct::Field<"vocabulary_",
                 std::shared_ptr<const helpers::VocabularyContainer>>>;

 private:
  /// Whether this is a FastProp algorithm
  static constexpr bool is_fastprop_ =
      std::is_same<FeatureLearnerType, fastprop::algorithm::FastProp>();

  /// Because FastProp are propositionalization
  /// approaches themselves, they do not have a propositionalization
  /// subfeature.
  static constexpr bool has_propositionalization_ = !is_fastprop_;

 private:
  typedef typename FeatureLearnerType::DataFrameType DataFrameType;
  typedef typename FeatureLearnerType::HypType HypType;
  typedef fct::define_named_tuple_t<
      typename HypType::NamedTupleType,
      typename commands::Fingerprint::Dependencies,
      typename commands::Fingerprint::OtherFLRequirements>
      FingerprintType;

  typedef typename std::conditional<
      has_propositionalization_,
      std::shared_ptr<const fastprop::Hyperparameters>, int>::type PropType;

 public:
  FeatureLearner(const FeatureLearnerParams& _params,
                 const HypType& _hyperparameters)
      : dependencies_(_params.get<"dependencies_">()),
        hyperparameters_(_hyperparameters),
        peripheral_(_params.get<"peripheral_">()),
        peripheral_schema_(_params.get<"peripheral_schema_">()),
        placeholder_(_params.get<"placeholder_">()),
        population_schema_(_params.get<"population_schema_">()),
        target_num_(_params.get<"target_num_">()) {}

  ~FeatureLearner() = default;

 public:
  /// Calculates the column importances for this ensemble.
  std::map<helpers::ColumnDescription, Float> column_importances(
      const std::vector<Float>& _importance_factors) const final;

  /// Returns the fingerprint of the feature learner (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final;

  /// Fits the model.
  void fit(const FitParams& _params) final;

  /// Loads the feature learner from a file designated by _fname.
  void load(const std::string& _fname) final;

  /// Saves the feature learner in JSON format, if applicable
  void save(const std::string& _fname) const final;

  /// Necessary for the automatic parsing to work.
  NamedTupleType named_tuple() const {
    if (!feature_learner_) {
      throw std::runtime_error(
          "Feature learner has not been fitted, cannot save.");
    }
    return feature_learner_->named_tuple() *
           fct::make_field<"fast_prop_container_">(fast_prop_container_) *
           fct::make_field<"target_num_">(target_num_) *
           fct::make_field<"vocabulary_">(vocabulary_);
  }

  /// Return feature learner as SQL code.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories, const bool _targets,
      const bool _subfeatures,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _prefix) const final;

  /// Generate features.
  containers::NumericalFeatures transform(
      const TransformParams& _params) const final;

  /// Returns a string describing the type of the feature learner.
  std::string type() const final;

 public:
  /// Creates a deep copy.
  std::shared_ptr<AbstractFeatureLearner> clone() const final {
    return std::make_shared<FeatureLearner<FeatureLearnerType>>(*this);
  }

  /// Whether the feature learner is used for classification.
  bool is_classification() const final {
    const auto loss_function = hyperparameters_.loss_function();
    return (loss_function != "SquareLoss");
  }

  /// Returns the placeholder not as passed by the user, but as seen by the
  /// feature learner (the difference matters for time series).
  helpers::Placeholder make_placeholder() const final {
    return make_feature_learner().placeholder();
  }

  /// Returns the number of features in the feature learner.
  size_t num_features() const final { return feature_learner().num_features(); }

  /// Determines whether the population table needs targets during
  /// transform (only for time series that include autoregression).
  bool population_needs_targets() const final { return false; }

  /// Whether the feature learner is for the premium version only.
  bool premium_only() const final { return FeatureLearnerType::premium_only_; }

  /// Whether the feature learner is to be silent.
  bool silent() const final {
    return make_feature_learner().hyperparameters().silent();
  }

  /// Whether the feature learner supports multiple targets.
  bool supports_multiple_targets() const final {
    return FeatureLearnerType::supports_multiple_targets_;
  }

 private:
  /// Extract a data frame of type FeatureLearnerType::DataFrameType from
  /// an containers::DataFrame using the pre-stored schema.
  template <typename SchemaType>
  DataFrameType extract_table_by_colnames(const SchemaType& _schema,
                                          const containers::DataFrame& _df,
                                          const Int _target_num,
                                          const bool _apply_subroles) const;

  /// Extract a vector of FeatureLearnerType::DataFrameType from
  /// an containers::DataFrame using the pre-stored schema.
  std::pair<DataFrameType, std::vector<DataFrameType>>
  extract_tables_by_colnames(
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs,
      const helpers::Schema& _population_schema,
      const std::vector<helpers::Schema>& _peripheral_schema,
      const bool _apply_subroles, const bool _population_needs_targets) const;

  /// Fits the propositionalization, if applicable.
  std::optional<
      std::pair<std::shared_ptr<const fastprop::subfeatures::FastPropContainer>,
                helpers::FeatureContainer>>
  fit_propositionalization(const DataFrameType& _population,
                           const std::vector<DataFrameType>& _peripheral,
                           const helpers::RowIndexContainer& _row_indices,
                           const helpers::WordIndexContainer& _word_indices,
                           const FitParams& _params) const;

  /// Splits the text fields, if necessary and trains the RowIndexContainer
  /// and WordIndexContainer.
  std::tuple<typename FeatureLearnerType::DataFrameType,
             std::vector<typename FeatureLearnerType::DataFrameType>,
             std::shared_ptr<const helpers::VocabularyContainer>,
             helpers::RowIndexContainer, helpers::WordIndexContainer>
  handle_text_fields(
      const DataFrameType& _population,
      const std::vector<DataFrameType>& _peripheral,
      const std::shared_ptr<const logging::AbstractLogger> _logger) const;

  /// Initializes the feature learner.
  FeatureLearnerType make_feature_learner() const;

  /// Interprets the subroles and indicates whether the column they belong to
  /// should be included.
  bool parse_subroles(const std::vector<std::string>& _subroles) const;

  /// Extracts the table and column name, if they are from a many-to-one
  /// join, needed for the column importances.
  std::pair<std::string, std::string> parse_table_colname(
      const std::string& _table, const std::string& _colname) const;

  /// Transforms the propositionalization features to SQL.
  void propositionalization_to_sql(
      const helpers::StringIterator& _categories,
      const helpers::VocabularyTree& _vocabulary,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _prefix, const bool _subfeatures,
      std::vector<std::string>* _sql) const;

  /// Removes the time difference marker from the colnames, needed for the
  /// column importances.
  std::string remove_time_diff(const std::string& _from_colname) const;

  /// Transforms the proppsitionalization.
  std::optional<const helpers::FeatureContainer> transform_propositionalization(
      const DataFrameType& _population,
      const std::vector<DataFrameType>& _peripheral,
      const helpers::WordIndexContainer& _word_indices,
      const TransformParams& _params) const;

 private:
  /// Trivial accessor.
  FeatureLearnerType& feature_learner() {
    if (!feature_learner_) {
      throw std::runtime_error(
          "Feature learning algorithm has not been fitted!");
    }

    return *feature_learner_;
  }

  /// Trivial accessor.
  const FeatureLearnerType& feature_learner() const {
    if (!feature_learner_) {
      throw std::runtime_error(
          "Feature learning algorithm has not been fitted!");
    }

    return *feature_learner_;
  }

  /// Infers whether we need the targets of a peripheral table.
  std::vector<bool> infer_needs_targets() const {
    auto needs_targets = placeholder().infer_needs_targets(peripheral());

    if (peripheral_schema().size() > needs_targets.size()) {
      needs_targets.insert(needs_targets.end(),
                           peripheral_schema().size() - needs_targets.size(),
                           population_needs_targets());
    }

    return needs_targets;
  }

  /// The minimum document frequency used for the vocabulary.
  size_t min_df(const HypType& _hyp) const { return _hyp.min_df(); }

  /// Trivial accessor.
  const std::vector<std::string>& peripheral() const { return *peripheral_; }

  /// Trivial accessor.
  std::vector<helpers::Schema> peripheral_schema() const {
    return *peripheral_schema_;
  }

  /// Trivial accessor.
  const helpers::Placeholder& placeholder() const { return *placeholder_; }

  /// Trivial accessor.
  helpers::Schema population_schema() const { return *population_schema_; }

  /// Extracts the propositionalization from the hyperparameters, if they
  /// exist.
  PropType propositionalization(const HypType& _hyp) const {
    if constexpr (has_propositionalization_) {
      return _hyp.propositionalization();
    }

    if constexpr (!has_propositionalization_) {
      return 0;
    }
  }

  /// Extracts the propositionalization features.
  PropType propositionalization() const {
    return propositionalization(feature_learner().hyperparameters());
  }

  /// The size of the vocabulary.
  size_t vocab_size(const HypType& _hyp) const { return _hyp.vocab_size(); }

 private:
  /// The dependencies used to build the fingerprint.
  fct::Ref<const std::vector<commands::Fingerprint>> dependencies_;

  /// The containers for the propositionalization.
  std::shared_ptr<const fastprop::subfeatures::FastPropContainer>
      fast_prop_container_;

  /// The underlying feature learning algorithm.
  std::optional<FeatureLearnerType> feature_learner_;

  /// The underlying hyperparameters.
  HypType hyperparameters_;

  /// The names of the peripheral tables
  fct::Ref<const std::vector<std::string>> peripheral_;

  /// The schema of the peripheral tables.
  fct::Ref<const std::vector<helpers::Schema>> peripheral_schema_;

  /// The placeholder describing the data schema.
  fct::Ref<const helpers::Placeholder> placeholder_;

  /// The schema of the population table.
  fct::Ref<const helpers::Schema> population_schema_;

  /// Indicates which target to use
  Int target_num_;

  /// The vocabulary used for the text fields.
  std::shared_ptr<const helpers::VocabularyContainer> vocabulary_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::map<helpers::ColumnDescription, Float>
FeatureLearner<FeatureLearnerType>::column_importances(
    const std::vector<Float>& _importance_factors) const {
  const auto is_non_zero = [](const auto& p) -> bool { return p.second > 0.0; };

  const auto filter_non_zeros = [is_non_zero](const auto& _importances) {
    return fct::collect::map<helpers::ColumnDescription, Float>(
        _importances | VIEWS::filter(is_non_zero));
  };

  if constexpr (!has_propositionalization_) {
    return filter_non_zeros(
        feature_learner().column_importances(_importance_factors, false));
  }

  if constexpr (has_propositionalization_) {
    assert_true(fast_prop_container_);
    return filter_non_zeros(feature_learner().column_importances(
        _importance_factors, *fast_prop_container_, false));
  }
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
template <typename SchemaType>
typename FeatureLearnerType::DataFrameType
FeatureLearner<FeatureLearnerType>::extract_table_by_colnames(
    const SchemaType& _schema, const containers::DataFrame& _df,
    const Int _target_num, const bool _apply_subroles) const {
  assert_true(_target_num < 0 ||
              static_cast<size_t>(_target_num) < _schema.targets().size());

  const auto include_target = [&_df, _target_num,
                               &_schema](const std::string& _name) -> bool {
    if (_target_num == AbstractFeatureLearner::IGNORE_TARGETS) {
      return false;
    }

    if (_target_num >= 0 && _name != _schema.targets().at(_target_num)) {
      return false;
    }

    const bool exists = _df.has_target(_name);

    if (exists) {
      return true;
    }

    throw std::runtime_error("Target '" + _name +
                             "' not found in data frame '" + _df.name() +
                             "', but is required to generate the "
                             "prediction. This is because you have set "
                             "allow_lagged_targets to True.");

    return false;
  };

  const auto targets = fct::collect::vector<std::string>(
      _schema.targets() | VIEWS::filter(include_target));

  const auto include = [this, &_df](const std::string& _colname) -> bool {
    return parse_subroles(_df.subroles(_colname));
  };

  const auto categoricals =
      _apply_subroles ? fct::collect::vector<std::string>(
                            _schema.categoricals() | VIEWS::filter(include))
                      : _schema.categoricals();

  const auto discretes = _apply_subroles
                             ? fct::collect::vector<std::string>(
                                   _schema.discretes() | VIEWS::filter(include))
                             : _schema.discretes();

  const auto numericals =
      _apply_subroles ? fct::collect::vector<std::string>(
                            _schema.numericals() | VIEWS::filter(include))
                      : _schema.numericals();

  const auto text = _apply_subroles
                        ? fct::collect::vector<std::string>(
                              _schema.text() | VIEWS::filter(include))
                        : _schema.text();

  const auto schema = helpers::Schema(_schema.named_tuple().replace(
      fct::make_field<"categorical_">(categoricals),
      fct::make_field<"discrete_">(std::make_optional(discretes)),
      fct::make_field<"numerical_">(numericals),
      fct::make_field<"targets_">(targets), fct::make_field<"text_">(text)));

  return _df.to_immutable<typename FeatureLearnerType::DataFrameType>(schema);
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<typename FeatureLearnerType::DataFrameType,
          std::vector<typename FeatureLearnerType::DataFrameType>>
FeatureLearner<FeatureLearnerType>::extract_tables_by_colnames(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Schema& _population_schema,
    const std::vector<helpers::Schema>& _peripheral_schema,
    const bool _apply_subroles, const bool _population_needs_targets) const {
  const auto population_table = extract_table_by_colnames(
      _population_schema, _population_df,
      _population_needs_targets ? target_num_
                                : AbstractFeatureLearner::IGNORE_TARGETS,
      _apply_subroles);

  if (_peripheral_schema.size() != _peripheral_dfs.size()) {
    throw std::runtime_error("Expected " +
                             std::to_string(_peripheral_schema.size()) +
                             " peripheral tables, got " +
                             std::to_string(_peripheral_dfs.size()) + ".");
  }

  const auto peripheral_needs_targets = infer_needs_targets();

  assert_true(peripheral_needs_targets.size() == _peripheral_schema.size());

  const auto to_peripheral =
      [this, &peripheral_needs_targets, &_peripheral_schema, &_peripheral_dfs,
       _apply_subroles](const size_t _i) -> DataFrameType {
    const auto t = (peripheral_needs_targets.at(_i)
                        ? AbstractFeatureLearner::USE_ALL_TARGETS
                        : AbstractFeatureLearner::IGNORE_TARGETS);

    return extract_table_by_colnames(
        _peripheral_schema.at(_i), _peripheral_dfs.at(_i), t, _apply_subroles);
  };

  const auto iota = fct::iota<size_t>(0, _peripheral_schema.size());

  const auto peripheral_tables = fct::collect::vector<DataFrameType>(
      iota | VIEWS::transform(to_peripheral));

  return std::make_pair(population_table, peripheral_tables);
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
commands::Fingerprint FeatureLearner<FeatureLearnerType>::fingerprint() const {
  return commands::Fingerprint(
      FingerprintType(hyperparameters_.named_tuple() *
                      fct::make_field<"dependencies_">(*dependencies_) *
                      fct::make_field<"peripheral_">(peripheral_) *
                      fct::make_field<"placeholder_">(placeholder_) *
                      fct::make_field<"target_num_">(target_num_)));
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::fit(const FitParams& _params) {
  feature_learner_ = make_feature_learner();

  const auto [population_table, peripheral_tables] = extract_tables_by_colnames(
      _params.population_df_, _params.peripheral_dfs_, population_schema(),
      peripheral_schema(), true, true);

  const auto [population, peripheral, vocabulary, row_indices, word_indices] =
      handle_text_fields(population_table, peripheral_tables, _params.logger_);

  const auto prop_pair = fit_propositionalization(
      population, peripheral, row_indices, word_indices, _params);

  const auto params = typename FeatureLearnerType::FitParamsType{
      .feature_container_ =
          prop_pair ? prop_pair->second
                    : std::optional<const helpers::FeatureContainer>(),
      .logger_ = _params.logger_,
      .peripheral_ = peripheral,
      .population_ = population,
      .row_indices_ = row_indices,
      .temp_dir_ = _params.temp_dir_,
      .word_indices_ = word_indices};

  feature_learner_->fit(params);

  fast_prop_container_ =
      prop_pair
          ? prop_pair->first
          : std::shared_ptr<const fastprop::subfeatures::FastPropContainer>();

  vocabulary_ = vocabulary;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::optional<
    std::pair<std::shared_ptr<const fastprop::subfeatures::FastPropContainer>,
              helpers::FeatureContainer>>
FeatureLearner<FeatureLearnerType>::fit_propositionalization(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const helpers::RowIndexContainer& _row_indices,
    const helpers::WordIndexContainer& _word_indices,
    const FitParams& _params) const {
  if constexpr (has_propositionalization_) {
    assert_true(feature_learner_);

    const auto is_true = [](const bool _val) { return _val; };

    const bool all_propositionalization =
        (feature_learner_.value().placeholder().propositionalization().size() >
         0) &&
        std::all_of(
            feature_learner_.value()
                .placeholder()
                .propositionalization()
                .begin(),
            feature_learner_.value().placeholder().propositionalization().end(),
            is_true);

    if (all_propositionalization) {
      throw std::runtime_error(
          "All joins in the data model have been set to "
          "propositionalization. You should use FastProp "
          "instead.");
    }

    const auto hyp =
        propositionalization(feature_learner_.value().hyperparameters());

    if (!hyp) {
      return std::nullopt;
    }

    const auto peripheral_names =
        std::make_shared<const std::vector<std::string>>(
            feature_learner_.value().peripheral());

    using MakerParams = fastprop::subfeatures::MakerParams;

    const auto params =
        MakerParams{.hyperparameters_ = hyp,
                    .logger_ = _params.logger_,
                    .peripheral_ = _peripheral,
                    .peripheral_names_ = peripheral_names,
                    .placeholder_ = feature_learner_.value().placeholder(),
                    .population_ = _population,
                    .prefix_ = _params.prefix_,
                    .row_index_container_ = _row_indices,
                    .temp_dir_ = _params.temp_dir_,
                    .word_index_container_ = _word_indices};

    return fastprop::subfeatures::Maker::fit(params);
  }

  return std::nullopt;
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::tuple<typename FeatureLearnerType::DataFrameType,
           std::vector<typename FeatureLearnerType::DataFrameType>,
           std::shared_ptr<const helpers::VocabularyContainer>,
           helpers::RowIndexContainer, helpers::WordIndexContainer>
FeatureLearner<FeatureLearnerType>::handle_text_fields(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger) const {
  assert_true(_logger);

  const auto has_text_fields = [](const helpers::DataFrame& _df) -> bool {
    return _df.num_text() > 0;
  };

  const bool any_text_fields =
      has_text_fields(_population) ||
      std::any_of(_peripheral.begin(), _peripheral.end(), has_text_fields);

  if (any_text_fields) _logger->log("Indexing text fields...");

  const auto vocabulary = std::make_shared<const helpers::VocabularyContainer>(
      min_df(hyperparameters_), vocab_size(hyperparameters_), _population,
      _peripheral);

#ifndef NDEBUG
  assert_true(_population.num_text() == vocabulary->population().size());

  assert_true(_peripheral.size() == vocabulary->peripheral().size());

  for (size_t i = 0; i < _peripheral.size(); ++i) {
    assert_true(_peripheral.at(i).num_text() ==
                vocabulary->peripheral().at(i).size());
  }
#endif

  if (any_text_fields) _logger->log("Progress: 33%.");

  const auto word_indices =
      helpers::WordIndexContainer(_population, _peripheral, *vocabulary);

  if (any_text_fields) _logger->log("Progress: 66%.");

  const auto row_indices = helpers::RowIndexContainer(word_indices);

  if (any_text_fields) _logger->log("Progress: 100%.");

  return std::make_tuple(_population, _peripheral, vocabulary, row_indices,
                         word_indices);
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::load(const std::string& _fname) {
  using FLNamedTupleType = typename FeatureLearnerType::NamedTupleType;
  const auto val = helpers::Loader::load_from_json<NamedTupleType>(_fname);
  fast_prop_container_ = fct::get<"fast_prop_container_">(val);
  feature_learner_ = FeatureLearnerType(FLNamedTupleType(val));
  target_num_ = fct::get<"target_num_">(val);
  vocabulary_ = fct::get<"vocabulary_">(val);
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
FeatureLearnerType FeatureLearner<FeatureLearnerType>::make_feature_learner()
    const {
  // TODO: Remove the shared_ptr
  return FeatureLearnerType(std::make_shared<const HypType>(hyperparameters_),
                            peripheral_.ptr(), placeholder_.ptr());
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
bool FeatureLearner<FeatureLearnerType>::parse_subroles(
    const std::vector<std::string>& _subroles) const {
  auto blacklist = std::vector<helpers::Subrole>(
      {helpers::Subrole::exclude_feature_learners, helpers::Subrole::email_only,
       helpers::Subrole::substring_only});

  if constexpr (is_fastprop_) {
    blacklist.push_back(helpers::Subrole::exclude_fastprop);
    return !helpers::SubroleParser::contains_any(_subroles, blacklist);
  }

  assert_true(false);

  return true;
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<std::string, std::string>
FeatureLearner<FeatureLearnerType>::parse_table_colname(
    const std::string& _table, const std::string& _colname) const {
  if (_colname.find(helpers::Macros::table()) == std::string::npos) {
    if (_table.find(helpers::Macros::name()) == std::string::npos) {
      return std::make_pair(_table, _colname);
    }

    const auto table_end = _colname.find(helpers::Macros::name());

    const auto table = _colname.substr(0, table_end);

    return std::make_pair(table, _colname);
  }

  const auto table_begin = _colname.rfind(helpers::Macros::table()) +
                           helpers::Macros::table().length() + 1;

  const auto table_end = _colname.rfind(helpers::Macros::column());

  assert_true(table_end >= table_begin);

  const auto table_len = table_end - table_begin;

  const auto table = _colname.substr(table_begin, table_len);

  const auto colname_begin = table_end + helpers::Macros::column().length() + 1;

  const auto colname = _colname.substr(colname_begin);

  return std::make_pair(table, colname);
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::propositionalization_to_sql(
    const helpers::StringIterator& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix, const bool _subfeatures,
    std::vector<std::string>* _sql) const {
  if constexpr (has_propositionalization_) {
    if (!fast_prop_container_) {
      return;
    }

    fast_prop_container_->to_sql(_categories, _vocabulary,
                                 _sql_dialect_generator, _prefix, _subfeatures,
                                 _sql);
  }
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::remove_time_diff(
    const std::string& _from_colname) const {
  if (_from_colname.find(helpers::Macros::generated_ts()) ==
      std::string::npos) {
    return _from_colname;
  }

  const auto pos = _from_colname.find("\", '");

  if (pos == std::string::npos) {
    return _from_colname;
  }

  return _from_colname.substr(0, pos);
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::save(const std::string& _fname) const {
  helpers::Saver::save_as_json(_fname, *this);
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::vector<std::string> FeatureLearner<FeatureLearnerType>::to_sql(
    const helpers::StringIterator& _categories, const bool _targets,
    const bool _subfeatures,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix) const {
  std::vector<std::string> sql;

  throw_unless(vocabulary_, "Pipeline has not been fitted.");

  const auto vocabulary_tree = helpers::VocabularyTree(
      vocabulary_->population(), vocabulary_->peripheral(),
      feature_learner().placeholder(), feature_learner().peripheral(),
      feature_learner().peripheral_schema());

  propositionalization_to_sql(_categories, vocabulary_tree,
                              _sql_dialect_generator, _prefix, _subfeatures,
                              &sql);

  const auto features = feature_learner().to_sql(_categories, vocabulary_tree,
                                                 _sql_dialect_generator,
                                                 _prefix, 0, _subfeatures);

  sql.insert(sql.end(), features.begin(), features.end());

  return sql;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
containers::NumericalFeatures FeatureLearner<FeatureLearnerType>::transform(
    const TransformParams& _params) const {
  const auto [population, peripheral] = extract_tables_by_colnames(
      _params.population_df_, _params.peripheral_dfs_,
      feature_learner().population_schema(),
      feature_learner().peripheral_schema(), false, population_needs_targets());

  assert_true(vocabulary_);

  const auto word_indices =
      helpers::WordIndexContainer(population, peripheral, *vocabulary_);

  const auto feature_container = transform_propositionalization(
      population, peripheral, word_indices, _params);

  using FLTransformParams = typename FeatureLearnerType::TransformParamsType;

  const auto params = FLTransformParams{.feature_container_ = feature_container,
                                        .index_ = _params.index_,
                                        .logger_ = _params.logger_,
                                        .peripheral_ = peripheral,
                                        .population_ = population,
                                        .temp_dir_ = _params.temp_dir_,
                                        .word_indices_ = word_indices};

  return feature_learner().transform(params).to_safe_features();
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::optional<const helpers::FeatureContainer>
FeatureLearner<FeatureLearnerType>::transform_propositionalization(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const helpers::WordIndexContainer& _word_indices,
    const TransformParams& _params) const {
  if constexpr (has_propositionalization_) {
    if (!propositionalization()) {
      return std::nullopt;
    }

    assert_true(fast_prop_container_);

    const auto peripheral_names =
        std::make_shared<const std::vector<std::string>>(
            feature_learner().peripheral());

    using MakerParams = fastprop::subfeatures::MakerParams;

    assert_true(_params.prefix_ != "");

    const auto params =
        MakerParams{.fast_prop_container_ = fast_prop_container_,
                    .hyperparameters_ = propositionalization(),
                    .logger_ = _params.logger_,
                    .peripheral_ = _peripheral,
                    .peripheral_names_ = peripheral_names,
                    .placeholder_ = feature_learner().placeholder(),
                    .population_ = _population,
                    .prefix_ = _params.prefix_,
                    .temp_dir_ = _params.temp_dir_,
                    .word_index_container_ = _word_indices};

    const auto feature_container =
        fastprop::subfeatures::Maker::transform(params);

    return feature_container;
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::type() const {
  constexpr bool unknown_feature_learner = !is_fastprop_;

  static_assert(!unknown_feature_learner, "Unknown feature learner!");

  if constexpr (is_fastprop_) {
    return AbstractFeatureLearner::FASTPROP;
  }
}

// ----------------------------------------------------------------------------
}  // namespace featurelearners

#endif  // FEATURELEARNERS_FEATURELEARNER_HPP_

