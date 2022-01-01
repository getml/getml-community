#include "multirel/ensemble/DecisionTreeEnsemble.hpp"

// ----------------------------------------------------------------------------

#include "multirel/ensemble/CandidateTreeBuilder.hpp"
#include "multirel/ensemble/SameUnitIdentifier.hpp"
#include "multirel/ensemble/SubtreeHelper.hpp"
#include "multirel/ensemble/Threadutils.hpp"
#include "multirel/ensemble/TreeFitter.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace ensemble {
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const descriptors::Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const containers::Placeholder> &_placeholder)
    : impl_(_hyperparameters, _peripheral, _placeholder) {
  placeholder().check_data_model(peripheral(), true);
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(const Poco::JSON::Object &_json_obj)
    : impl_(std::make_shared<const descriptors::Hyperparameters>(
                *JSON::get_object(_json_obj, "hyperparameters_")),
            std::make_shared<const std::vector<std::string>>(
                JSON::array_to_vector<std::string>(
                    JSON::get_array(_json_obj, "peripheral_"))),
            std::make_shared<const containers::Placeholder>(
                *JSON::get_object(_json_obj, "placeholder_"))) {
  *this = from_json_obj(_json_obj);
  placeholder().check_data_model(peripheral(), true);
}

// ----------------------------------------------------------------------------

std::list<decisiontrees::DecisionTree> DecisionTreeEnsemble::build_candidates(
    const size_t _ix_feature,
    const std::vector<descriptors::SameUnits> &_same_units,
    const decisiontrees::TableHolder &_table_holder) {
  assert_true(random_number_generator());

  return CandidateTreeBuilder::build_candidates(
      _table_holder, _same_units, _ix_feature, hyperparameters(),
      random_number_generator().get(), comm());
}

// ----------------------------------------------------------------------------

std::pair<Int, std::vector<size_t>> DecisionTreeEnsemble::calc_thread_nums(
    const containers::DataFrame &_population) const {
  auto num_threads =
      Threadutils::get_num_threads(hyperparameters().num_threads_);

  const auto [thread_nums, n_unique] =
      utils::DataFrameScatterer::build_thread_nums(_population.join_keys(),
                                                   num_threads);

  if (num_threads > n_unique) {
    num_threads = n_unique;
  }

  return std::make_pair(num_threads, thread_nums);
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility_of_targets(
    const containers::DataFrame &_population_table) {
  if (_population_table.num_targets() < 1) {
    throw std::invalid_argument(
        "The population table must have at least one target "
        "column!");
  }

  for (size_t j = 0; j < _population_table.num_targets(); ++j) {
    for (size_t i = 0; i < _population_table.nrows(); ++i) {
      if (std::isnan(_population_table.target(i, j)) ||
          std::isinf(_population_table.target(i, j))) {
        throw std::invalid_argument(
            "Target values can not be NULL or "
            "infinite!");
      }
    }
  }

  if (is_classification()) {
    for (size_t j = 0; j < _population_table.num_targets(); ++j) {
      for (size_t i = 0; i < _population_table.nrows(); ++i) {
        if (_population_table.target(i, j) != 0.0 &&
            _population_table.target(i, j) != 1.0) {
          throw std::invalid_argument(
              "Target values for a "
              "classification "
              "problem have to be 0.0 or 1.0!");
        }
      }
    }
  }
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importances(
    const std::vector<Float> &_importance_factors,
    const fastprop::subfeatures::FastPropContainer &_fast_prop_container,
    const bool _is_subfeatures) const {
  auto importance_maker = utils::ImportanceMaker();

  assert_true(_importance_factors.size() == trees().size());

  for (size_t i = 0; i < trees().size(); ++i) {
    const auto importances = column_importance_for_tree(
        _importance_factors.at(i), _fast_prop_container, _is_subfeatures,
        trees().at(i));

    importance_maker.merge(importances);
  }

  if (_is_subfeatures) {
    importance_maker.transfer_population();
  }

  return importance_maker.importances();
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importance_for_tree(
    const Float _importance_factor,
    const fastprop::subfeatures::FastPropContainer &_fast_prop_container,
    const bool _is_subfeatures,
    const decisiontrees::DecisionTree &_tree) const {
  if (_importance_factor == 0.0) {
    return std::map<helpers::ColumnDescription, Float>();
  }

  const auto p = _tree.ix_perip_used();

  assert_true(subensembles_avg_.size() == subensembles_sum_.size());

  assert_true(p < subensembles_avg_.size());

  const auto &sub_avg = subensembles_avg_.at(p);

  const auto &sub_sum = subensembles_sum_.at(p);

  assert_true((sub_avg && true) == (sub_sum && true));

  assert_true(!sub_avg || (sub_avg->num_features() == sub_sum->num_features()));

  const size_t num_subfeatures = sub_avg ? sub_avg->num_features() : 0;

  auto importance_maker = utils::ImportanceMaker(num_subfeatures);

  _tree.column_importances(&importance_maker);

  if (sub_avg) {
    const auto importances_avg = sub_avg->column_importances(
        importance_maker.importance_factors_avg(), _fast_prop_container, true);

    importance_maker.merge(importances_avg);

    const auto importances_sum = sub_sum->column_importances(
        importance_maker.importance_factors_sum(), _fast_prop_container, true);

    importance_maker.merge(importances_sum);
  }

  _tree.handle_fast_prop_importances(_fast_prop_container, _is_subfeatures,
                                     &importance_maker);

  importance_maker.normalize();

  importance_maker.multiply(_importance_factor);

  return importance_maker.importances();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::extract_schemas(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral) {
  impl().population_schema_ =
      std::make_shared<const helpers::Schema>(_population.to_schema());

  auto peripheral_schema = std::make_shared<std::vector<helpers::Schema>>();

  for (auto &df : _peripheral) {
    peripheral_schema->push_back(df.to_schema());
  }

  impl().peripheral_schema_ = peripheral_schema;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(const FitParams &_params) {
  if (num_features() != 0) {
    throw std::runtime_error("Multirel model has already been fitted!");
  }

  if (_params.population_.nrows() == 0) {
    throw std::runtime_error(
        "Population table needs to contain at least some data!");
  }

  check_plausibility_of_targets(_params.population_);

  extract_schemas(_params.population_, _params.peripheral_);

  fit_spawn_threads(_params.population_, _params.peripheral_,
                    _params.row_indices_, _params.word_indices_,
                    _params.feature_container_, _params.logger_);
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
    const helpers::WordIndexContainer &_word_indices,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const size_t _num_features,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion> &_opt,
    multithreading::Communicator *_comm) {
  // ----------------------------------------------------------------

  set_comm(_comm);

  // ----------------------------------------------------------------

  if (_table_holder->main_tables().size() == 0) {
    throw std::invalid_argument("You need to provide a population table!");
  }

  if (_table_holder->main_tables()[0].nrows() == 0) {
    throw std::invalid_argument(
        "Your population table needs to contain at least one "
        "row!");
  }

  // ----------------------------------------------------------------
  // Store the column numbers, so we can make sure that the user
  // passes properly formatted data to predict(...)

  assert_true(_table_holder->main_tables().size() > 0);

  // ----------------------------------------------------------------
  // Make sure that the data passed by the user is plausible

  assert_true(_table_holder->main_tables().size() > 0);

  // ----------------------------------------------------------------
  // Store names of the targets

  if (_table_holder->main_tables()[0].num_targets() > 0) {
    targets() = {_table_holder->main_tables()[0].target_name(0)};
  }

  // ----------------------------------------------------------------
  // If there are any subfeatures, fit them.

  SubtreeHelper::fit_subensembles(_table_holder, _word_indices, _logger, *this,
                                  _opt.get(), _comm, &subensembles_avg_,
                                  &subensembles_sum_);

  // ----------------------------------------------------------------
  // If there are any subfeatures, create them.

  const auto subpredictions = SubtreeHelper::make_predictions(
      *_table_holder, subensembles_avg_, subensembles_sum_, _logger, _comm);

  const auto subfeatures =
      SubtreeHelper::make_subfeatures(*_table_holder, subpredictions);

  // ----------------------------------------------------------------

  assert_true(_table_holder->main_tables().size() > 0);

  const auto aggregation_impl = std::make_shared<aggregations::AggregationImpl>(
      _table_holder->main_tables().at(0).nrows());

  // ----------------------------------------------------------------
  // Columns that share the same units are candidates for direct
  // comparison

  assert_true(_table_holder->main_tables().size() > 0);

  assert_true(_table_holder->main_tables().size() ==
              _table_holder->peripheral_tables().size());

  auto same_units = SameUnitIdentifier::identify_same_units(
      _table_holder->peripheral_tables(),
      _table_holder->main_tables().at(0).df());

  // ----------------------------------------------------------------
  // containers::Match weights are needed for the random-forest-like
  // functionality

  if (hyperparameters().seed_ < 0) {
    throw std::invalid_argument("Seed must be positive!");
  }

  assert_true(_table_holder->main_tables().size() > 0);

  const auto nrows = _table_holder->main_tables()[0].nrows();

  auto sample_weights = std::make_shared<std::vector<Float>>(nrows, 1.0);

  random_number_generator().reset(
      new std::mt19937(static_cast<unsigned int>(hyperparameters().seed_)));

  // ----------------------------------------------------------------
  // containers::Match containers are pointers to simple structs, which
  // represent a match between a key in the peripheral table and a key in
  // the population table.

  const auto num_peripheral = _table_holder->peripheral_tables().size();

  assert_true(_table_holder->main_tables().size() == num_peripheral);

  std::vector<containers::Matches> matches(num_peripheral);

  std::vector<containers::MatchPtrs> match_ptrs(num_peripheral);

  if (hyperparameters().sampling_factor_ <= 0.0) {
    for (size_t i = 0; i < num_peripheral; ++i) {
      matches.at(i) = utils::Matchmaker::make_matches(
          _table_holder->main_tables().at(i),
          _table_holder->peripheral_tables().at(i), sample_weights);

      match_ptrs.at(i) = utils::Matchmaker::make_pointers(&matches.at(i));
    }
  }

  // ----------------------------------------------------------------

  if (!is_subensemble()) {
    utils::Logger::log("Multirel: Training features...", _logger, _comm);
  } else {
    utils::Logger::log("Multirel: Training subfeatures...", _logger, _comm);
  }

  // ----------------------------------------------------------------

  for (size_t ix_feature = 0; ix_feature < _num_features; ++ix_feature) {
    // ----------------------------------------------------------------
    // containers::Match for a random-forest-like algorithm - can be
    // turned off by setting _sampling_rate to 0.0

    if (hyperparameters().sampling_factor_ > 0.0) {
      sample_weights = _opt->make_sample_weights();

      assert_true(_table_holder->main_tables().size() > 0);

      assert_true(sample_weights->size() ==
                  _table_holder->main_tables().at(0).nrows());

      for (size_t i = 0; i < num_peripheral; ++i) {
        matches.at(i) = utils::Matchmaker::make_matches(
            _table_holder->main_tables().at(i),
            _table_holder->peripheral_tables().at(i), sample_weights);

        match_ptrs.at(i) = utils::Matchmaker::make_pointers(&matches.at(i));
      }
    }

    // ----------------------------------------------------------------
    // Reset the optimization criterion based on the residuals
    // generated
    // from the last prediction

    _opt->init(*sample_weights);

    // ----------------------------------------------------------------

    auto candidate_trees =
        build_candidates(ix_feature, same_units, *_table_holder);

    // ----------------------------------------------------------------

    TreeFitter tree_fitter(impl().hyperparameters_,
                           random_number_generator().get(), comm());

    tree_fitter.fit(*_table_holder, subfeatures, aggregation_impl, _opt,
                    &matches, &match_ptrs, &candidate_trees, &trees());

    // -------------------------------------------------------------
    // Recalculate residuals, which is needed for the gradient
    // boosting algorithm.

    if (hyperparameters().shrinkage_ > 0.0) {
      const auto ix = last_tree()->ix_perip_used();

      const auto new_feature = last_tree()->transform(
          _table_holder->main_tables().at(ix),
          _table_holder->peripheral_tables().at(ix), subfeatures.at(ix));

      assert_true(new_feature);

      _opt->update_yhat_old(*sample_weights, *new_feature);

      _opt->calc_residuals();
    }

    // -------------------------------------------------------------

    const auto progress = ((ix_feature + 1) * 100) / _num_features;

    utils::Logger::log("Trained FEATURE_" + std::to_string(ix_feature + 1) +
                           ". Progress: " + std::to_string(progress) + "%.",
                       _logger, _comm);

    // -------------------------------------------------------------
  }

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const helpers::RowIndexContainer &_row_indices,
    const helpers::WordIndexContainer &_word_indices,
    const std::optional<const helpers::FeatureContainer> &_feature_container,
    const std::shared_ptr<const logging::AbstractLogger> _logger) {
  // ------------------------------------------------------

  const auto [num_threads, thread_nums] = calc_thread_nums(_population);

  multithreading::Communicator comm(num_threads);

  // ------------------------------------------------------

  std::vector<ensemble::DecisionTreeEnsemble> ensembles;

  for (size_t i = 0; i < num_threads - 1; ++i) {
    ensembles.push_back(*this);
  }

  assert_true(impl().hyperparameters_);

  // ------------------------------------------------------

  std::vector<std::thread> threads;

  for (size_t i = 0; i < num_threads - 1; ++i) {
    threads.push_back(std::thread(
        Threadutils::fit_ensemble,
        ThreadutilsFitParams{.comm_ = &comm,
                             .ensemble_ = &ensembles.at(i),
                             .feature_container_ = _feature_container,
                             .peripheral_ = _peripheral,
                             .population_ = _population,
                             .row_indices_ = _row_indices,
                             .this_thread_num_ = i + 1,
                             .thread_nums_ = thread_nums,
                             .word_indices_ = _word_indices}));
  }

  // ------------------------------------------------------

  try {
    Threadutils::fit_ensemble(
        ThreadutilsFitParams{.comm_ = &comm,
                             .ensemble_ = this,
                             .feature_container_ = _feature_container,
                             .logger_ = _logger,
                             .peripheral_ = _peripheral,
                             .population_ = _population,
                             .row_indices_ = _row_indices,
                             .this_thread_num_ = 0,
                             .thread_nums_ = thread_nums,
                             .word_indices_ = _word_indices});
  } catch (std::exception &e) {
    for (auto &thr : threads) {
      thr.join();
    }

    throw std::runtime_error(e.what());
  }

  // ------------------------------------------------------

  for (auto &thr : threads) {
    thr.join();
  }

  // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble DecisionTreeEnsemble::from_json_obj(
    const Poco::JSON::Object &_json_obj) const {
  // ----------------------------------------

  DecisionTreeEnsemble model(*this);

  // ----------------------------------------
  // Extract hyperparameters

  model.impl().hyperparameters_.reset(new descriptors::Hyperparameters(
      *JSON::get_object(_json_obj, "hyperparameters_")));

  // ----------------------------------------
  // Extract placeholders.

  model.impl().peripheral_ = std::make_shared<const std::vector<std::string>>(
      JSON::array_to_vector<std::string>(
          JSON::get_array(_json_obj, "peripheral_")));

  model.impl().placeholder_.reset(new containers::Placeholder(
      *JSON::get_object(_json_obj, "placeholder_")));

  // ----------------------------------------

  model.allow_http() = _json_obj.has("allow_http_")
                           ? JSON::get_value<bool>(_json_obj, "allow_http_")
                           : false;

  // ----------------------------------------
  // Extract features.

  auto features = JSON::get_array(_json_obj, "features_");

  for (size_t i = 0; i < features->size(); ++i) {
    auto obj = features->getObject(static_cast<unsigned int>(i));

    model.trees().push_back(decisiontrees::DecisionTree(
        model.hyperparameters().tree_hyperparameters_, *obj));
  }

  // ----------------------------------------
  // Extract targets

  model.targets() = JSON::array_to_vector<std::string>(
      JSON::get_array(_json_obj, "targets_"));

  // ----------------------------------------
  // Extract subensembles_avg_

  auto features_avg = JSON::get_array(_json_obj, "subfeatures1_");

  model.subensembles_avg_ =
      std::vector<containers::Optional<DecisionTreeEnsemble>>(
          features_avg->size());

  for (size_t i = 0; i < features_avg->size(); ++i) {
    auto obj = features_avg->getObject(static_cast<unsigned int>(i));

    if (obj) {
      model.subensembles_avg_[i].reset(new DecisionTreeEnsemble(*obj));
    }
  }

  // ----------------------------------------
  // Extract subensembles_sum_

  auto features_sum = JSON::get_array(_json_obj, "subfeatures2_");

  model.subensembles_sum_ =
      std::vector<containers::Optional<DecisionTreeEnsemble>>(
          features_sum->size());

  for (size_t i = 0; i < features_sum->size(); ++i) {
    auto obj = features_sum->getObject(static_cast<unsigned int>(i));

    if (obj) {
      model.subensembles_sum_[i].reset(new DecisionTreeEnsemble(*obj));
    }
  }

  // ------------------------------------------------------------------------

  if (_json_obj.has("population_schema_")) {
    model.impl().population_schema_ =
        std::make_shared<const helpers::Schema>(helpers::Schema::from_json(
            *JSON::get_object(_json_obj, "population_schema_")));

    model.impl().peripheral_schema_ = helpers::Schema::from_json(
        *jsonutils::JSON::get_object_array(_json_obj, "peripheral_schema_"));
  }

  // ----------------------------------------

  return model;

  // -------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<std::string, std::string>>
DecisionTreeEnsemble::make_peripheral_map() const {
  const auto peripheral_map =
      std::make_shared<std::map<std::string, std::string>>();

  for (size_t i = 0; i < peripheral_schema().size(); ++i) {
    const auto schema_name = peripheral_schema().at(i).name();

    assert_true(schema_name.find(helpers::Macros::staging_table_num()) !=
                std::string::npos);

    const auto placeholder_name =
        helpers::StringSplitter::split(schema_name,
                                       helpers::Macros::staging_table_num())
            .at(0);

    (*peripheral_map)[placeholder_name] = schema_name;
  }

  return peripheral_map;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::select_features(const std::vector<size_t> &_index) {
  assert_true(_index.size() >= trees().size());

  const auto num_selected_features =
      (hyperparameters().num_selected_features() > 0 &&
       hyperparameters().num_selected_features() < _index.size())
          ? (hyperparameters().num_selected_features())
          : (_index.size());

  std::vector<decisiontrees::DecisionTree> selected_trees;

  for (size_t i = 0; i < num_selected_features; ++i) {
    const auto ix = _index[i];

    if (ix < trees().size()) {
      selected_trees.push_back(trees()[ix]);
    }
  }

  trees() = selected_trees;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::subfeatures_to_sql(
    const helpers::StringIterator &_categories,
    const helpers::VocabularyTree &_vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix, const size_t _offset,
    const std::shared_ptr<const std::map<std::string, std::string>>
        _peripheral_map,
    std::vector<std::string> *_sql) const {
  assert_true(subensembles_avg_.size() == subensembles_sum_.size());

  assert_msg(
      subensembles_avg_.size() == _vocabulary.subtrees().size(),
      "subensembles_avg_.size(): " + std::to_string(subensembles_avg_.size()) +
          ", _vocabulary.subtrees().size(): " +
          std::to_string(_vocabulary.subtrees().size()));

  assert_true(_sql_dialect_generator);

  assert_true(_peripheral_map);

  for (size_t i = 0; i < subensembles_avg_.size(); ++i) {
    if (subensembles_avg_.at(i)) {
      assert_true(_vocabulary.subtrees().at(i));

      const auto sub_avg = subensembles_avg_.at(i)->to_sql(
          _categories, _vocabulary.subtrees().at(i).value(),
          _sql_dialect_generator, _feature_prefix + std::to_string(i + 1) + "_",
          0, true, _peripheral_map);

      _sql->insert(_sql->end(), sub_avg.begin(), sub_avg.end());

      assert_true(subensembles_sum_.at(i));

      const auto sub_sum = subensembles_sum_.at(i)->to_sql(
          _categories, _vocabulary.subtrees().at(i).value(),
          _sql_dialect_generator, _feature_prefix + std::to_string(i + 1) + "_",
          subensembles_avg_.at(i)->num_features(), true, _peripheral_map);

      _sql->insert(_sql->end(), sub_sum.begin(), sub_sum.end());

      const auto to_feature_name =
          [&_feature_prefix, i](const size_t _feature_num) -> std::string {
        return "feature_" + _feature_prefix + std::to_string(i + 1) + "_" +
               std::to_string(_feature_num + 1);
      };

      const auto iota =
          stl::iota<size_t>(0, subensembles_avg_.at(i)->num_features() +
                                   subensembles_sum_.at(i)->num_features());

      const auto autofeatures = stl::collect::vector<std::string>(
          iota | VIEWS::transform(to_feature_name));

      const auto main_table = subensembles_avg_.at(i)->placeholder().name();

      if (autofeatures.size() > 0) {
        const auto feature_table = _sql_dialect_generator->make_feature_table(
            main_table, autofeatures, {}, {}, {},
            "_" + _feature_prefix + std::to_string(i + 1));

        _sql->push_back(feature_table + "\n");
      }
    }
  }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj(
    const bool _schema_only) const {
  // ----------------------------------------

  Poco::JSON::Object obj;

  // ------------------------------------------------------------------------

  obj.set("type_", "Multirel");

  // ----------------------------------------

  obj.set("hyperparameters_", hyperparameters().to_json_obj());

  // ----------------------------------------

  obj.set("peripheral_", JSON::vector_to_array_ptr(peripheral()));

  obj.set("placeholder_", placeholder().to_json_obj());

  // ----------------------------------------

  if (impl().population_schema_) {
    Poco::JSON::Array peripheral_schema_arr;

    for (auto &p : peripheral_schema()) {
      peripheral_schema_arr.add(p.to_json_obj());
    }

    obj.set("peripheral_schema_", peripheral_schema_arr);

    obj.set("population_schema_", population_schema().to_json_obj());
  }

  // ----------------------------------------

  if (_schema_only) {
    return obj;
  }

  // ----------------------------------------

  obj.set("allow_http_", allow_http());

  // ----------------------------------------
  // Extract features

  Poco::JSON::Array features;

  for (auto &tree : trees()) {
    features.add(tree.to_json_obj());
  }

  obj.set("features_", features);

  // ----------------------------------------
  // Extract targets

  obj.set("targets_", JSON::vector_to_array_ptr<std::string>(targets()));

  // ----------------------------------------
  // Extract subensembles_avg_

  Poco::JSON::Array features_avg;

  for (const auto &sub : subensembles_avg_) {
    if (sub) {
      features_avg.add(sub->to_json_obj());
    } else {
      features_avg.add(Poco::Dynamic::Var());
    }
  }

  obj.set("subfeatures1_", features_avg);

  // ----------------------------------------
  // Extract subensembles_sum_

  Poco::JSON::Array features_sum;

  for (const auto &sub : subensembles_sum_) {
    if (sub) {
      features_sum.add(sub->to_json_obj());
    } else {
      features_sum.add(Poco::Dynamic::Var());
    }
  }

  obj.set("subfeatures2_", features_sum);

  // ----------------------------------------

  return obj;

  // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> DecisionTreeEnsemble::to_sql(
    const helpers::StringIterator &_categories,
    const helpers::VocabularyTree &_vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix, const size_t _offset,
    const bool _subfeatures,
    const std::shared_ptr<const std::map<std::string, std::string>>
        &_peripheral_map) const {
  std::vector<std::string> sql;

  if (_subfeatures) {
    subfeatures_to_sql(
        _categories, _vocabulary, _sql_dialect_generator, _feature_prefix,
        _offset, _peripheral_map ? _peripheral_map : make_peripheral_map(),
        &sql);
  }

  for (size_t i = 0; i < trees().size(); ++i) {
    const auto &tree = trees().at(i);

    const auto p = tree.ix_perip_used();

    const auto &prop_out = placeholder().propositionalization_;

    const auto prop_in =
        p < placeholder().joined_tables_.size()
            ? placeholder().joined_tables_.at(p).propositionalization_
            : std::vector<bool>();

    const bool has_normal_subfeatures =
        std::any_of(prop_in.begin(), prop_in.end(), std::logical_not());

    const auto is_true = [](const bool _var) { return _var; };

    const bool output_has_prop =
        std::any_of(prop_out.begin(), prop_out.end(), is_true);

    const bool input_has_prop =
        std::any_of(prop_in.begin(), prop_in.end(), is_true);

    const auto has_subfeatures = std::make_tuple(
        has_normal_subfeatures, output_has_prop, input_has_prop);

    sql.push_back(trees().at(i).to_sql(
        _categories, _vocabulary, _sql_dialect_generator, _feature_prefix,
        std::to_string(_offset + i + 1), has_subfeatures));
  }

  return sql;
}

// ----------------------------------------------------------------------------

containers::Features DecisionTreeEnsemble::transform(
    const TransformParams &_params) const {
  if (_params.population_.nrows() == 0) {
    throw std::runtime_error(
        "Population table needs to contain at least some data!");
  }

  auto features = containers::Features(
      _params.population_.nrows(), _params.index_.size(), _params.temp_dir_);

  transform_spawn_threads(_params.population_, _params.peripheral_,
                          _params.index_, _params.word_indices_,
                          _params.feature_container_, _params.logger_,
                          &features);

  return features;
}

// ----------------------------------------------------------------------------

containers::Predictions DecisionTreeEnsemble::transform(
    const decisiontrees::TableHolder &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator *_comm,
    containers::Optional<aggregations::AggregationImpl> *_impl) const {
  // ----------------------------------------------------------------

  assert_true(_table_holder.main_tables().size() > 0);

  // ----------------------------------------------------------------

  const auto subpredictions = SubtreeHelper::make_predictions(
      _table_holder, subensembles_avg_, subensembles_sum_, _logger, _comm);

  const auto subfeatures =
      SubtreeHelper::make_subfeatures(_table_holder, subpredictions);

  // ----------------------------------------------------------------

  utils::Logger::log("Multirel: Building subfeatures...", _logger, _comm);

  containers::Predictions predictions;

  for (size_t i = 0; i < trees().size(); ++i) {
    const auto new_prediction = transform(_table_holder, subfeatures, i, _impl);

    const auto progress = ((i + 1) * 100) / trees().size();

    utils::Logger::log("Built FEATURE_" + std::to_string(i + 1) +
                           ". Progress: " + std::to_string(progress) + "%.",
                       _logger, _comm);

    predictions.emplace_back(std::move(new_prediction));
  }

  // ----------------------------------------------------------------

  return predictions;

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Float>> DecisionTreeEnsemble::transform(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const size_t _num_feature,
    containers::Optional<aggregations::AggregationImpl> *_impl) const {
  assert_true(_num_feature < trees().size());

  assert_true(_table_holder.main_tables().size() ==
              _table_holder.peripheral_tables().size());

  assert_true(_table_holder.main_tables().size() ==
              _table_holder.subtables().size());

  assert_true(_table_holder.main_tables().size() == _subfeatures.size());

  const auto ix = trees().at(_num_feature).ix_perip_used();

  assert_true(ix < _table_holder.main_tables().size());

  auto new_feature = trees()
                         .at(_num_feature)
                         .transform(_table_holder.main_tables().at(ix),
                                    _table_holder.peripheral_tables().at(ix),
                                    _subfeatures.at(ix));

  return new_feature;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::transform_spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<size_t> &_index,
    const std::optional<helpers::WordIndexContainer> &_word_indices,
    const std::optional<const helpers::FeatureContainer> &_feature_container,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    containers::Features *_features) const {
  // -------------------------------------------------------

  const auto [num_threads, thread_nums] = calc_thread_nums(_population);

  // -------------------------------------------------------

  multithreading::Communicator comm(num_threads);

  // -------------------------------------------------------

  std::vector<std::thread> threads;

  for (size_t i = 0; i < num_threads - 1; ++i) {
    threads.push_back(std::thread(
        Threadutils::transform_ensemble,
        ThreadutilsTransformParams{.comm_ = &comm,
                                   .ensemble_ = *this,
                                   .feature_container_ = _feature_container,
                                   .features_ = _features,
                                   .index_ = _index,
                                   .peripheral_ = _peripheral,
                                   .population_ = _population,
                                   .this_thread_num_ = i + 1,
                                   .thread_nums_ = thread_nums,
                                   .word_indices_ = _word_indices}));
  }

  // ------------------------------------------------------

  try {
    Threadutils::transform_ensemble(
        ThreadutilsTransformParams{.comm_ = &comm,
                                   .ensemble_ = *this,
                                   .feature_container_ = _feature_container,
                                   .features_ = _features,
                                   .index_ = _index,
                                   .logger_ = _logger,
                                   .peripheral_ = _peripheral,
                                   .population_ = _population,
                                   .this_thread_num_ = 0,
                                   .thread_nums_ = thread_nums,
                                   .word_indices_ = _word_indices});
  } catch (std::exception &e) {
    for (auto &thr : threads) {
      thr.join();
    }

    throw std::runtime_error(e.what());
  }

  // ------------------------------------------------------

  for (auto &thr : threads) {
    thr.join();
  }

  // -------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel
