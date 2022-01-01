#include "relmt/decisiontrees/DecisionTree.hpp"

// ----------------------------------------------------------------------------

#include "relmt/aggregations/aggregations.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace decisiontrees {
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const size_t _peripheral_used,
    const std::shared_ptr<const utils::StandardScaler>& _output_scaler,
    const std::shared_ptr<const utils::StandardScaler>& _input_scaler,
    multithreading::Communicator* _comm)
    : comm_(_comm),
      hyperparameters_(_hyperparameters),
      initial_loss_reduction_(0.0),
      input_scaler_(_input_scaler),
      intercept_(0.0),
      loss_function_(_loss_function),
      output_scaler_(_output_scaler),
      peripheral_used_(_peripheral_used),
      update_rate_(0.0) {}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    const Poco::JSON::Object& _obj)
    : comm_(nullptr),
      hyperparameters_(_hyperparameters),
      initial_loss_reduction_(0.0) {
  from_json_obj(_obj, _loss_function);
}

// ----------------------------------------------------------------------------

std::tuple<Float, Float, std::vector<Float>> DecisionTree::calc_initial_weights(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end) {
  const auto ncols = _input.num_numericals() + _input.num_discretes() +
                     _output.num_numericals() + _output.num_discretes() +
                     _subfeatures.size();

  const auto zero_weights = std::vector<Float>(ncols + 1);

  const auto [_, new_weights] =
      loss_function().calc_pair(enums::Revert::False, enums::Update::calc_one,
                                hyperparameters().min_num_samples_, 0.0,
                                zero_weights, _begin, _begin, _end, _end);

  loss_function().revert_to_commit();

  const auto loss_reduction = loss_function().evaluate_split(
      0.0, zero_weights, new_weights, _begin, _end, _end);

  loss_function().commit(0.0, zero_weights, new_weights);

  return std::make_tuple(loss_reduction, std::get<0>(new_weights),
                         std::get<1>(new_weights));
}

// ----------------------------------------------------------------------------

void DecisionTree::column_importances(
    utils::ImportanceMaker* _importance_maker) const {
  // ------------------------------------------------------------------------

  assert_true(root_);

  assert_true(input_);

  assert_true(output_);

  assert_true(initial_weights_.size() > 0);

  assert_true(initial_weights_.size() >=
              input_->discretes_.size() + input_->numericals_.size() +
                  output_->discretes_.size() + output_->numericals_.size() + 1);

  // ------------------------------------------------------------------------

  const auto sum_abs = [](const Float init, const Float val) {
    return init + std::abs(val);
  };

  const auto sum_weights = std::accumulate(
      initial_weights_.begin() + 1, initial_weights_.end(), 0.0, sum_abs);

  const auto divide_by_sum = [sum_weights](const Float val) {
    return std::abs(val) / sum_weights;
  };

  auto rescaled_weights = std::vector<Float>(initial_weights_.size() - 1);

  if (sum_weights > 0.0) {
    std::transform(initial_weights_.begin() + 1, initial_weights_.end(),
                   rescaled_weights.begin(), divide_by_sum);
  }

  // ------------------------------------------------------------------------

  size_t i = 0;

  for (size_t j = 0; j < output_->discretes_.size(); ++i, ++j) {
    _importance_maker->add(*input_, *output_, enums::DataUsed::discrete_output,
                           j, 0,
                           initial_loss_reduction_ * rescaled_weights.at(i));
  }

  for (size_t j = 0; j < output_->numericals_.size(); ++i, ++j) {
    _importance_maker->add(*input_, *output_, enums::DataUsed::numerical_output,
                           j, 0,
                           initial_loss_reduction_ * rescaled_weights.at(i));
  }

  for (size_t j = 0; j < input_->discretes_.size(); ++i, ++j) {
    _importance_maker->add(*input_, *output_, enums::DataUsed::discrete_input,
                           j, 0,
                           initial_loss_reduction_ * rescaled_weights.at(i));
  }

  for (size_t j = 0; j < input_->numericals_.size(); ++i, ++j) {
    _importance_maker->add(*input_, *output_, enums::DataUsed::numerical_input,
                           j, 0,
                           initial_loss_reduction_ * rescaled_weights.at(i));
  }

  // ------------------------------------------------------------------------

  for (size_t j = 0; i < rescaled_weights.size(); ++i, ++j) {
    _importance_maker->add(*input_, *output_, enums::DataUsed::subfeatures, j,
                           0, initial_loss_reduction_ * rescaled_weights.at(i));
  }

  // ------------------------------------------------------------------------

  root_->column_importances(_importance_maker);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(const containers::DataFrameView& _output,
                       const std::optional<containers::DataFrame>& _input,
                       const containers::Subfeatures& _subfeatures,
                       const containers::Rescaled& _output_rescaled,
                       const containers::Rescaled& _input_rescaled,
                       const std::vector<containers::Match>::iterator _begin,
                       const std::vector<containers::Match>::iterator _end) {
  // ------------------------------------------------------------------------

  input_ = std::make_shared<const helpers::Schema>(_input->to_schema());

  output_ = std::make_shared<const helpers::Schema>(_output.df().to_schema());

  // ------------------------------------------------------------------------

  // TODO: Remove optional
  assert_true(_input);

  std::tie(initial_loss_reduction_, intercept_, initial_weights_) =
      calc_initial_weights(_output, *_input, _subfeatures, _begin, _end);

  is_ts_ = make_is_ts(_output, *_input);

  // ------------------------------------------------------------------------

  root_.reset(new DecisionTreeNode(
      utils::ConditionMaker(hyperparameters().delta_t_, peripheral_used(),
                            input_scaler_, output_scaler_,
                            std::make_shared<const std::vector<bool>>(is_ts_)),
      0, hyperparameters_, loss_function_, input_, output_, initial_weights_,
      &comm()));

  root_->fit(_output, _input, _subfeatures, _output_rescaled, _input_rescaled,
             _begin, _end, &intercept_);

  // ------------------------------------------------------------------------
  // Reset the loss function, so that it can be used for the next tree.

  loss_function_->reset();

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::from_json_obj(
    const Poco::JSON::Object& _obj,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function) {
  loss_function_ = aggregations::AggregationParser::parse(
      JSON::get_value<std::string>(_obj, "loss_"), _loss_function);

  input_ = std::make_shared<const helpers::Schema>(
      helpers::Schema::from_json(*JSON::get_object(_obj, "input_")));

  intercept_ = JSON::get_value<Float>(_obj, "intercept_");

  output_ = std::make_shared<const helpers::Schema>(
      helpers::Schema::from_json(*JSON::get_object(_obj, "output_")));

  peripheral_used_ = JSON::get_value<size_t>(_obj, "peripheral_used_");

  update_rate_ = JSON::get_value<Float>(_obj, "update_rate_");

  input_scaler_ = std::make_shared<utils::StandardScaler>(
      *JSON::get_object(_obj, "input_scaler_"));

  output_scaler_ = std::make_shared<utils::StandardScaler>(
      *JSON::get_object(_obj, "output_scaler_"));

  initial_loss_reduction_ =
      JSON::get_value<Float>(_obj, "initial_loss_reduction_");

  initial_weights_ =
      JSON::array_to_vector<Float>(JSON::get_array(_obj, "initial_weights_"));

  is_ts_ = JSON::array_to_vector<bool>(JSON::get_array(_obj, "is_ts_"));

  root_.reset(new DecisionTreeNode(
      utils::ConditionMaker(hyperparameters().delta_t_, peripheral_used(),
                            input_scaler_, output_scaler_,
                            std::make_shared<const std::vector<bool>>(is_ts_)),
      0,  // _depth
      hyperparameters_, loss_function_, input_, output_,
      *JSON::get_object(_obj, "root_")));
}

// ----------------------------------------------------------------------------

void DecisionTree::handle_fast_prop_importances(
    const fastprop::subfeatures::FastPropContainer& _fast_prop_container,
    const bool _is_subfeatures,
    utils::ImportanceMaker* _importance_maker) const {
  const auto is_fast_prop = [](const std::string& _name) -> bool {
    return _name.find(helpers::Macros::fast_prop_feature()) !=
           std::string::npos;
  };

  const auto make_col_descs =
      [](const std::string& _marker, const std::string& _table,
         const std::string& _name) -> helpers::ColumnDescription {
    return helpers::ColumnDescription(_marker, _table, _name);
  };

  const auto make_col_descs_output =
      std::bind(make_col_descs, helpers::ColumnDescription::POPULATION,
                output().name(), std::placeholders::_1);

  const auto make_col_descs_input =
      std::bind(make_col_descs, helpers::ColumnDescription::PERIPHERAL,
                input().name(), std::placeholders::_1);

  const auto fast_prop_input =
      peripheral_used() < _fast_prop_container.size()
          ? _fast_prop_container.subcontainers(peripheral_used())
          : std::shared_ptr<const fastprop::subfeatures::FastPropContainer>();

  if (_fast_prop_container.has_fast_prop()) {
    const auto range = output().numericals_ | VIEWS::filter(is_fast_prop) |
                       VIEWS::transform(make_col_descs_output);

    const auto descs = stl::collect::vector<helpers::ColumnDescription>(range);

    const auto importance_factors =
        _importance_maker->retrieve_fast_prop(descs);

    const auto importances =
        _fast_prop_container.fast_prop().column_importances(importance_factors,
                                                            _is_subfeatures);

    _importance_maker->merge(importances);
  }

  if (fast_prop_input && fast_prop_input->has_fast_prop()) {
    const auto range = input().numericals_ | VIEWS::filter(is_fast_prop) |
                       VIEWS::transform(make_col_descs_input);

    const auto descs = stl::collect::vector<helpers::ColumnDescription>(range);

    const auto importance_factors =
        _importance_maker->retrieve_fast_prop(descs);

    const auto importances = fast_prop_input->fast_prop().column_importances(
        importance_factors, _is_subfeatures);

    _importance_maker->merge(importances);
  }
}

// ----------------------------------------------------------------------------

std::vector<bool> DecisionTree::make_is_ts(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input) const {
  const auto unit_has_ts = [](const std::string unit) {
    return unit.find("time stamp") != std::string::npos;
  };

  auto is_ts = std::vector<bool>();

  for (size_t j = 0; j < _output.num_discretes(); ++j) {
    is_ts.push_back(unit_has_ts(_output.discrete_unit(j)));
  }

  for (size_t j = 0; j < _output.num_numericals(); ++j) {
    is_ts.push_back(unit_has_ts(_output.numerical_unit(j)));
  }

  for (size_t j = 0; j < _input.num_discretes(); ++j) {
    is_ts.push_back(unit_has_ts(_input.discrete_unit(j)));
  }

  for (size_t j = 0; j < _input.num_numericals(); ++j) {
    is_ts.push_back(unit_has_ts(_input.numerical_unit(j)));
  }

  return is_ts;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr DecisionTree::to_json_obj() const {
  Poco::JSON::Object::Ptr obj(new Poco::JSON::Object);

  assert_true(root_);

  assert_true(input_);

  obj->set("input_", input().to_json_obj());

  obj->set("intercept_", intercept_);

  obj->set("loss_", loss_function().type());

  obj->set("output_", output().to_json_obj());

  obj->set("peripheral_used_", peripheral_used_);

  obj->set("root_", root_->to_json_obj());

  obj->set("update_rate_", update_rate_);

  obj->set("input_scaler_", input_scaler().to_json_obj());

  obj->set("output_scaler_", output_scaler().to_json_obj());

  obj->set("initial_loss_reduction_", initial_loss_reduction_);

  obj->set("initial_weights_", JSON::vector_to_array_ptr(initial_weights_));

  obj->set("is_ts_", JSON::vector_to_array_ptr(is_ts_));

  return obj;
}

// ----------------------------------------------------------------------------

std::set<size_t> DecisionTree::make_subfeatures_used() const {
  assert_true(initial_weights_.size() >=
              input().num_discretes() + input().num_numericals() +
                  output().num_discretes() + output().num_numericals() + 1);

  const auto num_subfeatures =
      initial_weights_.size() - input().num_discretes() -
      input().num_numericals() - output().num_discretes() -
      output().num_numericals() - 1;

  std::set<size_t> subfeatures_used;

  for (size_t i = 0; i < num_subfeatures; ++i) {
    subfeatures_used.insert(i);
  }

  return subfeatures_used;
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const helpers::StringIterator& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _feature_prefix, const std::string _feature_num,
    const std::tuple<bool, bool, bool> _has_subfeatures) const {
  // -------------------------------------------------------------------

  assert_true(_sql_dialect_generator);

  // -------------------------------------------------------------------

  const auto quote1 = _sql_dialect_generator->quotechar1();
  const auto quote2 = _sql_dialect_generator->quotechar2();

  const std::string tab = "    ";

  // -------------------------------------------------------------------

  std::stringstream sql;

  // -------------------------------------------------------------------

  sql << "DROP TABLE IF EXISTS " << quote1 << "FEATURE_" << _feature_prefix
      << _feature_num << quote2 << ";" << std::endl
      << std::endl;

  // -------------------------------------------------------------------

  sql << "CREATE TABLE " << quote1 << "FEATURE_" << _feature_prefix
      << _feature_num << quote2 << " AS" << std::endl;

  // -------------------------------------------------------------------

  sql << "SELECT ";

  sql << loss_function().type() << "( " << std::endl;

  // -------------------------------------------------------------------

  std::vector<std::string> conditions;

  assert_true(root_);

  assert_true(peripheral_used() < _vocabulary.peripheral().size());

  root_->to_sql(_categories, _vocabulary.population(),
                _vocabulary.peripheral().at(peripheral_used()),
                _sql_dialect_generator, _feature_prefix, _feature_num, "",
                &conditions);

  // -------------------------------------------------------------------

  if (conditions.size() > 1) {
    sql << tab << "CASE" << std::endl;

    for (size_t i = 0; i < conditions.size(); ++i) {
      sql << tab << tab << conditions.at(i) << std::endl;
    }

    sql << tab << tab << "ELSE NULL" << std::endl
        << tab << "END" << std::endl
        << ") ";
  } else {
    assert_true(conditions.size() == 1);

    sql << tab << conditions.at(0).substr(5) << std::endl;
  }

  // -------------------------------------------------------------------

  sql << "AS " << quote1 << "feature_" << _feature_prefix << _feature_num
      << quote2 << "," << std::endl;

  sql << tab << " t1.rowid AS " << quote1 << "rownum" << quote2 << std::endl;

  // -------------------------------------------------------------------

  sql << _sql_dialect_generator->make_joins(output().name(), input().name(),
                                            output().join_keys_name(),
                                            input().join_keys_name());

  // -------------------------------------------------------------------

  const auto [has_normal_subfeatures, output_has_prop, input_has_prop] =
      _has_subfeatures;

  if (has_normal_subfeatures) {
    sql << _sql_dialect_generator->make_subfeature_joins(_feature_prefix,
                                                         peripheral_used_);
  }

  if (output_has_prop) {
    sql << _sql_dialect_generator->make_subfeature_joins(
        _feature_prefix, peripheral_used_, "t1", "_PROPOSITIONALIZATION");
  }

  if (input_has_prop) {
    sql << _sql_dialect_generator->make_subfeature_joins(
        _feature_prefix, peripheral_used_, "t2", "_PROPOSITIONALIZATION");
  }

  // -------------------------------------------------------------------

  if (input().num_time_stamps() > 0 && output().num_time_stamps() > 0) {
    sql << "WHERE ";

    const auto upper_ts = input().num_time_stamps() > 1
                              ? input().upper_time_stamps_name()
                              : std::string("");

    sql << _sql_dialect_generator->make_time_stamps(output().time_stamps_name(),
                                                    input().time_stamps_name(),
                                                    upper_ts, "t1", "t2", "t1");
  }

  // -------------------------------------------------------------------

  sql << "GROUP BY t1.rowid"
      << ";" << std::endl
      << std::endl
      << std::endl;

  // -------------------------------------------------------------------

  return sql.str();

  // -------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<Float>> DecisionTree::transform(
    const containers::DataFrameView& _output,
    const containers::DataFrame& _input,
    const containers::Subfeatures& _subfeatures) const {
  // ------------------------------------------------------------------------

  assert_true(root_);

  // ------------------------------------------------------------------------

  auto predictions = std::make_shared<std::vector<Float>>(_output.nrows());

  const auto output_map = std::make_shared<containers::Rescaled::MapType>(
      _output.nrows(), _output.nrows());

  const auto input_map = std::make_shared<containers::Rescaled::MapType>(
      _input.nrows(), _input.nrows());

  // ------------------------------------------------------------------------

  for (size_t ix_output = 0; ix_output < _output.nrows(); ++ix_output) {
    // ------------------------------------------------------------------------

    std::vector<containers::Match> matches;

    utils::Matchmaker::make_matches(_output, _input, ix_output, &matches);

    // ------------------------------------------------------------------------

    const auto output_rescaled = output_scaler().transform(
        _output, std::nullopt, output_map, matches.begin(), matches.end());

    const auto input_rescaled = input_scaler().transform(
        _input, _subfeatures, input_map, matches.begin(), matches.end());

    // ------------------------------------------------------------------------

    std::vector<Float> weights(matches.size());

    for (size_t i = 0; i < matches.size(); ++i) {
      weights[i] =
          root_->transform(_output, _input, _subfeatures, output_rescaled,
                           input_rescaled, matches[i]);
    }

    // ------------------------------------------------------------------------

    (*predictions)[ix_output] = loss_function_->transform(weights);

    // ------------------------------------------------------------------------
  }

  // ------------------------------------------------------------------------

  return predictions;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relmt
