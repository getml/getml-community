// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/PlaceholderMaker.hpp"

namespace engine {
namespace pipelines {
// ----------------------------------------------------------------------------

void PlaceholderMaker::extract_joined_tables(
    const helpers::Placeholder& _placeholder, std::set<std::string>* _names) {
  for (const auto& p : _placeholder.joined_tables()) {
    extract_joined_tables(p, _names);
    _names->insert(p.name());
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::handle_horizon(
    const helpers::Placeholder& _placeholder,
    const std::vector<Float>& _horizon) {
  auto other_time_stamps_used = _placeholder.other_time_stamps_used();

  assert_true(other_time_stamps_used.size() == _horizon.size());

  for (size_t i = 0; i < _horizon.size(); ++i) {
    if (_horizon.at(i) == 0.0) {
      continue;
    }

    other_time_stamps_used.at(i) = make_ts_name(
        _placeholder.other_time_stamps_used().at(i), _horizon.at(i));
  }

  return other_time_stamps_used;
}

// ----------------------------------------------------------------------------

helpers::Placeholder PlaceholderMaker::handle_joined_tables(
    const helpers::Placeholder& _placeholder, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias,
    const Poco::JSON::Array& _joined_tables_arr,
    const std::vector<std::string>& _relationship,
    const std::vector<std::string>& _other_time_stamps_used,
    const std::vector<std::string>& _upper_time_stamps_used,
    const bool _is_population) {
  // ------------------------------------------------------------------------

  const auto size = _joined_tables_arr.size();

  assert_true(_relationship.size() == size);

  assert_true(_placeholder.allow_lagged_targets().size() == size);

  assert_true(_placeholder.join_keys_used().size() == size);

  assert_true(_placeholder.other_join_keys_used().size() == size);

  assert_true(_other_time_stamps_used.size() == size);

  assert_true(_placeholder.time_stamps_used().size() == size);

  assert_true(_upper_time_stamps_used.size() == size);

  // ------------------------------------------------------------------------

  auto allow_lagged_targets = std::vector<bool>();

  auto join_keys_used = std::vector<std::string>();

  auto joined_tables = std::vector<helpers::Placeholder>();

  auto name = _is_population
                  ? _placeholder.name() + helpers::Macros::population()
                  : _placeholder.name();

  auto other_join_keys_used = std::vector<std::string>();

  auto other_time_stamps_used = std::vector<std::string>();

  auto propositionalization = std::vector<bool>();

  auto time_stamps_used = std::vector<std::string>();

  auto upper_time_stamps_used = std::vector<std::string>();

  // ------------------------------------------------------------------------

  for (size_t i = 0; i < size; ++i) {
    const auto joined_table_obj =
        _joined_tables_arr.getObject(static_cast<unsigned int>(i));

    // Has already been checked when initializing the Placeholder.
    assert_true(joined_table_obj);

    if (is_to_many(_relationship.at(i))) {
      const auto joined_table = make_placeholder(
          *joined_table_obj, helpers::Macros::t1_or_t2(), _num_alias, false);

      allow_lagged_targets.push_back(_placeholder.allow_lagged_targets().at(i));

      join_keys_used.push_back(_placeholder.join_keys_used().at(i));

      joined_tables.push_back(joined_table);

      other_join_keys_used.push_back(_placeholder.other_join_keys_used().at(i));

      other_time_stamps_used.push_back(_other_time_stamps_used.at(i));

      propositionalization.push_back(_relationship.at(i) ==
                                     RELATIONSHIP_PROPOSITIONALIZATION);

      time_stamps_used.push_back(_placeholder.time_stamps_used().at(i));

      upper_time_stamps_used.push_back(_upper_time_stamps_used.at(i));

      continue;
    }

    const auto alias = make_alias(_num_alias);

    const auto joined_table =
        make_placeholder(*joined_table_obj, alias, _num_alias, false);

    const auto joined_name =
        JSON::get_value<std::string>(*joined_table_obj, "name_");

    append(joined_table.allow_lagged_targets(), &allow_lagged_targets);

    const auto modified_jk =
        make_colnames(joined_name, alias, joined_table.join_keys_used());

    append(modified_jk, &join_keys_used);

    append(joined_table.other_join_keys_used(), &other_join_keys_used);

    append(joined_table.joined_tables(), &joined_tables);

    append(joined_table.other_time_stamps_used(), &other_time_stamps_used);

    append(joined_table.propositionalization(), &propositionalization);

    const auto modified_ts =
        make_colnames(joined_name, alias, joined_table.time_stamps_used());

    append(modified_ts, &time_stamps_used);

    append(joined_table.upper_time_stamps_used(), &upper_time_stamps_used);

    const auto one_to_one = (_relationship.at(i) == RELATIONSHIP_ONE_TO_ONE);

    name += helpers::Macros::make_table_name(
        _placeholder.join_keys_used().at(i),
        _placeholder.other_join_keys_used().at(i),
        _placeholder.time_stamps_used().at(i),
        _placeholder.other_time_stamps_used().at(i),
        _placeholder.upper_time_stamps_used().at(i), joined_table.name(), alias,
        _placeholder.name(), _alias, one_to_one);
  }

  return helpers::Placeholder(
      fct::make_field<"allow_lagged_targets_">(allow_lagged_targets) *
      fct::make_field<"joined_tables_">(joined_tables) *
      fct::make_field<"join_keys_used_">(join_keys_used) *
      fct::make_field<"name_">(name) *
      fct::make_field<"other_join_keys_used_">(other_join_keys_used) *
      fct::make_field<"other_time_stamps_used_">(other_time_stamps_used) *
      fct::make_field<"propositionalization_">(propositionalization) *
      fct::make_field<"time_stamps_used_">(time_stamps_used) *
      fct::make_field<"upper_time_stamps_used_">(upper_time_stamps_used));
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::handle_memory(
    const helpers::Placeholder& _placeholder,
    const std::vector<Float>& _horizon, const std::vector<Float>& _memory) {
  auto upper_time_stamps_used = _placeholder.upper_time_stamps_used();

  assert_true(_memory.size() == upper_time_stamps_used.size());
  assert_true(_memory.size() == _horizon.size());
  assert_true(_memory.size() == _placeholder.other_time_stamps_used().size());

  for (size_t i = 0; i < _memory.size(); ++i) {
    if (_memory.at(i) <= 0.0) {
      continue;
    }

    if (upper_time_stamps_used.at(i) != "") {
      throw std::runtime_error(
          "You can either set an upper time stamp or "
          "memory, but not both!");
    }

    upper_time_stamps_used.at(i) =
        make_ts_name(_placeholder.other_time_stamps_used().at(i),
                     _horizon.at(i) + _memory.at(i));
  }

  return upper_time_stamps_used;
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::make_colnames(
    const std::string& _tname, const std::string& _alias,
    const std::vector<std::string>& _old_colnames) {
  std::vector<std::string> names;

  for (const auto& colname : _old_colnames) {
    const auto name =
        colname == "" ? std::string("")
                      : helpers::Macros::make_colname(_tname, _alias, colname);

    names.push_back(name);
  }

  return names;
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::make_peripheral(
    const helpers::Placeholder& _placeholder) {
  std::set<std::string> names;

  extract_joined_tables(_placeholder, &names);

  return std::vector<std::string>(names.begin(), names.end());
}

// ----------------------------------------------------------------------------

helpers::Placeholder PlaceholderMaker::make_placeholder(
    const Poco::JSON::Object& _obj, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias, const bool _is_population) {
  // ----------------------------------------------------------

  const auto num_alias = _num_alias ? _num_alias : std::make_shared<size_t>(2);

  // ----------------------------------------------------------

  const auto placeholder = helpers::Placeholder(_obj);

  // ------------------------------------------------------------------------

  const auto joined_tables_arr = _obj.getArray("joined_tables_");

  assert_true(joined_tables_arr);

  const auto expected_size = joined_tables_arr->size();

  // ------------------------------------------------------------------------

  const auto horizon = extract_vector<Float>(_obj, "horizon_", expected_size);

  const auto memory = extract_vector<Float>(_obj, "memory_", expected_size);

  const auto relationship =
      _obj.has("relationship_")
          ? extract_vector<std::string>(_obj, "relationship_", expected_size)
          : std::vector<std::string>(expected_size, RELATIONSHIP_MANY_TO_MANY);

  // ------------------------------------------------------------------------

  const auto other_time_stamps_used = handle_horizon(placeholder, horizon);

  // ------------------------------------------------------------------------

  const auto upper_time_stamps_used =
      handle_memory(placeholder, horizon, memory);

  // ------------------------------------------------------------------------

  return handle_joined_tables(
      placeholder, _alias, num_alias, *joined_tables_arr, relationship,
      other_time_stamps_used, upper_time_stamps_used, _is_population);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string PlaceholderMaker::make_ts_name(const std::string& _ts_used,
                                           const Float _diff) {
  const bool is_rowid =
      (_ts_used.find(helpers::Macros::rowid()) != std::string::npos);

  const auto diffstr =
      transpilation::SQLGenerator::make_time_stamp_diff(_diff, is_rowid);

  if (is_rowid) {
    return helpers::Macros::open_bracket() + _ts_used + diffstr +
           helpers::Macros::close_bracket();
  }

  return helpers::Macros::generated_ts() + _ts_used + diffstr;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
