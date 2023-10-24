// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/make_placeholder.hpp"

#include "debug/debug.hpp"
#include "helpers/Macros.hpp"
#include "transpilation/SQLGenerator.hpp"

namespace engine {
namespace pipelines {
namespace make_placeholder {

using RelationshipLiteral = typename commands::DataModel::RelationshipLiteral;

void extract_joined_tables(const helpers::Placeholder& _placeholder,
                           std::set<std::string>* _names);

std::vector<std::string> handle_horizon(const commands::DataModel& _data_model,
                                        const std::vector<Float>& _horizon);

fct::Ref<const helpers::Placeholder> handle_joined_tables(
    const commands::DataModel& _data_model, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias,
    const std::vector<commands::DataModel>& _joined_tables,
    const std::vector<RelationshipLiteral>& _relationship,
    const std::vector<std::string>& _other_time_stamps_used,
    const std::vector<std::string>& _upper_time_stamps_used,
    const bool _is_population);

std::vector<std::string> handle_memory(const commands::DataModel& _data_model,
                                       const std::vector<Float>& _horizon,
                                       const std::vector<Float>& _memory);

std::vector<std::string> make_colnames(
    const std::string& _tname, const std::string& _alias,
    const std::vector<std::string>& _old_colnames);

// ----------------------------------------------------------------------------

bool is_to_many(const RelationshipLiteral& _relationship) {
  return (_relationship.value() ==
              RelationshipLiteral::value_of<"many-to-many">() ||
          _relationship.value() ==
              RelationshipLiteral::value_of<"propositionalization">() ||
          _relationship.value() ==
              RelationshipLiteral::value_of<"one-to-many">());
}

std::string make_alias(const std::shared_ptr<size_t> _num_alias) {
  assert_true(_num_alias);
  auto& num_alias = *_num_alias;
  return "t" + std::to_string(++num_alias);
}

// ----------------------------------------------------------------------------

template <typename T>
void append(const std::vector<T>& _vec2, std::vector<T>* _vec1) {
  for (const auto& elem : _vec2) {
    _vec1->push_back(elem);
  }
}

// ----------------------------------------------------------------------------

void extract_joined_tables(const helpers::Placeholder& _placeholder,
                           std::set<std::string>* _names) {
  for (const auto& p : _placeholder.joined_tables()) {
    extract_joined_tables(p, _names);
    _names->insert(p.name());
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> handle_horizon(const commands::DataModel& _data_model,
                                        const std::vector<Float>& _horizon) {
  auto other_time_stamps_used =
      _data_model.val_.get<"other_time_stamps_used_">();

  assert_true(other_time_stamps_used.size() == _horizon.size());

  for (size_t i = 0; i < _horizon.size(); ++i) {
    if (_horizon.at(i) == 0.0) {
      continue;
    }

    other_time_stamps_used.at(i) =
        make_ts_name(_data_model.val_.get<"other_time_stamps_used_">().at(i),
                     _horizon.at(i));
  }

  return other_time_stamps_used;
}

// ----------------------------------------------------------------------------

fct::Ref<const helpers::Placeholder> handle_joined_tables(
    const commands::DataModel& _data_model, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias,
    const std::vector<commands::DataModel>& _joined_tables,
    const std::vector<RelationshipLiteral>& _relationship,
    const std::vector<std::string>& _other_time_stamps_used,
    const std::vector<std::string>& _upper_time_stamps_used,
    const bool _is_population) {
  const auto size = _joined_tables.size();

  auto allow_lagged_targets = std::vector<bool>();

  auto join_keys_used = std::vector<std::string>();

  auto joined_tables = std::vector<helpers::Placeholder>();

  auto name = _is_population ? _data_model.val_.get<"name_">() +
                                   helpers::Macros::population()
                             : _data_model.val_.get<"name_">();

  auto other_join_keys_used = std::vector<std::string>();

  auto other_time_stamps_used = std::vector<std::string>();

  auto propositionalization = std::vector<bool>();

  auto time_stamps_used = std::vector<std::string>();

  auto upper_time_stamps_used = std::vector<std::string>();

  for (size_t i = 0; i < size; ++i) {
    const auto joined_table_obj =
        _data_model.val_.get<"joined_tables_">().at(i);

    if (is_to_many(_relationship.at(i))) {
      const auto joined_table = *make_placeholder(
          joined_table_obj, helpers::Macros::t1_or_t2(), _num_alias, false);

      allow_lagged_targets.push_back(
          _data_model.val_.get<"allow_lagged_targets_">().at(i));

      join_keys_used.push_back(_data_model.val_.get<"join_keys_used_">().at(i));

      joined_tables.push_back(joined_table);

      other_join_keys_used.push_back(
          _data_model.val_.get<"other_join_keys_used_">().at(i));

      other_time_stamps_used.push_back(_other_time_stamps_used.at(i));

      propositionalization.push_back(
          _relationship.at(i).value() ==
          RelationshipLiteral::value_of<"propositionalization">());

      time_stamps_used.push_back(
          _data_model.val_.get<"time_stamps_used_">().at(i));

      upper_time_stamps_used.push_back(_upper_time_stamps_used.at(i));

      continue;
    }

    const auto alias = make_alias(_num_alias);

    const auto joined_table =
        *make_placeholder(joined_table_obj, alias, _num_alias, false);

    const auto& joined_name = joined_table_obj.val_.get<"name_">();

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

    const auto one_to_one = (_relationship.at(i).value() ==
                             RelationshipLiteral::value_of<"one-to-one">());

    name += helpers::Macros::make_table_name(
        _data_model.val_.get<"join_keys_used_">().at(i),
        _data_model.val_.get<"other_join_keys_used_">().at(i),
        _data_model.val_.get<"time_stamps_used_">().at(i),
        _data_model.val_.get<"other_time_stamps_used_">().at(i),
        _data_model.val_.get<"upper_time_stamps_used_">().at(i),
        joined_table.name(), alias, _data_model.val_.get<"name_">(), _alias,
        one_to_one);
  }

  return fct::Ref<const helpers::Placeholder>::make(
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

std::vector<std::string> handle_memory(const commands::DataModel& _data_model,
                                       const std::vector<Float>& _horizon,
                                       const std::vector<Float>& _memory) {
  auto upper_time_stamps_used =
      _data_model.val_.get<"upper_time_stamps_used_">();

  const auto& other_time_stamps_used =
      _data_model.val_.get<"other_time_stamps_used_">();

  assert_true(_memory.size() == upper_time_stamps_used.size());
  assert_true(_memory.size() == _horizon.size());
  assert_true(_memory.size() == other_time_stamps_used.size());

  for (size_t i = 0; i < _memory.size(); ++i) {
    if (_memory.at(i) <= 0.0) {
      continue;
    }

    if (upper_time_stamps_used.at(i) != "") {
      throw std::runtime_error(
          "You can either set an upper time stamp or "
          "memory, but not both!");
    }

    upper_time_stamps_used.at(i) = make_ts_name(other_time_stamps_used.at(i),
                                                _horizon.at(i) + _memory.at(i));
  }

  return upper_time_stamps_used;
}

// ----------------------------------------------------------------------------

std::vector<std::string> make_colnames(
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

std::vector<std::string> make_peripheral(
    const helpers::Placeholder& _placeholder) {
  std::set<std::string> names;

  extract_joined_tables(_placeholder, &names);

  return std::vector<std::string>(names.begin(), names.end());
}

// ----------------------------------------------------------------------------

fct::Ref<const helpers::Placeholder> make_placeholder(
    const commands::DataModel& _data_model, const std::string& _alias,
    const std::shared_ptr<size_t> _num_alias, const bool _is_population) {
  const auto num_alias = _num_alias ? _num_alias : std::make_shared<size_t>(2);

  const auto& joined_tables = _data_model.val_.get<"joined_tables_">();

  const auto& horizon = _data_model.val_.get<"horizon_">();

  const auto& memory = _data_model.val_.get<"memory_">();

  const auto& relationship = _data_model.val_.get<"relationship_">();

  const auto other_time_stamps_used = handle_horizon(_data_model, horizon);

  const auto upper_time_stamps_used =
      handle_memory(_data_model, horizon, memory);

  return handle_joined_tables(_data_model, _alias, num_alias, joined_tables,
                              relationship, other_time_stamps_used,
                              upper_time_stamps_used, _is_population);
}

// ----------------------------------------------------------------------------

std::string make_ts_name(const std::string& _ts_used, const Float _diff) {
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

}  // namespace make_placeholder
}  // namespace pipelines
}  // namespace engine
