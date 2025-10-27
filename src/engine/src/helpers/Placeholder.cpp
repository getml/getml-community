// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/Placeholder.hpp"

#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>

namespace helpers {

// ----------------------------------------------------------------------------

Placeholder::Placeholder(const NeededForTraining& _params)
    : val_(_params * f_categoricals({}) * f_discretes({}) * f_join_keys({}) *
           f_numericals({}) * f_targets({}) * f_text({}) * f_time_stamps({})) {
  check_vector_length();
}

// ----------------------------------------------------------------------------

Placeholder::Placeholder(const NeededForPythonAPI& _params)
    : val_(_params * f_allow_lagged_targets({}) * f_joined_tables({}) *
           f_join_keys_used({}) * f_other_join_keys_used({}) *
           f_other_time_stamps_used({}) * f_propositionalization({}) *
           f_time_stamps_used({}) * f_upper_time_stamps_used({})) {}

// ----------------------------------------------------------------------------

Placeholder::Placeholder(const ReflectionType& _val) : val_(_val) {}

// ----------------------------------------------------------------------------

Placeholder Placeholder::from_json_obj(const InputVarType& _json_obj) {
  return Placeholder(rfl::json::read<ReflectionType>(_json_obj).value());
}

// ----------------------------------------------------------------------------

void Placeholder::check_data_model(
    const std::vector<std::string>& _peripheral_names,
    const bool _is_population) const {
  if (_is_population && joined_tables().size() == 0) {
    throw std::runtime_error(
        "The population placeholder contains no joined tables!");
  }

  for (const auto& joined_table : joined_tables()) {
    const auto it = std::find(_peripheral_names.begin(),
                              _peripheral_names.end(), joined_table.name());

    if (it == _peripheral_names.end()) {
      throw std::runtime_error(
          "Placeholder '" + joined_table.name() +
          "' is contained in the relational tree, but not among "
          "the peripheral placeholders!");
    }

    joined_table.check_data_model(_peripheral_names, false);
  }
}

// ----------------------------------------------------------------------------

void Placeholder::check_vector_length() {
  const size_t expected = joined_tables().size();

  if (allow_lagged_targets().size() != expected) {
    throw std::runtime_error(
        "Length of 'allow lagged targets' does not match length "
        "of joined tables (expected: " +
        std::to_string(expected) +
        ", got: " + std::to_string(allow_lagged_targets().size()) + ").");
  }

  if (join_keys_used().size() != expected) {
    throw std::runtime_error(
        "Error: Length of join keys used does not match length of "
        "joined tables!");
  }

  if (other_join_keys_used().size() != expected) {
    throw std::runtime_error(
        "Error: Length of other join keys used does not match "
        "length of joined tables!");
  }

  if (time_stamps_used().size() != expected) {
    throw std::runtime_error(
        "Error: Length of time stamps used does not match length "
        "of joined tables!");
  }

  if (other_time_stamps_used().size() != expected) {
    throw std::runtime_error(
        "Error: Length of other time stamps used does not match "
        "length of joined tables!");
  }

  if (upper_time_stamps_used().size() != expected) {
    throw std::runtime_error(
        "Error: Length of upper time stamps used does not match "
        "length of joined tables!");
  }
}

// ----------------------------------------------------------------------------

std::vector<bool> Placeholder::infer_needs_targets(
    const std::vector<std::string>& _peripheral_names) const {
  std::vector<bool> needs_targets(_peripheral_names.size());

  assert_true(allow_lagged_targets().size() == joined_tables().size());

  for (size_t i = 0; i < joined_tables().size(); ++i) {
    const auto& joined_table = joined_tables().at(i);

    if (allow_lagged_targets().at(i)) {
      const auto name = joined_table.name();

      const auto it =
          std::find(_peripheral_names.begin(), _peripheral_names.end(), name);

      if (it == _peripheral_names.end()) {
        throw std::runtime_error("Peripheral placeholder named '" + name +
                                 "' not found!");
      }

      const auto dist = std::distance(_peripheral_names.begin(), it);

      needs_targets.at(dist) = true;
    }

    const auto joined_table_needs_targets =
        joined_table.infer_needs_targets(_peripheral_names);

    std::transform(needs_targets.begin(), needs_targets.end(),
                   joined_table_needs_targets.begin(), needs_targets.begin(),
                   std::logical_or<bool>());
  }

  return needs_targets;
}

// ----------------------------------------------------------------------------

std::string Placeholder::to_json() const { return rfl::json::write(*this); }

// ----------------------------------------------------------------------------
}  // namespace helpers
