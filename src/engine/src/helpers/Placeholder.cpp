// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "helpers/Placeholder.hpp"

namespace helpers {
// ----------------------------------------------------------------------------

void Placeholder::check_data_model(
    const std::vector<std::string>& _peripheral_names,
    const bool _is_population) const {
  if (_is_population && joined_tables_.size() == 0) {
    throw std::runtime_error(
        "The population placeholder contains no joined tables!");
  }

  for (const auto& joined_table : joined_tables_) {
    const auto it = std::find(_peripheral_names.begin(),
                              _peripheral_names.end(), joined_table.name_);

    if (it == _peripheral_names.end()) {
      throw std::runtime_error(
          "Placeholder '" + joined_table.name_ +
          "' is contained in the relational tree, but not among "
          "the peripheral placeholders!");
    }

    joined_table.check_data_model(_peripheral_names, false);
  }
}

// ----------------------------------------------------------------------------

void Placeholder::check_vector_length() {
  const size_t expected = joined_tables_.size();

  if (allow_lagged_targets_.size() != expected) {
    throw std::runtime_error(
        "Length of 'allow lagged targets' does not match length "
        "of joined tables (expected: " +
        std::to_string(expected) +
        ", got: " + std::to_string(allow_lagged_targets_.size()) + ").");
  }

  if (join_keys_used_.size() != expected) {
    throw std::runtime_error(
        "Error: Length of join keys used does not match length of "
        "joined tables!");
  }

  if (other_join_keys_used_.size() != expected) {
    throw std::runtime_error(
        "Error: Length of other join keys used does not match "
        "length of "
        "joined tables!");
  }

  if (time_stamps_used_.size() != expected) {
    throw std::runtime_error(
        "Error: Length of time stamps used does not match length "
        "of "
        "joined tables!");
  }

  if (other_time_stamps_used_.size() != expected) {
    throw std::runtime_error(
        "Error: Length of other time stamps used does not match "
        "length of "
        "joined tables!");
  }

  if (upper_time_stamps_used_.size() != expected) {
    throw std::runtime_error(
        "Error: Length of upper time stamps used does not match "
        "length of "
        "joined tables!");
  }
}

// ----------------------------------------------------------------------------

std::vector<bool> Placeholder::infer_needs_targets(
    const std::vector<std::string>& _peripheral_names) const {
  // ------------------------------------------------------------------------

  std::vector<bool> needs_targets(_peripheral_names.size());

  // ------------------------------------------------------------------------

  assert_true(allow_lagged_targets_.size() == joined_tables_.size());

  // ------------------------------------------------------------------------

  for (size_t i = 0; i < joined_tables_.size(); ++i) {
    const auto& joined_table = joined_tables_.at(i);

    if (allow_lagged_targets_.at(i)) {
      const auto name = joined_table.name_;

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

  // ------------------------------------------------------------------------

  return needs_targets;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Placeholder::joined_tables_to_array(
    const std::vector<Placeholder>& _vector) {
  Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());

  for (auto& elem : _vector) {
    arr->add(elem.to_json_obj());
  }

  return arr;
}

// ----------------------------------------------------------------------------

std::vector<Placeholder> Placeholder::parse_joined_tables(
    const Poco::JSON::Array::Ptr _array) {
  std::vector<Placeholder> vec;

  if (_array.isNull()) {
    return vec;
  }

  for (size_t i = 0; i < _array->size(); ++i) {
    vec.push_back(
        Placeholder(*_array->getObject(static_cast<unsigned int>(i))));
  }

  return vec;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Placeholder::to_json_obj() const {
  Poco::JSON::Object::Ptr obj(new Poco::JSON::Object());

  // ---------------------------------------------------------

  obj->set("allow_lagged_targets_",
           jsonutils::JSON::vector_to_array_ptr(allow_lagged_targets_));

  obj->set("joined_tables_",
           Placeholder::joined_tables_to_array(joined_tables_));

  obj->set("join_keys_used_",
           jsonutils::JSON::vector_to_array_ptr(join_keys_used_));

  obj->set("other_join_keys_used_",
           jsonutils::JSON::vector_to_array_ptr(other_join_keys_used_));

  obj->set("other_time_stamps_used_",
           jsonutils::JSON::vector_to_array_ptr(other_time_stamps_used_));

  obj->set("propositionalization_",
           jsonutils::JSON::vector_to_array_ptr(propositionalization_));

  obj->set("name_", name_);

  obj->set("time_stamps_used_",
           jsonutils::JSON::vector_to_array_ptr(time_stamps_used_));

  obj->set("upper_time_stamps_used_",
           jsonutils::JSON::vector_to_array_ptr(upper_time_stamps_used_));

  // ---------------------------------------------------------

  obj->set("categorical_", jsonutils::JSON::vector_to_array_ptr(categoricals_));

  obj->set("discrete_", jsonutils::JSON::vector_to_array_ptr(discretes_));

  obj->set("join_keys_", jsonutils::JSON::vector_to_array_ptr(join_keys_));

  obj->set("numerical_", jsonutils::JSON::vector_to_array_ptr(numericals_));

  obj->set("targets_", jsonutils::JSON::vector_to_array_ptr(targets_));

  obj->set("text_", jsonutils::JSON::vector_to_array_ptr(text_));

  obj->set("time_stamps_", jsonutils::JSON::vector_to_array_ptr(time_stamps_));

  // ---------------------------------------------------------

  return obj;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
