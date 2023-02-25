// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_SCHEMA_HPP_
#define HELPERS_SCHEMA_HPP_

#include <memory>
#include <string>
#include <vector>

#include "debug/debug.hpp"
#include "fct/NamedTuple.hpp"
#include "json/json.hpp"
#include "jsonutils/jsonutils.hpp"

namespace helpers {

/// The names of the categorical columns
using f_categoricals = fct::Field<"categoricals_", std::vector<std::string>>;

/// The names of the discrete columns
using f_discretes = fct::Field<"discretes_", std::vector<std::string>>;

/// The names of the join keys
using f_join_keys = fct::Field<"join_keys_", std::vector<std::string>>;

/// The table name
using f_name = fct::Field<"name_", std::string>;

/// The names of the numerical columns
using f_numericals = fct::Field<"numericals_", std::vector<std::string>>;

/// The names of the target columns
using f_targets = fct::Field<"targets_", std::vector<std::string>>;

/// The names of the text columns
using f_text = fct::Field<"text_", std::vector<std::string>>;

/// The names of the time stamp columns
using f_time_stamps = fct::Field<"time_stamps_", std::vector<std::string>>;

/// The names of the unused float columns
using f_unused_floats = fct::Field<"unused_floats_", std::vector<std::string>>;

/// The names of the unused string columns
using f_unused_strings =
    fct::Field<"unused_strings_", std::vector<std::string>>;

struct Schema {
  using NamedTupleType =
      fct::NamedTuple<f_categoricals, f_discretes, f_join_keys, f_name,
                      f_numericals, f_targets, f_text, f_time_stamps,
                      f_unused_floats, f_unused_strings>;

  /// Constructs a new schema from a named tuple.
  Schema(const NamedTupleType& _val) : val_(_val) {}

  /// Trivial getter
  const std::vector<std::string>& categoricals() const {
    return val_.get<f_categoricals>();
  }

  /// Getter for a categorical name.
  const std::string& categorical_name(size_t _j) const {
    assert_true(_j < val_.get<f_categoricals>().size());
    return val_.get<f_categoricals>().at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& discretes() const {
    return val_.get<f_discretes>();
  }

  /// Getter for a discrete name.
  const std::string& discrete_name(size_t _j) const {
    assert_true(_j < val_.get<f_discretes>().size());
    return val_.get<f_discretes>().at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& join_keys() const {
    return val_.get<f_join_keys>();
  }

  /// Getter for a join keys name  name.
  const std::string& join_keys_name(size_t _j) const {
    assert_true(_j < val_.get<f_join_keys>().size());
    return val_.get<f_join_keys>().at(_j);
  }

  /// Getter for the join key name.
  const std::string& join_keys_name() const {
    assert_true(val_.get<f_join_keys>().size() == 1);
    return val_.get<f_join_keys>().at(0);
  }

  /// Return the name of the data frame.
  const std::string& name() const { return val_.get<f_name>(); }

  /// Trivial getter
  size_t num_categoricals() const { return val_.get<f_categoricals>().size(); }

  /// Trivial getter
  size_t num_discretes() const { return val_.get<f_discretes>().size(); }

  /// Trivial getter
  size_t num_join_keys() const { return val_.get<f_join_keys>().size(); }

  /// Trivial getter
  size_t num_numericals() const { return val_.get<f_numericals>().size(); }

  /// Trivial getter
  size_t num_targets() const { return val_.get<f_targets>().size(); }

  /// Trivial getter
  size_t num_text() const { return val_.get<f_text>().size(); }

  /// Trivial getter
  size_t num_time_stamps() const { return val_.get<f_time_stamps>().size(); }

  /// Trivial getter
  const std::vector<std::string>& numericals() const {
    return val_.get<f_numericals>();
  }

  /// Getter for a numerical name.
  const std::string& numerical_name(size_t _j) const {
    assert_true(_j < val_.get<f_numericals>().size());
    return val_.get<f_numericals>().at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& targets() const {
    return val_.get<f_targets>();
  }

  /// Getter for a target name.
  const std::string& target_name(size_t _j) const {
    assert_true(_j < val_.get<f_targets>().size());
    return val_.get<f_targets>().at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& text() const { return val_.get<f_text>(); }

  /// Getter for a text name.
  const std::string& text_name(size_t _j) const {
    assert_true(_j < val_.get<text>().size());
    return val_.get<f_text>().at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& time_stamps() const {
    return val_.get<f_time_stamps>();
  }

  /// Getter for a time stamp name.
  const std::string& time_stamps_name(size_t _j) const {
    assert_true(_j < val_.get<f_time_stamps>().size());
    return val_.get<f_time_stamps>().at(_j);
  }

  /// Getter for the time stamps name.
  const std::string& time_stamps_name() const {
    assert_true(val_.get<f_time_stamps>().size() == 1 ||
                val_.get<f_time_stamps>().size() == 2);
    return val_.get<f_time_stamps>().at(0);
  }

  /// Transforms the placeholder into a JSON string
  std::string to_json() const { return json::to_json(*this); }

  /// Getter for the time stamps name.
  const std::string& upper_time_stamps_name() const {
    assert_true(val_.get<f_time_stamps>().size() == 2);
    return val_.get<f_time_stamps>().at(1);
  }

  /// Trivial getter
  const std::vector<std::string>& unused_floats() const {
    return val_.get<f_unused_floats>();
  }

  /// Trivial getter
  const std::vector<std::string>& unused_strings() const {
    return val_.get<f_unused_strings>();
  }

  /// Usually used to break a recursive definition, but in
  /// this case it is used for backwards compabatability.
  const NamedTupleType val_;
};

}  // namespace helpers

#endif  // HELPERS_SCHEMA_HPP_
