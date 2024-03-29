// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_SCHEMA_HPP_
#define HELPERS_SCHEMA_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "debug/debug.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"

namespace helpers {

/// The names of the categorical columns
using f_categoricals = fct::Field<"categorical_", std::vector<std::string>>;

/// The names of the discrete columns
using f_discretes =
    fct::Field<"discrete_", std::optional<std::vector<std::string>>>;

/// The names of the join keys
using f_join_keys = fct::Field<"join_keys_", std::vector<std::string>>;

/// The table name
using f_name = fct::Field<"name_", std::string>;

/// The names of the numerical columns
using f_numericals = fct::Field<"numerical_", std::vector<std::string>>;

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

class Schema {
 public:
  using NamedTupleType =
      fct::NamedTuple<f_categoricals, f_discretes, f_join_keys, f_name,
                      f_numericals, f_targets, f_text, f_time_stamps,
                      f_unused_floats, f_unused_strings>;

 public:
  Schema(const NamedTupleType& _obj);

  ~Schema();

  /// Expresses the schema as a named tuple.
  NamedTupleType named_tuple() const;

 public:
  /// Trivial getter
  const std::vector<std::string>& categoricals() const { return categoricals_; }

  /// Getter for a categorical name.
  const std::string& categorical_name(size_t _j) const {
    assert_true(_j < categoricals_.size());
    return categoricals_.at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& discretes() const { return discretes_; }

  /// Getter for a discrete name.
  const std::string& discrete_name(size_t _j) const {
    assert_true(_j < discretes_.size());
    return discretes_.at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& join_keys() const { return join_keys_; }

  /// Getter for a join keys name  name.
  const std::string& join_keys_name(size_t _j) const {
    assert_true(_j < join_keys_.size());
    return join_keys_.at(_j);
  }

  /// Getter for the join key name.
  const std::string& join_keys_name() const {
    assert_true(join_keys_.size() == 1);
    return join_keys_.at(0);
  }

  /// Return the name of the data frame.
  const std::string& name() const { return name_; }

  /// Trivial getter
  size_t num_categoricals() const { return categoricals_.size(); }

  /// Trivial getter
  size_t num_discretes() const { return discretes_.size(); }

  /// Trivial getter
  size_t num_join_keys() const { return join_keys_.size(); }

  /// Trivial getter
  size_t num_numericals() const { return numericals_.size(); }

  /// Trivial getter
  size_t num_targets() const { return targets_.size(); }

  /// Trivial getter
  size_t num_text() const { return text_.size(); }

  /// Trivial getter
  size_t num_time_stamps() const { return time_stamps_.size(); }

  /// Trivial getter
  const std::vector<std::string>& numericals() const { return numericals_; }

  /// Getter for a numerical name.
  const std::string& numerical_name(size_t _j) const {
    assert_true(_j < numericals_.size());
    return numericals_.at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& targets() const { return targets_; }

  /// Getter for a target name.
  const std::string& target_name(size_t _j) const {
    assert_true(_j < targets_.size());
    return targets_.at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& text() const { return text_; }

  /// Getter for a text name.
  const std::string& text_name(size_t _j) const {
    assert_true(_j < text_.size());
    return text_.at(_j);
  }

  /// Trivial getter
  const std::vector<std::string>& time_stamps() const { return time_stamps_; }

  /// Getter for a time stamp name.
  const std::string& time_stamps_name(size_t _j) const {
    assert_true(_j < time_stamps_.size());
    return time_stamps_.at(_j);
  }

  /// Getter for the time stamps name.
  const std::string& time_stamps_name() const {
    assert_true(time_stamps_.size() == 1 || time_stamps_.size() == 2);
    return time_stamps_.at(0);
  }

  /// Trivial getter
  const std::vector<std::string>& unused_floats() const {
    return unused_floats_;
  }

  /// Trivial getter
  const std::vector<std::string>& unused_strings() const {
    return unused_strings_;
  }

  /// Getter for the time stamps name.
  const std::string& upper_time_stamps_name() const {
    assert_true(time_stamps_.size() == 2);
    return time_stamps_.at(1);
  }

 private:
  /// The names of the categorical columns
  const std::vector<std::string> categoricals_;

  /// The names of the discrete columns
  const std::vector<std::string> discretes_;

  /// The names of the join keys
  const std::vector<std::string> join_keys_;

  /// The table name
  const std::string name_;

  /// The names of the numerical columns
  const std::vector<std::string> numericals_;

  /// The names of the target columns
  const std::vector<std::string> targets_;

  /// The names of the text columns
  const std::vector<std::string> text_;

  /// The names of the time stamp columns
  const std::vector<std::string> time_stamps_;

  /// The names of the unused float columns
  const std::vector<std::string> unused_floats_;

  /// The names of the unused string columns
  const std::vector<std::string> unused_strings_;
};

}  // namespace helpers

#endif  // HELPERS_SCHEMA_HPP_
