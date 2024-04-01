// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_PLACEHOLDER_HPP_
#define HELPERS_PLACEHOLDER_HPP_

#include <string>
#include <vector>

#include "debug/debug.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/define_named_tuple.hpp"
#include "rfl/json.hpp"
#include "rfl/remove_fields.hpp"

namespace helpers {

struct Placeholder {
  constexpr static const char* RELATIONSHIP_PROPOSITIONALIZATION =
      "propositionalization";

  /// Whether you want to allow the targets to be used as numerical values.
  using f_allow_lagged_targets =
      rfl::Field<"allow_lagged_targets_", std::vector<bool>>;

  /// The name of the categorical columns
  /// (this is only required for the Python API).
  using f_categoricals = rfl::Field<"categoricals_", std::vector<std::string>>;

  /// The name of the discrete columns
  /// (this is only required for the Python API).
  using f_discretes = rfl::Field<"discretes_", std::vector<std::string>>;

  /// Placeholders that are LEFT JOINED
  /// to this placeholder
  using f_joined_tables =
      rfl::Field<"joined_tables_", std::vector<Placeholder>>;

  /// The name of the join keys
  /// (this is only required for the Python API).
  using f_join_keys = rfl::Field<"join_keys_", std::vector<std::string>>;

  /// Names of the join keys used (LEFT) - should have
  /// same length as joined_tables_
  using f_join_keys_used =
      rfl::Field<"join_keys_used_", std::vector<std::string>>;

  /// Name of the Placeholder object
  using f_name = rfl::Field<"name_", std::string>;

  /// The name of the numerical columns
  /// (this is only required for the Python API).
  using f_numericals = rfl::Field<"numericals_", std::vector<std::string>>;

  /// Names of the join keys used (RIGHT) - should have
  /// same length as joined_tables_
  using f_other_join_keys_used =
      rfl::Field<"other_join_keys_used_", std::vector<std::string>>;

  /// Names of the time stamps used (RIGHT) - should have
  /// same length as joined_tables_
  using f_other_time_stamps_used =
      rfl::Field<"other_time_stamps_used_", std::vector<std::string>>;

  /// Whether we want to use propositionalization for the relationship.
  using f_propositionalization =
      rfl::Field<"propositionalization_", std::vector<bool>>;

  /// The name of the target columns
  /// (this is only required for the Python API).
  using f_targets = rfl::Field<"targets_", std::vector<std::string>>;

  /// The name of the text columns
  /// (this is only required for the Python API).
  using f_text = rfl::Field<"text_", std::vector<std::string>>;

  /// The name of the time stamp columns
  /// (this is only required for the Python API).
  using f_time_stamps = rfl::Field<"time_stamps_", std::vector<std::string>>;

  /// Names of the time stamps used (LEFT) - should have
  /// same length as joined_tables_
  using f_time_stamps_used =
      rfl::Field<"time_stamps_used_", std::vector<std::string>>;

  /// Names of the time stamps used (LEFT) for the upper bound - should have
  /// same length as joined_tables_
  using f_upper_time_stamps_used =
      rfl::Field<"upper_time_stamps_used_", std::vector<std::string>>;

  /// Subset of the fields that are actually needed for training.
  using NeededForTraining =
      rfl::NamedTuple<f_allow_lagged_targets, f_joined_tables, f_join_keys_used,
                      f_name, f_other_join_keys_used, f_other_time_stamps_used,
                      f_propositionalization, f_time_stamps_used,
                      f_upper_time_stamps_used>;

  /// Subset of the fields that simply required for the refresh command in the
  /// Python API.
  using NeededForPythonAPI =
      rfl::NamedTuple<f_categoricals, f_discretes, f_join_keys, f_name,
                      f_numericals, f_targets, f_text, f_time_stamps>;

  /// The main JSON representation of a Placeholder. Combines NeededForTraiing
  /// and NeededForPythonAPI.
  using ReflectionType = rfl::define_named_tuple_t<
      rfl::remove_fields_t<NeededForTraining, "name_">, NeededForPythonAPI>;

  explicit Placeholder(const NeededForTraining& _params);

  explicit Placeholder(const NeededForPythonAPI& _params);

  explicit Placeholder(const ReflectionType& _val);

  ~Placeholder() = default;

  /// Makes sure that all joined tables are found in the peripheral names.
  void check_data_model(const std::vector<std::string>& _peripheral_names,
                        const bool _is_population) const;

  /// Checks the length of the vectors.
  void check_vector_length();

  /// Infers whether any of the tables noted in _peripheral_names require
  /// lagged targets.
  std::vector<bool> infer_needs_targets(
      const std::vector<std::string>& _peripheral_names) const;

  /// Transforms the placeholder into a JSON string
  std::string to_json() const;

  /// Trivial getter.
  const std::vector<bool>& allow_lagged_targets() const {
    return val_.get<f_allow_lagged_targets>();
  }

  /// Trivial getter.
  const std::vector<std::string>& categoricals() const {
    return val_.get<f_categoricals>();
  }

  /// Getter for a categorical name.
  const std::string& categorical_name(size_t _j) const {
    assert_true(_j < categoricals().size());
    return categoricals()[_j];
  }

  /// Trivial getter.
  const std::vector<std::string>& discretes() const {
    return val_.get<f_discretes>();
  }

  /// Getter for a discrete name.
  const std::string& discrete_name(size_t _j) const {
    assert_true(_j < discretes().size());
    return discretes()[_j];
  }

  /// Getter for the joined tables.
  const std::vector<Placeholder>& joined_tables() const {
    return val_.get<f_joined_tables>();
  }

  /// Trivial getter.
  const std::vector<std::string>& join_keys() const {
    return val_.get<f_join_keys>();
  }

  /// Getter for a join keys name  name.
  const std::string& join_keys_name(size_t _j) const {
    assert_true(_j < join_keys().size());
    return join_keys()[_j];
  }

  /// Getter for the join key name.
  const std::string& join_keys_name() const {
    assert_true(join_keys().size() == 1);
    return join_keys()[0];
  }

  /// Trivial getter.
  const std::vector<std::string>& join_keys_used() const {
    return val_.get<f_join_keys_used>();
  }

  /// Trivial getter.
  const std::string& name() const { return val_.get<f_name>(); }

  /// Trivial getter
  size_t num_categoricals() const { return categoricals().size(); }

  /// Trivial getter
  size_t num_discretes() const { return discretes().size(); }

  /// Trivial getter
  size_t num_join_keys() const { return join_keys().size(); }

  /// Trivial getter
  size_t num_numericals() const { return numericals().size(); }

  /// Trivial getter
  size_t num_targets() const { return targets().size(); }

  /// Trivial getter
  size_t num_text() const { return text().size(); }

  /// Trivial getter
  size_t num_time_stamps() const { return time_stamps().size(); }

  /// Trivial getter.
  const std::vector<std::string>& numericals() const {
    return val_.get<f_numericals>();
  }

  /// Getter for a numerical name.
  const std::string& numerical_name(size_t _j) const {
    assert_true(_j < numericals().size());
    return numericals()[_j];
  }

  /// Trivial getter.
  const std::vector<std::string>& other_join_keys_used() const {
    return val_.get<f_other_join_keys_used>();
  }

  /// Trivial getter.
  const std::vector<std::string>& other_time_stamps_used() const {
    return val_.get<f_other_time_stamps_used>();
  }

  /// Getter for the propositionalization vector
  const std::vector<bool>& propositionalization() const {
    return val_.get<f_propositionalization>();
  }

  /// Getter for a targets.
  const std::vector<std::string>& targets() const {
    return val_.get<f_targets>();
  }

  /// Getter for a target name.
  const std::string& target_name(size_t _j) const {
    assert_true(_j < targets().size());
    return targets()[_j];
  }

  /// Trivial getter.
  const std::vector<std::string>& text() const { return val_.get<f_text>(); }

  /// Getter for a text name.
  const std::string& text_name(size_t _j) const {
    assert_true(_j < text().size());
    return text()[_j];
  }

  /// Trivial getter.
  const std::vector<std::string>& time_stamps() const {
    return val_.get<f_time_stamps>();
  }

  /// Getter for a time stamp name.
  const std::string& time_stamps_name(size_t _j) const {
    assert_true(_j < time_stamps().size());
    return time_stamps()[_j];
  }

  /// Getter for the time stamps name.
  const std::string& time_stamps_name() const {
    assert_true(time_stamps().size() == 1 || time_stamps().size() == 2);
    return time_stamps()[0];
  }

  /// Trivial getter.
  const std::vector<std::string>& time_stamps_used() const {
    return val_.get<f_time_stamps_used>();
  }

  /// Getter for the time stamps name.
  const std::string& upper_time_stamps_name() const {
    assert_true(time_stamps().size() == 2);
    return time_stamps()[1];
  }

  /// Trivial getter.
  const std::vector<std::string>& upper_time_stamps_used() const {
    return val_.get<f_upper_time_stamps_used>();
  }

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static Placeholder from_json_obj(const InputVarType& _json_obj);

  /// Used to break the recursive definition.
  ReflectionType val_;
};

}  // namespace helpers

#endif  // HELPERS_PLACEHOLDER_HPP_
