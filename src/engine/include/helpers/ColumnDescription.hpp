// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_COLUMNDESCRIPTION_HPP_
#define HELPERS_COLUMNDESCRIPTION_HPP_

#include <string>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "json/json.hpp"

namespace helpers {

struct ColumnDescription {
  static constexpr const char* PERIPHERAL = "[PERIPHERAL]";
  static constexpr const char* POPULATION = "[POPULATION]";

  using MarkerType = fct::Literal<"[PERIPHERAL]", "[POPULATION]">;

  /// Which table are we referring to?
  using f_marker = fct::Field<"marker_", MarkerType>;

  /// The name of the column.
  using f_name = fct::Field<"name_", std::string>;

  /// The name of the table.
  using f_table = fct::Field<"table_", std::string>;

  using NamedTupleType = fct::NamedTuple<f_marker, f_name, f_table>;

  ColumnDescription(const MarkerType& _marker, const std::string& _table,
                    const std::string& _name)
      : val_(f_marker(_marker) * f_name(_name) * f_table(_table)) {}

  ColumnDescription(const std::string& _marker, const std::string& _table,
                    const std::string& _name)
      : val_(f_marker(_marker) * f_name(_name) * f_table(_table)) {}

  explicit ColumnDescription(const NamedTupleType& _val) : val_(_val) {}

  ~ColumnDescription() = default;

  /// Generates the full name from the description.
  std::string full_name() const {
    return marker().name() + " " + table() + "." + name();
  }

  /// Trivial accessor
  const MarkerType& marker() const { return val_.get<f_marker>(); }

  /// Trivial accessor
  const std::string& name() const { return val_.get<f_name>(); }

  /// Equal to operator
  bool operator==(const ColumnDescription& _other) const {
    return val_ == _other.val_;
  }

  /// Less than operator
  bool operator<(const ColumnDescription& _other) const {
    if (marker() != _other.marker()) {
      return (marker().value() < _other.marker().value());
    }

    if (table() != _other.table()) {
      return (table() < _other.table());
    }

    if (name() != _other.name()) {
      return (name() < _other.name());
    }

    return false;
  }

  /// Trivial accessor
  const std::string& table() const { return val_.get<f_table>(); }

  NamedTupleType val_;
};

}  // namespace helpers

#endif  // HELPERS_COLUMNDESCRIPTION_HPP_
