// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_CATOPPARSER_HPP_
#define ENGINE_HANDLERS_CATOPPARSER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

#include <map>
#include <memory>
#include <string>

#include "debug/debug.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"

namespace engine {
namespace handlers {

class CatOpParser {
 public:
  typedef containers::ColumnView<bool>::UnknownSize UnknownSize;
  typedef containers::ColumnView<bool>::NRowsType NRowsType;
  typedef containers::ColumnView<bool>::ValueFunc ValueFunc;

  static constexpr UnknownSize NOT_KNOWABLE =
      containers::ColumnView<bool>::NOT_KNOWABLE;
  static constexpr UnknownSize NROWS_INFINITE =
      containers::ColumnView<bool>::NROWS_INFINITE;

  static constexpr bool NROWS_MUST_MATCH =
      containers::ColumnView<bool>::NROWS_MUST_MATCH;

  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  static constexpr const char* FLOAT_COLUMN_VIEW =
      containers::Column<bool>::FLOAT_COLUMN_VIEW;
  static constexpr const char* STRING_COLUMN_VIEW =
      containers::Column<bool>::STRING_COLUMN_VIEW;
  static constexpr const char* BOOLEAN_COLUMN_VIEW =
      containers::Column<bool>::BOOLEAN_COLUMN_VIEW;

  // ------------------------------------------------------------------------

 public:
  CatOpParser(
      const fct::Ref<const containers::Encoding>& _categories,
      const fct::Ref<const containers::Encoding>& _join_keys_encoding,
      const fct::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~CatOpParser() = default;

  // ------------------------------------------------------------------------

 public:
  /// Checks the string column for any obvious problems.
  void check(const containers::Column<strings::String>& _col,
             const std::string& _name,
             const fct::Ref<const communication::Logger>& _logger,
             Poco::Net::StreamSocket* _socket) const;

  /// Parses a string column.
  containers::ColumnView<strings::String> parse(
      const Poco::JSON::Object& _col) const;

  // ------------------------------------------------------------------------

 private:
  /// Parses the operator and undertakes a binary operation.
  containers::ColumnView<strings::String> binary_operation(
      const Poco::JSON::Object& _col) const;

  /// Transforms a boolean column to a string.
  containers::ColumnView<strings::String> boolean_as_string(
      const Poco::JSON::Object& _col) const;

  /// Transforms a float column to a string.
  containers::ColumnView<strings::String> numerical_as_string(
      const Poco::JSON::Object& _col) const;

  /// Returns a subselection on the column.
  containers::ColumnView<strings::String> subselection(
      const Poco::JSON::Object& _col) const;

  /// Transforms an int column to a column view.
  containers::ColumnView<strings::String> to_view(
      const containers::Column<Int>& _col,
      const fct::Ref<const containers::Encoding>& _encoding) const;

  /// Transforms a string column to a column view.
  containers::ColumnView<strings::String> to_view(
      const containers::Column<strings::String>& _col) const;

  /// Parses the operator and undertakes a unary operation.
  containers::ColumnView<strings::String> unary_operation(
      const Poco::JSON::Object& _col) const;

  /// Returns an updated version of the column.
  containers::ColumnView<strings::String> update(
      const Poco::JSON::Object& _col) const;

  /// Returns a new column with new subroles.
  containers::ColumnView<strings::String> with_subroles(
      const Poco::JSON::Object& _col) const;

  /// Returns a new column with a new unit.
  containers::ColumnView<strings::String> with_unit(
      const Poco::JSON::Object& _col) const;

  // ------------------------------------------------------------------------

  /// Undertakes a binary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<strings::String> bin_op(const Poco::JSON::Object& _col,
                                                 const Operator& _op) const {
    const auto operand1 = parse(*JSON::get_object(_col, "operand1_"));

    const auto operand2 = parse(*JSON::get_object(_col, "operand2_"));

    return containers::ColumnView<strings::String>::from_bin_op(operand1,
                                                                operand2, _op);
  }

  /// Undertakes a unary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<strings::String> un_op(const Poco::JSON::Object& _col,
                                                const Operator& _op) const {
    const auto operand1 = parse(*JSON::get_object(_col, "operand1_"));

    return containers::ColumnView<strings::String>::from_un_op(operand1, _op);
  }

  // ------------------------------------------------------------------------

 private:
  /// Encodes the categories used.
  const fct::Ref<const containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const fct::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const fct::Ref<const containers::Encoding> join_keys_encoding_;

  // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_CATOPPARSER_HPP_
