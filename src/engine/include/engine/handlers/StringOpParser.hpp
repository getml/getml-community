// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_STRINGOPPARSER_HPP_
#define ENGINE_HANDLERS_STRINGOPPARSER_HPP_

#include "commands/StringColumnOrStringColumnView.hpp"
#include "communication/Logger.hpp"
#include "containers/ColumnView.hpp"
#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/Int.hpp"

#include <map>
#include <string>

namespace engine {
namespace handlers {

class StringOpParser {
 public:
  typedef containers::ColumnView<bool>::UnknownSize UnknownSize;
  typedef containers::ColumnView<bool>::NRowsType NRowsType;
  typedef containers::ColumnView<bool>::ValueFunc ValueFunc;

  typedef typename commands::StringColumnOrStringColumnView::StringBinaryOp
      StringBinaryOp;
  typedef typename commands::StringColumnOrStringColumnView::StringColumnOp
      StringColumnOp;
  typedef typename commands::StringColumnOrStringColumnView::StringConstOp
      StringConstOp;
  typedef
      typename commands::StringColumnOrStringColumnView::StringSubselectionOp
          StringSubselectionOp;
  typedef typename commands::StringColumnOrStringColumnView::StringSubstringOp
      StringSubstringOp;
  typedef
      typename commands::StringColumnOrStringColumnView::StringWithSubrolesOp
          StringWithSubrolesOp;
  typedef typename commands::StringColumnOrStringColumnView::StringUnaryOp
      StringUnaryOp;
  typedef typename commands::StringColumnOrStringColumnView::StringUpdateOp
      StringUpdateOp;
  typedef typename commands::StringColumnOrStringColumnView::StringWithUnitOp
      StringWithUnitOp;

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

 public:
  StringOpParser(
      const rfl::Ref<const containers::Encoding>& _categories,
      const rfl::Ref<const containers::Encoding>& _join_keys_encoding,
      const rfl::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~StringOpParser() = default;

 public:
  /// Checks the string column for any obvious problems.
  void check(const containers::Column<strings::String>& _col,
             const std::string& _name,
             const rfl::Ref<const communication::Logger>& _logger,
             Poco::Net::StreamSocket* _socket) const;

  /// Parses a string column.
  containers::ColumnView<strings::String> parse(
      const commands::StringColumnOrStringColumnView& _cmd) const;

 private:
  /// Parses the operator and undertakes a binary operation.
  containers::ColumnView<strings::String> binary_operation(
      const StringBinaryOp& _cmd) const;

  /// Transforms a boolean column to a string.
  containers::ColumnView<strings::String> boolean_as_string(
      const commands::BooleanColumnView& _col) const;

  /// Retrieves a column from a data frame.
  containers::ColumnView<strings::String> get_column(
      const StringColumnOp& _cmd) const;

  /// Transforms a float column to a string.
  containers::ColumnView<strings::String> numerical_as_string(
      const commands::FloatColumnOrFloatColumnView& _col) const;

  /// Returns a subselection on the column.
  containers::ColumnView<strings::String> subselection(
      const StringSubselectionOp& _cmd) const;

  /// Retrieves a substring from a string.
  containers::ColumnView<strings::String> substring(
      const StringSubstringOp& _cmd) const;

  /// Transforms an int column to a column view.
  containers::ColumnView<strings::String> to_view(
      const containers::Column<Int>& _col,
      const rfl::Ref<const containers::Encoding>& _encoding) const;

  /// Transforms a string column to a column view.
  containers::ColumnView<strings::String> to_view(
      const containers::Column<strings::String>& _col) const;

  /// Parses the operator and undertakes a unary operation.
  containers::ColumnView<strings::String> unary_operation(
      const StringUnaryOp& _cmd) const;

  /// Returns an updated version of the column.
  containers::ColumnView<strings::String> update(
      const StringUpdateOp& _cmd) const;

  /// Returns a new column with new subroles.
  containers::ColumnView<strings::String> with_subroles(
      const StringWithSubrolesOp& _cmd) const;

  /// Returns a new column with a new unit.
  containers::ColumnView<strings::String> with_unit(
      const StringWithUnitOp& _cmd) const;

  /// Undertakes a binary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<strings::String> bin_op(const StringBinaryOp& _cmd,
                                                 const Operator& _op) const {
    const auto operand1 = parse(*_cmd.operand1());
    const auto operand2 = parse(*_cmd.operand2());
    return containers::ColumnView<strings::String>::from_bin_op(operand1,
                                                                operand2, _op);
  }

 private:
  /// Encodes the categories used.
  const rfl::Ref<const containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const rfl::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const rfl::Ref<const containers::Encoding> join_keys_encoding_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_STRINGOPPARSER_HPP_
