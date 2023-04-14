// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_BOOLOPPARSER_HPP_
#define ENGINE_HANDLERS_BOOLOPPARSER_HPP_

#include <map>
#include <memory>
#include <string>

#include "commands/BooleanColumnView.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "json/json.hpp"

namespace engine {
namespace handlers {

class BoolOpParser {
 public:
  typedef containers::ColumnView<bool>::UnknownSize UnknownSize;
  typedef containers::ColumnView<bool>::NRowsType NRowsType;
  typedef containers::ColumnView<bool>::ValueFunc ValueFunc;

  typedef typename commands::BooleanColumnView::BooleanConstOp BooleanConstOp;
  typedef typename commands::BooleanColumnView::BooleanBinaryOp BooleanBinaryOp;
  typedef typename commands::BooleanColumnView::BooleanIsInfOp BooleanIsInfOp;
  typedef typename commands::BooleanColumnView::BooleanIsNullOp BooleanIsNullOp;
  typedef typename commands::BooleanColumnView::BooleanNotOp BooleanNotOp;
  typedef typename commands::BooleanColumnView::BooleanNumComparisonOp
      BooleanNumComparisonOp;
  typedef typename commands::BooleanColumnView::BooleanStrComparisonOp
      BooleanStrComparisonOp;
  typedef typename commands::BooleanColumnView::BooleanSubselectionOp
      BooleanSubselectionOp;
  typedef typename commands::BooleanColumnView::BooleanUpdateOp BooleanUpdateOp;

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
  BoolOpParser(
      const fct::Ref<const containers::Encoding>& _categories,
      const fct::Ref<const containers::Encoding>& _join_keys_encoding,
      const fct::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~BoolOpParser() = default;

 public:
  /// Parses a numerical column.
  containers::ColumnView<bool> parse(
      const commands::BooleanColumnView& _cmd) const;

 private:
  /// Parses the operator and undertakes a binary operation.
  containers::ColumnView<bool> binary_operation(
      const BooleanBinaryOp& _cmd) const;

  /// Parses the operator and undertakes a unary operation.
  containers::ColumnView<bool> is_null(const BooleanIsNullOp& _cmd) const;

  /// Handles any kind of comparison between two numerical columns.
  containers::ColumnView<bool> numerical_comparison(
      const BooleanNumComparisonOp& _cmd) const;

  /// Handles any kind of comparison between two string columns.
  containers::ColumnView<bool> string_comparison(
      const BooleanStrComparisonOp& _cmd) const;

  /// Returns a subselection on the column.
  containers::ColumnView<bool> subselection(
      const BooleanSubselectionOp& _cmd) const;

  /// Returns an updated version of the column.
  containers::ColumnView<bool> update(const BooleanUpdateOp& _cmd) const;

  /// Undertakes a binary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<bool> bin_op(const BooleanBinaryOp& _col,
                                      const Operator& _op) const {
    const auto operand1 = parse(*_col.get<"operand1_">());
    const auto operand2 = parse(*_col.get<"operand2_">());
    return containers::ColumnView<bool>::from_bin_op(operand1, operand2, _op);
  }

  /// Undertakes a unary operation based on template class
  /// Operator for categorical columns.
  template <class Operator>
  containers::ColumnView<bool> cat_un_op(
      const commands::StringColumnOrStringColumnView& _col,
      const Operator& _op) const {
    const auto operand1 =
        StringOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(_col);
    return containers::ColumnView<bool>::from_un_op(operand1, _op);
  }

  /// Undertakes a binary operation based on template class
  /// Operator for categorical columns.
  template <class Operator>
  containers::ColumnView<bool> cat_bin_op(const BooleanStrComparisonOp& _cmd,
                                          const Operator& _op) const {
    const auto operand1 =
        StringOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(*_cmd.get<"operand1_">());
    const auto operand2 =
        StringOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(*_cmd.get<"operand2_">());
    return containers::ColumnView<bool>::from_bin_op(operand1, operand2, _op);
  }

  /// Undertakes a binary operation based on template class
  /// Operator for numerical columns.
  template <class Operator>
  containers::ColumnView<bool> num_bin_op(const BooleanNumComparisonOp& _cmd,
                                          const Operator& _op) const {
    const auto operand1 =
        FloatOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(*_cmd.get<"operand1_">());
    const auto operand2 =
        FloatOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(*_cmd.get<"operand2_">());
    return containers::ColumnView<bool>::from_bin_op(operand1, operand2, _op);
  }

  /// Undertakes a unary operation based on template class
  /// Operator for numerical columns.
  template <class Operator>
  containers::ColumnView<bool> num_un_op(
      const commands::FloatColumnOrFloatColumnView& _col,
      const Operator& _op) const {
    const auto operand1 =
        FloatOpParser(categories_, join_keys_encoding_, data_frames_)
            .parse(_col);
    return containers::ColumnView<bool>::from_un_op(operand1, _op);
  }

 private:
  /// Encodes the categories used.
  const fct::Ref<const containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const fct::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const fct::Ref<const containers::Encoding> join_keys_encoding_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_BOOLOPPARSER_HPP_
