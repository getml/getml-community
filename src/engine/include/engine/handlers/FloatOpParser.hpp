// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_FLOATOPPARSER_HPP_
#define ENGINE_HANDLERS_FLOATOPPARSER_HPP_

#include <Poco/JSON/Object.h>

#include <map>
#include <memory>
#include <random>
#include <string>

#include "commands/FloatColumnOrFloatColumnView.hpp"
#include "debug/debug.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "json/json.hpp"

namespace engine {
namespace handlers {

class FloatOpParser {
 public:
  typedef typename containers::ColumnView<bool>::UnknownSize UnknownSize;
  typedef typename containers::ColumnView<bool>::NRowsType NRowsType;
  typedef typename containers::ColumnView<bool>::ValueFunc ValueFunc;

  typedef typename commands::FloatColumnOrFloatColumnView::FloatArangeOp
      FloatArangeOp;
  typedef
      typename commands::FloatColumnOrFloatColumnView::FloatAsTSOp FloatAsTSOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatBinaryOp
      FloatBinaryOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatColumnOp
      FloatColumnOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatConstOp
      FloatConstOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatFromBooleanOp
      FloatFromBooleanOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatFromStringOp
      FloatFromStringOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatRandomOp
      FloatRandomOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatSubselectionOp
      FloatSubselectionOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatUnaryOp
      FloatUnaryOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatUpdateOp
      FloatUpdateOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatWithUnitOp
      FloatWithUnitOp;
  typedef typename commands::FloatColumnOrFloatColumnView::FloatWithSubrolesOp
      FloatWithSubrolesOp;

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
  FloatOpParser(
      const fct::Ref<const containers::Encoding>& _categories,
      const fct::Ref<const containers::Encoding>& _join_keys_encoding,
      const fct::Ref<const std::map<std::string, containers::DataFrame>>&
          _data_frames)
      : categories_(_categories),
        data_frames_(_data_frames),
        join_keys_encoding_(_join_keys_encoding) {}

  ~FloatOpParser() = default;

 public:
  /// Checks a column for any obvious issues (such as high share of NULL
  /// values).
  void check(const containers::Column<Float>& _col,
             const fct::Ref<const communication::Logger>& _logger,
             Poco::Net::StreamSocket* _socket) const;

  /// Parses a numerical column.
  containers::ColumnView<Float> parse(
      const commands::FloatColumnOrFloatColumnView& _cmd) const;

  /// TODO: Remove this temporary solution.
  containers::ColumnView<Float> parse(const Poco::JSON::Object& _cmd) const {
    const auto cmd =
        json::from_json<commands::FloatColumnOrFloatColumnView>(_cmd);
    return parse(cmd);
  }

 private:
  /// Implements numpy's arange in a lazy fashion.
  containers::ColumnView<Float> arange(const FloatArangeOp& _col) const;

  /// Transforms a string column to a float.
  containers::ColumnView<Float> as_num(const FloatFromStringOp& _col) const;

  /// Transforms a string column to a time stamp.
  containers::ColumnView<Float> as_ts(const FloatAsTSOp& _col) const;

  /// Parses the operator and undertakes a binary operation.
  containers::ColumnView<Float> binary_operation(
      const FloatBinaryOp& _col) const;

  /// Transforms a boolean column to a float column.
  containers::ColumnView<Float> boolean_as_num(
      const FloatFromBooleanOp& _cmd) const;

  /// Retrieves a float column from a string.
  containers::ColumnView<Float> from_string(
      const FloatFromStringOp& _col) const;

  /// Returns an actual column.
  containers::ColumnView<Float> get_column(const FloatColumnOp& _cmd) const;

  /// Returns a subselection on the column.
  containers::ColumnView<Float> subselection(
      const FloatSubselectionOp& _cmd) const;

  /// Parses the operator and undertakes a unary operation.
  containers::ColumnView<Float> unary_operation(const FloatUnaryOp& _col) const;

  /// Returns an updated version of the column.
  containers::ColumnView<Float> update(const FloatUpdateOp& _cmd) const;

  /// Returns a new column with new subroles.
  containers::ColumnView<Float> with_subroles(
      const FloatWithSubrolesOp& _col) const;

  /// Returns a new column with a new unit.
  containers::ColumnView<Float> with_unit(const FloatWithUnitOp& _col) const;

  /// Undertakes a binary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<Float> bin_op(const FloatBinaryOp& _cmd,
                                       const Operator& _op) const {
    const auto operand1 = parse(*_cmd.get<"operand1_">());
    const auto operand2 = parse(*_cmd.get<"operand2_">());
    return containers::ColumnView<Float>::from_bin_op(operand1, operand2, _op);
  }

  /// Returns a columns containing random values.
  containers::ColumnView<Float> random(const FloatRandomOp& _cmd) const {
    const auto seed = _cmd.get<"seed_">();

    auto rng = std::mt19937(seed);

    std::uniform_real_distribution<Float> dis(0.0, 1.0);

    size_t next = 0;

    const auto value_func = [rng, dis, next,
                             seed](size_t _i) mutable -> std::optional<Float> {
      if (_i > next) {
        rng.discard(_i - next);
      }

      if (_i < next) {
        rng = std::mt19937(seed);
        rng.discard(_i);
      }

      next = _i + 1;

      return dis(rng);
    };

    return containers::ColumnView<Float>(value_func, NROWS_INFINITE);
  }

  /// Returns a columns containing the rowids.
  containers::ColumnView<Float> rowid() const {
    const auto value_func = [](size_t _i) -> std::optional<Float> {
      return static_cast<Float>(_i);
    };

    return containers::ColumnView<Float>(value_func, NROWS_INFINITE);
  }

  /// Undertakes a unary operation based on template class
  /// Operator.
  template <class Operator>
  containers::ColumnView<Float> un_op(const FloatUnaryOp& _cmd,
                                      const Operator& _op) const {
    const auto operand1 = parse(*_cmd.get<"operand1_">());
    return containers::ColumnView<Float>::from_un_op(operand1, _op);
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

#endif  // ENGINE_HANDLERS_FLOATOPPARSER_HPP_
