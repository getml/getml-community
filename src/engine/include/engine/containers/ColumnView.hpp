#ifndef ENGINE_CONTAINERS_COLUMNVIEW_HPP_
#define ENGINE_CONTAINERS_COLUMNVIEW_HPP_

// -------------------------------------------------------------------------

#include <arrow/api.h>

// -------------------------------------------------------------------------

#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

// -------------------------------------------------------------------------

#include "engine/ULong.hpp"
#include "engine/config/config.hpp"

// -------------------------------------------------------------------------

#include "engine/containers/ColumnViewIterator.hpp"

// -------------------------------------------------------------------------

namespace engine {
namespace containers {

template <class T>
class ColumnView {
 public:
  typedef T value_type;
  typedef bool UnknownSize;
  typedef std::variant<size_t, UnknownSize> NRowsType;
  typedef std::function<std::optional<T>(size_t)> ValueFunc;

  static constexpr UnknownSize NOT_KNOWABLE = true;
  static constexpr UnknownSize NROWS_INFINITE = false;

  static constexpr bool NROWS_MUST_MATCH = true;

 public:
  ColumnView(const ValueFunc& _value_func, const NRowsType& _nrows,
             const std::vector<std::string>& _subroles = {},
             const std::string& _unit = "")
      : nrows_(_nrows),
        subroles_(_subroles),
        unit_(_unit),
        value_func_(_value_func) {
    static_assert(!std::is_same<T, void>(), "Type cannot be void!");
  }

  ~ColumnView() {}

  /// Constructs a column view from a binary operation.
  template <class T1, class T2, class Operator>
  static ColumnView<T> from_bin_op(const ColumnView<T1>& _operand1,
                                   const ColumnView<T2>& _operand2,
                                   const Operator _op);

  /// Constructs a new column from a boolean subselection.
  static ColumnView<T> from_boolean_subselection(
      const ColumnView<T>& _data, const ColumnView<bool>& _indices);

  /// Constructs a new column from a numerical subselection.
  static ColumnView<T> from_numerical_subselection(
      const ColumnView<T>& _data, const ColumnView<Float>& _indices);

  /// Constructs a column view from a column.
  static ColumnView<T> from_column(const Column<T>& _col);

  /// Constructs a column view from a unary operator.
  template <class T1, class Operator>
  static ColumnView<T> from_un_op(const ColumnView<T1>& _operand,
                                  const Operator& _op);

  /// Constructs a column view from a ternary operation.
  template <class T1, class T2, class T3, class Operator>
  static ColumnView<T> from_tern_op(const ColumnView<T1>& _operand1,
                                    const ColumnView<T2>& _operand2,
                                    const ColumnView<T3>& _operand3,
                                    const Operator _op);

  /// Constructs a column view from a value.
  static ColumnView<T> from_value(const T _value);

 public:
  /// Returns the number of rows, calculates them if necessary,
  /// returns std::nullopt if and only if the number is infinite.
  std::optional<size_t> calc_nrows() const;

  /// Iterator to the beginning.
  ColumnViewIterator<T> begin() const {
    return ColumnViewIterator<T>(value_func_);
  }

  /// Iterator to the end.
  ColumnViewIterator<T> end() const { return ColumnViewIterator<T>(); }

  /// Transforms the ColumnView to an arrow::ChunkedArray.
  std::shared_ptr<arrow::ChunkedArray> to_array(
      const size_t _begin, const std::optional<size_t> _expected_length,
      const bool _nrows_must_match) const;

  /// Transforms the ColumnView to a physical column.
  auto to_column(const size_t _begin,
                 const std::optional<size_t> _expected_length,
                 const bool _nrows_must_match) const;

  /// Transforms the ColumnView to a physical vector.
  std::shared_ptr<std::vector<T>> to_vector(
      const size_t _begin, const std::optional<size_t> _expected_length,
      const bool _nrows_must_match) const;

  /// Returns all unique values in the column view.
  std::shared_ptr<arrow::ChunkedArray> unique() const;

  /// Returns a new column view with new subroles.
  ColumnView<T> with_subroles(const std::vector<std::string>& _subroles) const;

  /// Returns a new column view with a new unit.
  ColumnView<T> with_unit(const std::string& _unit) const;

 public:
  /// Whether the column view is infinite.
  bool is_infinite() const {
    return std::holds_alternative<UnknownSize>(nrows()) &&
           std::get<UnknownSize>(nrows()) == NROWS_INFINITE;
  }

  /// Accessor to data
  std::optional<T> operator[](const size_t _i) const { return value_func_(_i); }

  /// Trivial getter
  NRowsType nrows() const { return nrows_; }

  /// Trivial getter
  std::string nrows_to_str() const {
    if (std::holds_alternative<size_t>(nrows())) {
      return std::to_string(std::get<size_t>(nrows()));
    }

    if (std::get<UnknownSize>(nrows()) == NROWS_INFINITE) {
      return "infinite";
    }

    return "unknown";
  }

  /// Trivial getter
  const std::vector<std::string>& subroles() const { return subroles_; }

  /// Trivial getter
  const std::string& unit() const { return unit_; }

 private:
  /// Calculates the expected length of a vector.
  std::pair<size_t, bool> calc_expected_length(
      const size_t _begin, const std::optional<size_t> _expected_length,
      const bool _nrows_must_match) const;

  /// Checks whether the expected length was exceeded.
  void check_exceeds_expected(const size_t _begin, const bool _nrows_must_match,
                              const size_t _expected_length) const;

  /// Checks the plausibility of the value passed to expected length.
  void check_expected_length(const size_t _expected_length,
                             const bool _nrows_must_match,
                             const bool _expected_length_not_passed) const;

  /// Generates the arrow::ChunkedArray.
  template <class IteratorType>
  std::shared_ptr<arrow::ChunkedArray> make_array(
      const IteratorType _begin, const IteratorType _end) const;

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
#else
  /// Generates the vector in parallel.
  std::shared_ptr<std::vector<T>> make_parallel(
      const size_t _begin, const size_t _expected_length) const;
#endif

  /// Generates the vector sequentially.
  std::shared_ptr<std::vector<T>> make_sequential(
      const size_t _begin, const size_t _expected_length,
      const bool _nrows_must_match) const;

 private:
  /// Functiona returning the Number of rows (if that is knowable).
  const NRowsType nrows_;

  /// The subroles of the column view.
  const std::vector<std::string> subroles_;

  /// Unit of the column.
  const std::string unit_;

  /// The function returning the actual data point.
  const ValueFunc value_func_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
std::optional<size_t> ColumnView<T>::calc_nrows() const {
  if (std::holds_alternative<size_t>(nrows())) {
    return std::get<size_t>(nrows());
  }

  if (is_infinite()) {
    return std::nullopt;
  }

  for (size_t i = 0; true; ++i) {
    if (!value_func_(i)) {
      return i;
    }
  }

  return std::nullopt;
}

// -------------------------------------------------------------------------

template <class T>
template <class T1, class T2, class Operator>
ColumnView<T> ColumnView<T>::from_bin_op(const ColumnView<T1>& _operand1,
                                         const ColumnView<T2>& _operand2,
                                         const Operator _op) {
  const auto check_nrows = [](const auto _operand, const auto _op) {
    const bool nrows_do_not_match =
        (std::holds_alternative<size_t>(_operand.nrows()) ||
         std::get<UnknownSize>(_operand.nrows()) != NROWS_INFINITE) &&
        _op;

    if (nrows_do_not_match) {
      throw std::runtime_error(
          "Number of rows between two columns do not "
          "match, which is necessary for binary "
          "operations to be possible.");
    }
  };

  const auto value_func = [_operand1, _operand2, _op,
                           check_nrows](const size_t _i) -> std::optional<T> {
    const auto op1 = _operand1[_i];
    const auto op2 = _operand2[_i];

    if (!op1) {
      check_nrows(_operand2, op2);
      return std::nullopt;
    }

    if (!op2) {
      check_nrows(_operand1, op1);
      return std::nullopt;
    }

    return _op(*op1, *op2);
  };

  const auto check_same_size = [_operand1, _operand2]() {
    const bool nrows_do_not_match =
        std::holds_alternative<size_t>(_operand2.nrows()) &&
        std::get<size_t>(_operand1.nrows()) !=
            std::get<size_t>(_operand2.nrows());

    if (nrows_do_not_match) {
      throw std::runtime_error(
          "Number of rows between two columns do not "
          "match, which is necessary for binary "
          "operations to be possible: " +
          std::to_string(std::get<size_t>(_operand1.nrows())) + " vs. " +
          std::to_string(std::get<size_t>(_operand2.nrows())) + ".");
    }
  };

  const auto nrows_func = [_operand1, _operand2,
                           check_same_size]() -> NRowsType {
    if (std::holds_alternative<size_t>(_operand1.nrows())) {
      check_same_size();
      return _operand1.nrows();
    }

    if (std::holds_alternative<size_t>(_operand2.nrows())) {
      // check_same_size() not necessary, because we wouldn't get
      // here otherwise.
      return _operand2.nrows();
    }

    return std::get<UnknownSize>(_operand1.nrows()) ||
           std::get<UnknownSize>(_operand2.nrows());
  };

  return ColumnView<T>(value_func, nrows_func());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::from_boolean_subselection(
    const ColumnView<T>& _data, const ColumnView<bool>& _indices) {
  const bool nrows_do_not_match =
      std::holds_alternative<size_t>(_data.nrows()) &&
      std::holds_alternative<size_t>(_indices.nrows()) &&
      std::get<size_t>(_data.nrows()) != std::get<size_t>(_indices.nrows());

  if (nrows_do_not_match) {
    throw std::runtime_error(
        "Number of rows between two columns do not "
        "match, which is necessary for subselection "
        "operations on a boolean column to be possible: " +
        std::to_string(std::get<size_t>(_data.nrows())) + " vs. " +
        std::to_string(std::get<size_t>(_indices.nrows())) + ".");
  }

  if (_data.is_infinite()) {
    throw std::runtime_error(
        "The data or the indices must be finite for a boolean "
        "subselection to work!");
  }

  const auto find_next =
      [_indices, _data](size_t _begin, size_t _skip) -> std::optional<size_t> {
    size_t ix = _begin;

    size_t count = 0;

    while (true) {
      const auto val = _indices[ix];

      if (!val) {
        if (_data[ix]) {
          throw std::runtime_error(
              "Number of rows do not match on the "
              "boolean subselection. The data is longer "
              "than the indices.");
        }

        return std::nullopt;
      }

      if (!_data[ix]) {
        if (!_indices.is_infinite()) {
          throw std::runtime_error(
              "Number of rows do not match on the "
              "boolean subselection. The indices are "
              "longer than the data. This may only be "
              "the case if the indices are infinite.");
        }

        return std::nullopt;
      }

      if (*val) {
        if (++count > _skip) {
          return ix;
        }
      }

      ++ix;
    }

    assert_true(false);

    return 0;
  };

  size_t ix = 0;

  size_t next = 0;

  const auto value_func = [_data, _indices, ix, next, find_next](
                              const size_t _i) mutable -> std::optional<T> {
    if (_i == next) [[likely]] {
      const auto new_ix = find_next(ix, 0);
      if (!new_ix) {
        return std::nullopt;
      }
      ix = *new_ix;
    } else if (_i < next) [[unlikely]] {
      const auto new_ix = find_next(0, _i);
      if (!new_ix) {
        return std::nullopt;
      }
      ix = *new_ix;
    } else if (_i > next) [[unlikely]] {
      const auto new_ix = find_next(ix, _i - next);
      if (!new_ix) {
        return std::nullopt;
      }
      ix = *new_ix;
    }

    next = _i + 1;

    return _data[ix++];
  };

  return ColumnView<T>(value_func, NOT_KNOWABLE, _data.subroles(),
                       _data.unit());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::from_numerical_subselection(
    const ColumnView<T>& _data, const ColumnView<Float>& _indices) {
  const auto value_func = [_data,
                           _indices](const size_t _i) -> std::optional<T> {
    const auto ix_float = _indices[_i];

    if (!ix_float) {
      return std::nullopt;
    }

    assert_true(*ix_float >= 0.0);

    if (*ix_float < 0.0) {
      throw std::runtime_error(
          "Index on a numerical subselection cannot be smaller than "
          "zero!");
    }

    const auto ix = static_cast<size_t>(*ix_float);

    const auto d = _data[ix];

    assert_true(d);

    if (!d) {
      throw std::runtime_error(
          "Index on a numerical subselection out of bounds!");
    }

    return *d;
  };

  return ColumnView<T>(value_func, _indices.nrows(), _data.subroles(),
                       _data.unit());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::from_column(const Column<T>& _col) {
  const auto value_func = [_col](const size_t _i) -> std::optional<T> {
    if (_i >= _col.nrows()) {
      return std::nullopt;
    };

    return _col[_i];
  };

  return ColumnView<T>(value_func, _col.nrows(), _col.subroles(), _col.unit());
}

// -------------------------------------------------------------------------

template <class T>
template <class T1, class Operator>
ColumnView<T> ColumnView<T>::from_un_op(const ColumnView<T1>& _operand,
                                        const Operator& _op) {
  const auto value_func = [_operand, _op](const size_t _i) -> std::optional<T> {
    const auto op1 = _operand[_i];

    if (!op1) {
      return std::nullopt;
    }

    return _op(*op1);
  };

  return ColumnView<T>(value_func, _operand.nrows());
}

// -------------------------------------------------------------------------

template <class T>
template <class T1, class T2, class T3, class Operator>
ColumnView<T> ColumnView<T>::from_tern_op(const ColumnView<T1>& _operand1,
                                          const ColumnView<T2>& _operand2,
                                          const ColumnView<T3>& _operand3,
                                          const Operator _op) {
  const auto check_nrows = [](const auto _operand, const auto _op) {
    const bool nrows_do_not_match =
        (std::holds_alternative<size_t>(_operand.nrows()) ||
         std::get<UnknownSize>(_operand.nrows()) != NROWS_INFINITE) &&
        _op;

    if (nrows_do_not_match) {
      throw std::runtime_error(
          "Number of rows between two columns do not "
          "match, which is necessary for ternary "
          "operations to be possible.");
    }
  };

  const auto value_func = [_operand1, _operand2, _operand3, _op,
                           check_nrows](const size_t _i) -> std::optional<T> {
    const auto op1 = _operand1[_i];
    const auto op2 = _operand2[_i];
    const auto op3 = _operand3[_i];

    if (!op1) {
      check_nrows(_operand2, op2);
      check_nrows(_operand3, op3);
      return std::nullopt;
    }

    if (!op2) {
      check_nrows(_operand1, op1);
      check_nrows(_operand3, op3);
      return std::nullopt;
    }

    if (!op3) {
      check_nrows(_operand1, op1);
      check_nrows(_operand2, op2);
      return std::nullopt;
    }

    return _op(*op1, *op2, *op3);
  };

  const auto check_same_size = [](const auto& _operand1,
                                  const auto& _operand2) {
    const bool nrows_do_not_match =
        std::holds_alternative<size_t>(_operand2.nrows()) &&
        std::get<size_t>(_operand1.nrows()) !=
            std::get<size_t>(_operand2.nrows());

    if (nrows_do_not_match) {
      throw std::runtime_error(
          "Number of rows between two columns do not "
          "match, which is necessary for ternary "
          "operations to be possible.");
    }
  };

  const auto nrows_func = [_operand1, _operand2, _operand3,
                           check_same_size]() -> NRowsType {
    if (std::holds_alternative<size_t>(_operand1.nrows())) {
      check_same_size(_operand1, _operand2);
      check_same_size(_operand1, _operand3);
      return _operand1.nrows();
    }

    if (std::holds_alternative<size_t>(_operand2.nrows())) {
      check_same_size(_operand2, _operand3);
      return _operand2.nrows();
    }

    if (std::holds_alternative<size_t>(_operand3.nrows())) {
      return _operand3.nrows();
    }

    return std::get<UnknownSize>(_operand1.nrows()) ||
           std::get<UnknownSize>(_operand2.nrows()) ||
           std::get<UnknownSize>(_operand3.nrows());
  };

  return ColumnView<T>(value_func, nrows_func());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::from_value(const T _value) {
  const auto value_func = [_value](const size_t _i) -> std::optional<T> {
    return _value;
  };

  return ColumnView<T>(value_func, NROWS_INFINITE);
}

// -------------------------------------------------------------------------

template <class T>
template <class IteratorType>
std::shared_ptr<arrow::ChunkedArray> ColumnView<T>::make_array(
    const IteratorType _begin, const IteratorType _end) const {
  if constexpr (std::is_same<T, bool>()) {
    return ArrayMaker::make_boolean_array(_begin, _end);
  }

  if constexpr (std::is_same<T, Float>()) {
    if (unit().find("time stamp") != std::string::npos) {
      return ArrayMaker::make_time_stamp_array(_begin, _end);
    }

    return ArrayMaker::make_float_array(_begin, _end);
  }

  if constexpr (std::is_same<T, std::string>()) {
    return ArrayMaker::make_string_array(_begin, _end);
  }
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<arrow::ChunkedArray> ColumnView<T>::to_array(
    const size_t _begin, const std::optional<size_t> _expected_length,
    const bool _nrows_must_match) const {
  assert_true(_expected_length || !_nrows_must_match);

  const auto [expected_length, length_is_known] =
      calc_expected_length(_begin, _expected_length, _nrows_must_match);

  check_expected_length(expected_length, _nrows_must_match, !_expected_length);

  return make_array(begin(), end());
}

// -------------------------------------------------------------------------

template <class T>
auto ColumnView<T>::to_column(const size_t _begin,
                              const std::optional<size_t> _expected_length,
                              const bool _nrows_must_match) const {
  const auto data_ptr = to_vector(_begin, _expected_length, _nrows_must_match);

  if constexpr (!std::is_same<T, std::string>()) {
    auto col = Column<T>(data_ptr);
    col.set_unit(unit());
    return col;
  }

  if constexpr (std::is_same<T, std::string>()) {
    assert_true(data_ptr);
    const auto to_str = [](const std::string& _str) -> strings::String {
      return strings::String(_str);
    };
    const auto range = *data_ptr | VIEWS::transform(to_str);
    const auto new_data_ptr = std::make_shared<std::vector<strings::String>>(
        range.begin(), range.end());
    auto col = Column<strings::String>(new_data_ptr);
    col.set_unit(unit());
    return col;
  }
}

// -------------------------------------------------------------------------

template <class T>
std::pair<size_t, bool> ColumnView<T>::calc_expected_length(
    const size_t _begin, const std::optional<size_t> _expected_length,
    const bool _nrows_must_match) const {
  if (_expected_length) {
    return std::make_pair(*_expected_length, _nrows_must_match);
  }

  if (std::holds_alternative<size_t>(nrows())) {
    return std::make_pair(std::get<size_t>(nrows()) - _begin, true);
  }

  return std::make_pair(std::numeric_limits<size_t>::max(), false);
}

// -------------------------------------------------------------------------

template <class T>
void ColumnView<T>::check_exceeds_expected(
    const size_t _begin, const bool _nrows_must_match,
    const size_t _expected_length) const {
  const bool exceeds_expected_by_unknown_number =
      _nrows_must_match && std::holds_alternative<UnknownSize>(nrows()) &&
      std::get<UnknownSize>(nrows()) == NOT_KNOWABLE &&
      value_func_(_begin + _expected_length);

  if (exceeds_expected_by_unknown_number) {
    throw std::runtime_error("Expected " + std::to_string(_expected_length) +
                             " nrows, but there were more.");
  }
}

// -------------------------------------------------------------------------

template <class T>
void ColumnView<T>::check_expected_length(
    const size_t _expected_length, const bool _nrows_must_match,
    const bool _expected_length_not_passed) const {
  const bool nrows_do_not_match = _nrows_must_match &&
                                  std::holds_alternative<size_t>(nrows()) &&
                                  std::get<size_t>(nrows()) != _expected_length;

  if (nrows_do_not_match) {
    throw std::runtime_error("Expected " + std::to_string(_expected_length) +
                             " nrows, but got " +
                             std::to_string(std::get<size_t>(nrows())) + ".");
  }

  if (_expected_length_not_passed && is_infinite()) {
    throw std::runtime_error(
        "The length of the column view is infinite. You can look "
        "at "
        "it, but it cannot be transformed into an actual column "
        "unless "
        "the length can be inferred from somewhere else.");
  }
}

// -------------------------------------------------------------------------

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
#else
template <class T>
std::shared_ptr<std::vector<T>> ColumnView<T>::make_parallel(
    const size_t _begin, const size_t _expected_length) const {
  const auto data_ptr = std::make_shared<std::vector<T>>(_expected_length);

  const auto range = stl::iota<size_t>(0, _expected_length);

  const auto deep_copy = *this;

  const auto extract_value = [deep_copy, _begin, _expected_length,
                              data_ptr](const size_t _i) {
    const auto val = deep_copy.value_func_(_begin + _i);

    if (val) {
      (*data_ptr)[_i] = *val;
    } else {
      throw std::runtime_error("Expected " +
                               std::to_string(_begin + _expected_length) +
                               " elements, but there were fewer.");
    }
  };

  multithreading::parallel_for_each(range.begin(), range.end(), extract_value);

  return data_ptr;
}
#endif

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<std::vector<T>> ColumnView<T>::make_sequential(
    const size_t _begin, const size_t _expected_length,
    const bool _nrows_must_match) const {
  const auto data_ptr = std::make_shared<std::vector<T>>();

  for (size_t i = 0; i < _expected_length; ++i) {
    const auto val = value_func_(_begin + i);

    if (!val) {
      break;
    }

    data_ptr->push_back(*val);
  }

  if (_nrows_must_match && data_ptr->size() != _expected_length) {
    throw std::runtime_error("Expected " + std::to_string(_expected_length) +
                             " nrows, but got " +
                             std::to_string(data_ptr->size()) + ".");
  }

  return data_ptr;
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<std::vector<T>> ColumnView<T>::to_vector(
    const size_t _begin, const std::optional<size_t> _expected_length,
    const bool _nrows_must_match) const {
  assert_true(_expected_length || !_nrows_must_match);

  const auto [expected_length, length_is_known] =
      calc_expected_length(_begin, _expected_length, _nrows_must_match);

  check_expected_length(expected_length, _nrows_must_match, !_expected_length);

#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
  const auto data_ptr =
      make_sequential(_begin, expected_length, _nrows_must_match);
#else
  const auto data_ptr =
      length_is_known
          ? make_parallel(_begin, expected_length)
          : make_sequential(_begin, expected_length, _nrows_must_match);
#endif

  check_exceeds_expected(_begin, _nrows_must_match, expected_length);

  return data_ptr;
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<arrow::ChunkedArray> ColumnView<T>::unique() const {
  if (is_infinite()) {
    throw std::runtime_error(
        "You cannot retrieve unique values from an infinite column!");
  }

  auto unique_values = std::set<T>();

  for (size_t i = 0; true; ++i) {
    const auto val = value_func_(i);

    if (!val) {
      break;
    }

    if (helpers::NullChecker::is_null(*val)) {
      continue;
    }

    unique_values.insert(*val);
  }

  return make_array(unique_values.begin(), unique_values.end());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::with_subroles(
    const std::vector<std::string>& _subroles) const {
  // For checking only
  helpers::SubroleParser::parse(_subroles);

  const auto deep_copy = *this;

  const auto value_func = [deep_copy](const size_t _i) -> std::optional<T> {
    return deep_copy[_i];
  };

  return ColumnView<T>(value_func, nrows(), _subroles, unit());
}

// -------------------------------------------------------------------------

template <class T>
ColumnView<T> ColumnView<T>::with_unit(const std::string& _unit) const {
  const auto deep_copy = *this;

  const auto value_func = [deep_copy](const size_t _i) -> std::optional<T> {
    return deep_copy[_i];
  };

  return ColumnView<T>(value_func, nrows(), subroles(), _unit);
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_COLUMN_HPP_
