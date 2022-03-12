#ifndef HELPERS_COLUMNVIEW_HPP_
#define HELPERS_COLUMNVIEW_HPP_

// -------------------------------------------------------------------------

#include <map>
#include <memory>
#include <type_traits>
#include <vector>

// -------------------------------------------------------------------------

#include "helpers/Column.hpp"
#include "helpers/Int.hpp"

// -------------------------------------------------------------------------

namespace helpers {

template <class T, class ContainerType>
class ColumnView {
 public:
  ColumnView(const Column<T>& _col)
      : col_(_col), rows_(std::shared_ptr<const ContainerType>()) {}

  ColumnView(const Column<T>& _col,
             const std::shared_ptr<const ContainerType>& _rows)
      : col_(_col), rows_(_rows) {
#ifndef NDEBUG
    assert_true(rows_);
    if constexpr (std::is_same<ContainerType, std::vector<size_t>>()) {
      for (const auto row : *rows_) {
        assert_msg(row < col_.nrows_,
                   "row: " + std::to_string(row) +
                       ", col_.nrows_:" + std::to_string(col_.nrows_));
      }
    }
#endif  // NDEBUG
  }

  ~ColumnView() = default;

 public:
  /// Iterator begin
  inline auto begin() const {
    return fct::AccessIterator<T, ColumnView<T, ContainerType>>(0, this);
  }

  /// Returns the underlying column.
  inline const Column<T>& col() const { return col_; }

  /// Iterator end
  inline auto end() const { return begin() + nrows(); }

  /// Returns the number of rows.
  inline size_t nrows() const {
    assert_true(rows_);
    return rows_->size();
  }

  /// Accessor to data (when rows are std::vector<Int>)
  template <typename CType = ContainerType,
            typename std::enable_if<
                std::is_same<CType, std::vector<size_t>>::value, int>::type = 0>
  inline T operator[](const size_t _i) const {
    assert_true(rows_);
    assert_true(_i < rows_->size());
    assert_msg((*rows_)[_i] < col_.nrows_,
               "(*rows_)[_i]: " + std::to_string((*rows_)[_i]) +
                   ", col_.nrows_: " + std::to_string(col_.nrows_));
    return col_[(*rows_)[_i]];
  }

  /// Accessor to data (when rows are std::map<Int, Int>)
  template <typename CType = ContainerType,
            typename std::enable_if<
                std::is_same<CType, std::map<Int, Int>>::value, int>::type = 0>
  inline T operator[](const Int _i) const {
    assert_true(_i >= 0);
    assert_true(rows_);
    auto it = rows_->find(_i);
    assert_true(it != rows_->end());
    assert_true(it->second < col_.nrows_);
    return col_[it->second];
  }

  /// Returns the underlying column.
  inline const ContainerType& rows() const {
    assert_true(rows_);
    return rows_;
  }

 private:
  /// Shallow copy of the column in which we are interested.
  const Column<T> col_;

  /// Indices indicating all of the rows that are part of this view
  const std::shared_ptr<const ContainerType> rows_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_COLUMNVIEW_HPP_
