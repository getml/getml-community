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
// -------------------------------------------------------------------------

template <class T, class ContainerType>
class ColumnView {
  // -------------------------------

 public:
  ColumnView(const Column<T>& _col)
      : col_(_col), rows_(std::shared_ptr<const ContainerType>()) {}

  ColumnView(const Column<T>& _col,
             const std::shared_ptr<const ContainerType>& _rows)
      : col_(_col), rows_(_rows) {}

  ~ColumnView() = default;

  // -------------------------------

  /// Returns the underlying column.
  inline const Column<T>& col() const { return col_; }

  /// Accessor to data (when rows are std::vector<Int>)
  template <typename CType = ContainerType,
            typename std::enable_if<
                std::is_same<CType, std::vector<size_t>>::value, int>::type = 0>
  inline T operator[](const size_t _i) const {
    assert_true(rows_);
    assert_true(_i < rows_->size());
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
    return col_[it->second];
  }

  // -------------------------------

 private:
  /// Shallow copy of the column in which we are interested.
  Column<T> col_;

  /// Indices indicating all of the rows that are part of this view
  std::shared_ptr<const ContainerType> rows_;

  // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_COLUMNVIEW_HPP_
