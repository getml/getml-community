#ifndef RELMT_CONTAINERS_RESCALED_HPP_
#define RELMT_CONTAINERS_RESCALED_HPP_

// -------------------------------------------------------------------------

#include <memory>
#include <vector>

// -------------------------------------------------------------------------

#include "debug/debug.hpp"

// -------------------------------------------------------------------------

#include "relboost/Float.hpp"

// -------------------------------------------------------------------------
namespace relmt {
namespace containers {
// -------------------------------------------------------------------------

class Rescaled {
 public:
  typedef typename std::vector<size_t> MapType;

 public:
  Rescaled(const std::shared_ptr<std::vector<Float>>& _data,
           const size_t _nrows, const size_t _ncols,
           const std::shared_ptr<const MapType> _rows_map)
      : data_(_data), ncols_(_ncols), nrows_(_nrows), rows_map_(_rows_map) {
    assert_true(data_);
    assert_true(rows_map_);
    assert_true(rows_map().size() == nrows_);
  }

 public:
  /// The number of columns.
  const size_t ncols() const { return ncols_; }

  /// The number of rows.
  const size_t nrows() const { return nrows_; }

  /// Returns a pointer to the first element in row _i.
  const Float* row(size_t _i) const {
    assert_true(_i < rows_map().size());
    if (ncols_ == 0) {
      return nullptr;
    }
    assert_true(rows_map()[_i] * ncols_ < data_->size());
    return data_->data() + rows_map()[_i] * ncols_;
  }

 private:
  /// Trivial (private) accessor
  const MapType& rows_map() const { return *rows_map_; }

 private:
  /// The underlying data
  const std::shared_ptr<const std::vector<Float>> data_;

  /// The number of columns
  const size_t ncols_;

  /// The number of rows
  const size_t nrows_;

  /// Indices indicating all of the rows that are part of this view
  const std::shared_ptr<const MapType> rows_map_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

#endif  // RELMT_CONTAINERS_RESCALED_HPP_
