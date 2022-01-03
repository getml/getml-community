#ifndef PREDICTORS_XGBOOSTMATRIX_HPP_
#define PREDICTORS_XGBOOSTMATRIX_HPP_

// ----------------------------------------------------------------------

#include <xgboost/c_api.h>

// ----------------------------------------------------------------------

#include <functional>
#include <memory>

// -----------------------------------------------------------------------------

#include "predictors/XGBoostIterator.hpp"

// ----------------------------------------------------------------------

namespace predictors {

struct XGBoostMatrix {
  typedef XGBoostIterator::DMatrixDestructor DMatrixDestructor;
  typedef XGBoostIterator::DMatrixPtr DMatrixPtr;

  /// A pointer to the underlying matrix.
  DMatrixPtr d_matrix_;

  /// A pointer to the underlying iterator, if such an iterator exists.
  std::unique_ptr<XGBoostIterator> iter_;

  /// Returns the underlying handle
  DMatrixHandle* get() const { return d_matrix_.get(); }
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTMATRIX_HPP_
