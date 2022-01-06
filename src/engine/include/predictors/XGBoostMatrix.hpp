#ifndef PREDICTORS_XGBOOSTMATRIX_HPP_
#define PREDICTORS_XGBOOSTMATRIX_HPP_

// ----------------------------------------------------------------------

#include <xgboost/c_api.h>

// ----------------------------------------------------------------------

#include <functional>
#include <memory>
#include <variant>

// -----------------------------------------------------------------------------

#include "predictors/XGBoostIteratorDense.hpp"
#include "predictors/XGBoostIteratorSparse.hpp"

// ----------------------------------------------------------------------

namespace predictors {

struct XGBoostMatrix {
  typedef XGBoostIteratorDense::DMatrixDestructor DMatrixDestructor;
  typedef XGBoostIteratorDense::DMatrixPtr DMatrixPtr;

  /// A pointer to the underlying matrix.
  DMatrixPtr d_matrix_;

  /// A pointer to the underlying iterator, if such an iterator exists.
  std::variant<std::unique_ptr<XGBoostIteratorDense>,
               std::unique_ptr<XGBoostIteratorSparse>>
      iter_ = std::unique_ptr<XGBoostIteratorDense>();

  /// Returns the underlying handle
  DMatrixHandle* get() const { return d_matrix_.get(); }
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTMATRIX_HPP_
