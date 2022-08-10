// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef PREDICTORS_ENCODING_HPP_
#define PREDICTORS_ENCODING_HPP_

// -----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -----------------------------------------------------------------------------

#include <memory>

// -----------------------------------------------------------------------------

#include "memmap/memmap.hpp"

// -----------------------------------------------------------------------------

#include "predictors/Float.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/Int.hpp"
#include "predictors/IntFeature.hpp"
#include "predictors/JSON.hpp"

// -----------------------------------------------------------------------------

namespace predictors {

class Encoding {
 public:
  Encoding() : max_(1), min_(0) {}

  Encoding(const Poco::JSON::Object& _obj)
      : max_(JSON::get_value<Int>(_obj, "max_")),
        min_(JSON::get_value<Int>(_obj, "min_")) {}

  ~Encoding() = default;

  // -------------------------------

  /// Adds column to map or vector, returning the transformed column.
  void fit(const IntFeature& _val);

  /// Transform Encoding to JSON object.
  Poco::JSON::Object to_json_obj() const;

  /// Transforms the column to the mapped integers.
  IntFeature transform(const IntFeature& _val,
                       const std::shared_ptr<memmap::Pool>& _pool) const;

  // -------------------------------

  /// Size means the number of elements in this column.
  Int n_unique() const {
    if (min_ > max_) {
      throw std::runtime_error("Encoding has not been fitted!");
    }

    return max_ - min_ + 1;
  }

  // -------------------------------

 private:
  /// Maximum integer found.
  Int max_;

  /// Minimum integer found.
  Int min_;

  // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_ENCODING_HPP_
