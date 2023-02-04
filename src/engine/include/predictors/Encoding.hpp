// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_ENCODING_HPP_
#define PREDICTORS_ENCODING_HPP_

#include <Poco/JSON/Object.h>

#include <memory>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "json/json.hpp"
#include "memmap/memmap.hpp"
#include "predictors/Float.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/Int.hpp"
#include "predictors/IntFeature.hpp"

namespace predictors {

class Encoding {
 public:
  using f_max = fct::Field<"max_", Int>;

  using f_min = fct::Field<"min_", Int>;

  using NamedTupleType = fct::NamedTuple<f_max, f_min>;

 public:
  Encoding() : max_(1), min_(0) {}

  explicit Encoding(const NamedTupleType& _nt)
      : max_(_nt.get<f_max>()), min_(_nt.get<f_min>()) {}

  /// TODO: Remove this temporary fix.
  explicit Encoding(const Poco::JSON::Object& _obj)
      : Encoding(json::from_json<Encoding>(_obj)) {}

  ~Encoding() = default;

  /// Adds column to map or vector, returning the transformed column.
  void fit(const IntFeature& _val);

  /// Transform Encoding to JSON object.
  Poco::JSON::Object to_json_obj() const;

  /// Transforms the column to the mapped integers.
  IntFeature transform(const IntFeature& _val,
                       const std::shared_ptr<memmap::Pool>& _pool) const;

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const { return f_max(max_) * f_min(min_); }

  /// Size means the number of elements in this column.
  Int n_unique() const {
    if (min_ > max_) {
      throw std::runtime_error("Encoding has not been fitted!");
    }
    return max_ - min_ + 1;
  }

 private:
  /// Maximum integer found.
  Int max_;

  /// Minimum integer found.
  Int min_;
};

}  // namespace predictors

#endif  // PREDICTORS_ENCODING_HPP_
