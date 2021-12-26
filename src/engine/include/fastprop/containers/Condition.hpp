#ifndef FASTPROP_CONTAINERS_CONDITION_HPP_
#define FASTPROP_CONTAINERS_CONDITION_HPP_

// -------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -------------------------------------------------------------------------

#include <cstddef>
#include <memory>
#include <string>

// -------------------------------------------------------------------------

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/enums/enums.hpp"
#include "helpers/helpers.hpp"

// -------------------------------------------------------------------------
namespace fastprop {
namespace containers {
// -------------------------------------------------------------------------

struct Condition {
  Condition(const enums::DataUsed _data_used, const size_t _input_col,
            const size_t _output_col, const size_t _peripheral);

  Condition(const Int _category_used, const enums::DataUsed _data_used,
            const size_t _input_col, const size_t _peripheral);

  Condition(const Float _bound_lower, const Float _bound_upper,
            const enums::DataUsed _data_used, const size_t _peripheral);

  explicit Condition(const Poco::JSON::Object &_obj);

  ~Condition();

  /// Expresses the abstract feature as a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const;

  /// Expresses the abstract feature as SQL code.
  std::string to_sql(const helpers::StringIterator &_categories,
                     const std::shared_ptr<const helpers::SQLDialectGenerator>
                         &_sql_dialect_generator,
                     const std::string &_feature_prefix,
                     const helpers::Schema &_input,
                     const helpers::Schema &_output) const;

  /// The lower bound (when data_used_ == lag).
  const Float bound_lower_;

  /// The upper bound (when data_used_ == lag).
  const Float bound_upper_;

  /// The category used (when data_used_ == categorical).
  const Int category_used_;

  /// The kind of data used
  const enums::DataUsed data_used_;

  /// The colnum of the column in the input table
  const size_t input_col_;

  /// The colnum of the column in the output table
  const size_t output_col_;

  /// The number of the peripheral table used
  const size_t peripheral_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_CONDITION_HPP_
