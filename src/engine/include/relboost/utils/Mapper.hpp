#ifndef RELBOOST_UTILS_MAPPER_HPP_
#define RELBOOST_UTILS_MAPPER_HPP_

// ----------------------------------------------------------------------------

#include <map>
#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "relboost/Int.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ------------------------------------------------------------------------

struct Mapper {
  /// The rows map reverses the effects of rows_ in
  /// containers::DataFrameView.
  static std::shared_ptr<const std::map<Int, Int>> create_rows_map(
      const std::shared_ptr<const std::vector<size_t>>& _rows);
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_MAPPER_HPP_
