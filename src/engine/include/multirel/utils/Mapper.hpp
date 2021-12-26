#ifndef MULTIREL_UTILS_MAPPER_HPP_
#define MULTIREL_UTILS_MAPPER_HPP_

// ------------------------------------------------------------------------

#include <map>
#include <memory>
#include <vector>

// ------------------------------------------------------------------------

#include "multirel/Int.hpp"

// ------------------------------------------------------------------------

namespace multirel {
namespace utils {

struct Mapper {
  /// The rows map reverses the effects of rows_ in
  /// containers::DataFrameView.
  static std::shared_ptr<const std::map<Int, Int>> create_rows_map(
      const std::shared_ptr<const std::vector<size_t>>& _rows);
};

}  // namespace utils
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_UTILS_MAPPER_HPP_
