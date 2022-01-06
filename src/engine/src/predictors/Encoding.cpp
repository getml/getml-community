#include "predictors/Encoding.hpp"

#include <memory>

namespace predictors {

void Encoding::fit(const IntFeature& _col) {
  min_ = *std::min_element(_col.begin(), _col.end());
  max_ = *std::max_element(_col.begin(), _col.end());
}

// -----------------------------------------------------------------------------

Poco::JSON::Object Encoding::to_json_obj() const {
  Poco::JSON::Object obj;
  obj.set("max_", max_);
  obj.set("min_", min_);
  return obj;
}

// -----------------------------------------------------------------------------

IntFeature Encoding::transform(
    const IntFeature& _col, const std::shared_ptr<memmap::Pool>& _pool) const {
  auto output =
      _pool ? IntFeature(
                  std::make_shared<memmap::Vector<Int>>(_pool, _col.size()))
            : IntFeature(std::make_shared<std::vector<Int>>(_col.size()));

  for (size_t i = 0; i < _col.size(); ++i) {
    if (_col[i] < min_ || _col[i] > max_) {
      output[i] = -1;
    } else {
      output[i] = _col[i] - min_;
    }
  }

  return output;
}

// -----------------------------------------------------------------------------
}  // namespace predictors
