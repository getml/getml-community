#include "relmt/ensemble/DecisionTreeEnsembleImpl.hpp"

namespace relmt {
namespace ensemble {
// ----------------------------------------------------------------------------

void DecisionTreeEnsembleImpl::check_placeholder(
    const containers::Placeholder& _placeholder) const {
  assert_true(peripheral_);

  for (auto& joined : _placeholder.joined_tables_) {
    const bool not_found = (std::find(peripheral_->begin(), peripheral_->end(),
                                      joined.name_) == peripheral_->end());

    if (not_found) {
      throw std::runtime_error("Table  named '" + joined.name_ +
                               "' not among peripheral tables!");
    }

    check_placeholder(joined);
  }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relmt
