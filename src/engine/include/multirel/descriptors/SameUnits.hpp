#ifndef MULTIREL_DESCRIPTORS_SAMEUNITS_HPP_
#define MULTIREL_DESCRIPTORS_SAMEUNITS_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "fastprop/Hyperparameters.hpp"

// ----------------------------------------------------------------------------

#include "multirel/Int.hpp"
#include "multirel/JSON.hpp"
#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "multirel/descriptors/SameUnitsContainer.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace descriptors {
// ----------------------------------------------------------------------------

/// Store information on which columns contain the
/// same units.
struct SameUnits {
  SameUnits() = default;

  SameUnits(const Poco::JSON::Object& _obj) { *this = from_json_obj(_obj); };

  ~SameUnits() = default;

  // ------------------------------------------------------

  /// Reconstructs a SameUnits object from a JSON object.
  SameUnits from_json_obj(const Poco::JSON::Object& _obj) const;

  /// Figures out whether a same unit object is a time stamp
  bool is_ts(const containers::DataFrameView& _population,
             const containers::DataFrame& _peripheral,
             const descriptors::SameUnitsContainer& _same_units,
             const size_t _col) const;

  /// Helper function.
  descriptors::SameUnitsContainer json_arr_to_same_units(
      const Poco::JSON::Array& _json_arr) const;

  /// Helper function.
  Poco::JSON::Array same_units_to_json_arr(
      const descriptors::SameUnitsContainer& _same_units) const;

  /// Transforms the SameUnits into a JSON object.
  Poco::JSON::Object to_json_obj() const;

  // ------------------------------------------------------

  /// List of all pairs of categorical columns of the same unit
  std::shared_ptr<const descriptors::SameUnitsContainer>
      same_units_categorical_;

  /// List of all pairs of discrete columns of the same unit
  std::shared_ptr<const descriptors::SameUnitsContainer> same_units_discrete_;

  /// List of all pairs of numerical columns of the same unit
  std::shared_ptr<const descriptors::SameUnitsContainer> same_units_numerical_;

  // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel
#endif  // MULTIREL_DESCRIPTORS_SAMEUNITS_HPP_
