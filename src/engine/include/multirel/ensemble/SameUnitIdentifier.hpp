#ifndef MULTIREL_ENSEMBLE_SAMEUNITIDENTIFIER_HPP_
#define MULTIREL_ENSEMBLE_SAMEUNITIDENTIFIER_HPP_

// ----------------------------------------------------------------------------

#include <map>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"
#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"
#include "multirel/decisiontrees/decisiontrees.hpp"
#include "multirel/descriptors/descriptors.hpp"
#include "multirel/enums/enums.hpp"

// ----------------------------------------------------------------------------
namespace multirel {
namespace ensemble {
// ----------------------------------------------------------------------------

class SameUnitIdentifier {
  // -------------------------------------------------------------------------

 public:
  /// Identifies the same units between the peripheral tables
  /// and the population table.
  static std::vector<descriptors::SameUnits> identify_same_units(
      const std::vector<containers::DataFrame>& _peripheral_tables,
      const containers::DataFrame& _population_table);

  // -------------------------------------------------------------------------

 private:
  /// Parses the units of _data and adds them to
  /// _unit_map
  template <class T>
  static void add_to_unit_map(
      const enums::DataUsed _data_used, const Int _ix_perip_used,
      const size_t _ix_column_used, const containers::Column<T>& _data,
      std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>*
          _unit_map);

  /// Finds the same units for categorical columns.
  static std::vector<descriptors::SameUnitsContainer>
  get_same_units_categorical(
      const std::vector<containers::DataFrame>& _peripheral_tables,
      const containers::DataFrame& _population_table);

  /// Finds the same units for discrete columns.
  static std::vector<descriptors::SameUnitsContainer> get_same_units_discrete(
      const std::vector<containers::DataFrame>& _peripheral_tables,
      const containers::DataFrame& _population_table);

  /// Finds the same units for numerical columns.
  static std::vector<descriptors::SameUnitsContainer> get_same_units_numerical(
      const std::vector<containers::DataFrame>& _peripheral_tables,
      const containers::DataFrame& _population_table);

  /// Once the unit maps have been fitted, this transforms it to a vector of
  /// descriptors::SameUnitsContainer objects.
  static void unit_map_to_same_unit_container(
      const std::map<std::string,
                     std::vector<descriptors::ColumnToBeAggregated>>& _unit_map,
      std::vector<descriptors::SameUnitsContainer>* _same_units);

  // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class T>
void SameUnitIdentifier::add_to_unit_map(
    const enums::DataUsed _data_used, const Int _ix_perip_used,
    const size_t _ix_column_used, const containers::Column<T>& _data,
    std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>*
        _unit_map) {
  const auto& unit = _data.unit_;

  if (unit != "") {
    auto it = _unit_map->find(unit);

    auto new_column = descriptors::ColumnToBeAggregated(
        _ix_column_used, _data_used, _ix_perip_used);

    if (it == _unit_map->end()) {
      _unit_map->insert(
          std::pair<std::string,
                    std::vector<descriptors::ColumnToBeAggregated>>(
              unit, {new_column}));
    } else {
      it->second.push_back(new_column);
    }
  }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel
#endif  // MULTIREL_ENSEMBLE_SAMEUNITIDENTIFIER_HPP_
