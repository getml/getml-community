#ifndef AUTOSQL_DESCRIPTORS_SAMEUNITS_HPP_
#define AUTOSQL_DESCRIPTORS_SAMEUNITS_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

/// Store information on which columns contain the
/// same units.
struct SameUnits
{
    // ------------------------------------------------------

    SameUnits() = default;

    ~SameUnits() = default;

    // ------------------------------------------------------

    /// List of all pairs of categorical columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_categorical;

    /// List of all pairs of discrete columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_discrete;

    /// List of all pairs of numerical columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_numerical;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DESCRIPTORS_SAMEUNITS_HPP_
