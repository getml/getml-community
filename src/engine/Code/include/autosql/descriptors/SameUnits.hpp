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

    SameUnits( const Poco::JSON::Object& _obj )
    {
        *this = from_json_obj( _obj );
    };

    ~SameUnits() = default;

    // ------------------------------------------------------

    /// Reconstructs a SameUnits object from a JSON object.
    SameUnits from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Helper function.
    AUTOSQL_SAME_UNITS_CONTAINER json_arr_to_same_units(
        const Poco::JSON::Array& _json_arr ) const;

    /// Helper function.
    Poco::JSON::Array same_units_to_json_arr(
        const AUTOSQL_SAME_UNITS_CONTAINER& _same_units ) const;

    /// Transforms the SameUnits into a JSON object.
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------

    /// List of all pairs of categorical columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_categorical_;

    /// List of all pairs of discrete columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_discrete_;

    /// List of all pairs of numerical columns of the same unit
    std::shared_ptr<const AUTOSQL_SAME_UNITS_CONTAINER> same_units_numerical_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
#endif  // AUTOSQL_DESCRIPTORS_SAMEUNITS_HPP_
