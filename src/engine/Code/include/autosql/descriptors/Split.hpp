#ifndef AUTOSQL_DESCRIPTORS_SPLIT_HPP_
#define AUTOSQL_DESCRIPTORS_SPLIT_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

struct Split
{
    /// Constructor for splits on numerical values.
    Split(
        const bool _apply_from_above,
        const AUTOSQL_FLOAT _critical_value,
        const AUTOSQL_INT _column_used,
        const DataUsed _data_used )
        : apply_from_above( _apply_from_above ),
          categories_used( std::make_shared<std::vector<AUTOSQL_INT>>( 0 ) ),
          categories_used_begin( categories_used->cbegin() ),
          categories_used_end( categories_used->cend() ),
          column_used( _column_used ),
          critical_value( _critical_value ),
          data_used( _data_used )
    {
    }

    /// Constructor for splits on categorical values.
    Split(
        const bool _apply_from_above,
        const std::shared_ptr<const std::vector<AUTOSQL_INT>> _categories_used,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_used_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_used_end,
        const AUTOSQL_INT _column_used,
        const DataUsed _data_used )
        : apply_from_above( _apply_from_above ),
          categories_used( _categories_used ),
          categories_used_begin( _categories_used_begin ),
          categories_used_end( _categories_used_end ),
          column_used( _column_used ),
          critical_value( 0.0 ),
          data_used( _data_used )
    {
    }

    /// Constructor for deep copies.
    Split(
        const bool _apply_from_above,
        const AUTOSQL_FLOAT _critical_value,
        const std::shared_ptr<const std::vector<AUTOSQL_INT>> _categories_used,
        const AUTOSQL_INT _column_used,
        const DataUsed _data_used )
        : apply_from_above( _apply_from_above ),
          categories_used( _categories_used ),
          categories_used_begin( _categories_used->begin() ),
          categories_used_end( _categories_used->end() ),
          column_used( _column_used ),
          critical_value( _critical_value ),
          data_used( _data_used )
    {
    }

    /// Constructor for from Poco::JSON::Object.
    Split( const Poco::JSON::Object &_json_obj )
        : apply_from_above( _json_obj.AUTOSQL_GET( "app_" ) ),
          categories_used( std::make_shared<std::vector<AUTOSQL_INT>>(
              JSON::array_to_vector<AUTOSQL_INT>(
                  _json_obj.AUTOSQL_GET_ARRAY( "categories_used_" ) ) ) ),
          categories_used_begin( categories_used->cbegin() ),
          categories_used_end( categories_used->cend() ),
          column_used( _json_obj.AUTOSQL_GET( "column_used_" ) ),
          critical_value( _json_obj.AUTOSQL_GET( "critical_value_" ) ),
          data_used(
              JSON::int_to_data_used( _json_obj.AUTOSQL_GET( "data_used_" ) ) )
    {
    }

    ~Split() = default;

    // ------------------------------------------------------------------------

    /// Returns a deep copy of the Split.
    Split deep_copy() const
    {
        auto sorted = std::make_shared<std::vector<AUTOSQL_INT>>(
            categories_used_begin, categories_used_end );

        std::sort( sorted->begin(), sorted->end() );

        return Split(
            apply_from_above, critical_value, sorted, column_used, data_used );
    }

    // ------------------------------------------------------------------------

    // If true, the status change from activation to
    // deactivation or from deactivation to activation
    // is applied to all values greater than the critical
    // value and vice versa.
    const bool apply_from_above;

    // Categories used for the node - for categorical values.
    const std::shared_ptr<const std::vector<AUTOSQL_INT>> categories_used;

    // Iterator pointing to the beginning of the categories used.
    const std::vector<AUTOSQL_INT>::const_iterator categories_used_begin;

    // Iterator pointing to the end of the categories used.
    const std::vector<AUTOSQL_INT>::const_iterator categories_used_end;

    // Number of column used
    const AUTOSQL_INT column_used;

    // Critical value for the node - for numeric values
    const AUTOSQL_FLOAT critical_value;

    // Signifies whether we use x_popul_numerical, x_popul_discrete,
    // x_perip_numerical, x_perip_discrete or time_stamps_diff
    // The class DataUsed is defined in ColumnToBeAggregated.hpp
    const DataUsed data_used;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_SPLIT_HPP_
