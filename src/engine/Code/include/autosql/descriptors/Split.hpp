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
        const enums::DataUsed _data_used )
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
        const enums::DataUsed _data_used )
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
        const enums::DataUsed _data_used )
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
        : apply_from_above( JSON::get_value<bool>( _json_obj, "app_" ) ),
          categories_used( std::make_shared<std::vector<AUTOSQL_INT>>(
              JSON::array_to_vector<AUTOSQL_INT>(
                  JSON::get_array( _json_obj, "categories_used_" ) ) ) ),
          categories_used_begin( categories_used->cbegin() ),
          categories_used_end( categories_used->cend() ),
          column_used(
              JSON::get_value<AUTOSQL_INT>( _json_obj, "column_used_" ) ),
          critical_value(
              JSON::get_value<AUTOSQL_FLOAT>( _json_obj, "critical_value_" ) ),
          data_used( JSON::int_to_data_used(
              JSON::get_value<AUTOSQL_INT>( _json_obj, "data_used_" ) ) )
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
    const enums::DataUsed data_used;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_SPLIT_HPP_
