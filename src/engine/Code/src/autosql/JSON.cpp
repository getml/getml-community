#include "autosql/JSON.hpp"

namespace autosql
{
// ------------------------------------------------------------------------

size_t JSON::data_used_to_int( const enums::DataUsed& _data_used )
{
    switch ( _data_used )
        {
            case enums::DataUsed::not_applicable:
                return 0;

            case enums::DataUsed::same_unit_categorical:
                return 1;

            case enums::DataUsed::same_unit_discrete:
                return 2;

            case enums::DataUsed::same_unit_numerical:
                return 3;

            case enums::DataUsed::x_perip_categorical:
                return 4;

            case enums::DataUsed::x_perip_numerical:
                return 5;

            case enums::DataUsed::x_perip_discrete:
                return 6;

            case enums::DataUsed::x_popul_categorical:
                return 7;

            case enums::DataUsed::x_popul_numerical:
                return 8;

            case enums::DataUsed::x_popul_discrete:
                return 9;

            case enums::DataUsed::x_subfeature:
                return 10;

            case enums::DataUsed::time_stamps_diff:
                return 11;

            default:
                assert( !"Unknown enums::DataUsed!" );
                return 0;
        }
}

// ------------------------------------------------------------------------

/// Gets an array from a JSON object or throws.
Poco::JSON::Array::Ptr JSON::get_array(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    auto arr = _obj.getArray( _key );

    if ( !arr )
        {
            throw std::runtime_error( "Array named '" + _key + "' not found!" );
        }

    return arr;
}

// ------------------------------------------------------------------------

Poco::JSON::Object::Ptr JSON::get_object(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    auto ptr = _obj.getObject( _key );

    if ( !ptr )
        {
            throw std::runtime_error(
                "Object named '" + _key + "' not found!" );
        }

    return ptr;
}

// ------------------------------------------------------------------------

std::string JSON::stringify( const Poco::JSON::Object& _obj )
{
    std::stringstream json;

    _obj.stringify( json );

    return json.str();
}

// ----------------------------------------------------------------------------

enums::DataUsed JSON::int_to_data_used( const size_t& _val )
{
    switch ( _val )
        {
            case 0:
                return enums::DataUsed::not_applicable;

            case 1:
                return enums::DataUsed::same_unit_categorical;

            case 2:
                return enums::DataUsed::same_unit_discrete;

            case 3:
                return enums::DataUsed::same_unit_numerical;

            case 4:
                return enums::DataUsed::x_perip_categorical;

            case 5:
                return enums::DataUsed::x_perip_numerical;

            case 6:
                return enums::DataUsed::x_perip_discrete;

            case 7:
                return enums::DataUsed::x_popul_categorical;

            case 8:
                return enums::DataUsed::x_popul_numerical;

            case 9:
                return enums::DataUsed::x_popul_discrete;

            case 10:
                return enums::DataUsed::x_subfeature;

            case 11:
                return enums::DataUsed::time_stamps_diff;

            default:
                assert( !"Unknown enums::DataUsed!" );
                return enums::DataUsed::not_applicable;
        }
}

// ------------------------------------------------------------------------

AUTOSQL_SAME_UNITS_CONTAINER JSON::json_arr_to_same_units(
    const Poco::JSON::Array& _json_arr )
{
    AUTOSQL_SAME_UNITS_CONTAINER same_units;

    for ( size_t j = 0; j < _json_arr.size(); ++j )
        {
            // -------------------------

            auto& obj = *_json_arr.getObject( static_cast<unsigned int>( j ) );

            // -------------------------

            auto& child1 = *JSON::get_object( obj, "first" );

            enums::ColumnToBeAggregated column_to_be_aggregated1;

            column_to_be_aggregated1.ix_column_used =
                JSON::get_value<size_t>( child1, "ix_column_used" );

            column_to_be_aggregated1.data_used = JSON::int_to_data_used(
                JSON::get_value<int>( child1, "data_used" ) );

            column_to_be_aggregated1.ix_perip_used =
                JSON::get_value<size_t>( child1, "ix_perip_used" );

            // -------------------------

            auto& child2 = *JSON::get_object( obj, "second" );

            enums::ColumnToBeAggregated column_to_be_aggregated2;

            column_to_be_aggregated2.ix_column_used =
                JSON::get_value<size_t>( child2, "ix_column_used" );

            column_to_be_aggregated2.data_used = JSON::int_to_data_used(
                JSON::get_value<int>( child2, "data_used" ) );

            column_to_be_aggregated2.ix_perip_used =
                JSON::get_value<size_t>( child2, "ix_perip_used" );

            // -------------------------

            same_units.push_back( std::make_tuple(
                column_to_be_aggregated1, column_to_be_aggregated2 ) );

            // -------------------------
        }

    return same_units;
}

// ------------------------------------------------------------------------

Poco::JSON::Array JSON::same_units_to_json_arr(
    const AUTOSQL_SAME_UNITS_CONTAINER& _same_units )
{
    Poco::JSON::Array arr;

    for ( const std::tuple<
              enums::ColumnToBeAggregated,
              enums::ColumnToBeAggregated>& same_unit : _same_units )
        {
            Poco::JSON::Object same_unit_obj;

            // -------------------------

            Poco::JSON::Object child1;

            child1.set(
                "ix_column_used", std::get<0>( same_unit ).ix_column_used );

            child1.set(
                "data_used",
                JSON::data_used_to_int( std::get<0>( same_unit ).data_used ) );

            child1.set(
                "ix_perip_used", std::get<0>( same_unit ).ix_perip_used );

            same_unit_obj.set( "first", child1 );

            // -------------------------

            Poco::JSON::Object child2;

            child2.set(
                "ix_column_used", std::get<1>( same_unit ).ix_column_used );

            child2.set(
                "data_used",
                JSON::data_used_to_int( std::get<1>( same_unit ).data_used ) );

            child2.set(
                "ix_perip_used", std::get<1>( same_unit ).ix_perip_used );

            same_unit_obj.set( "second", child2 );

            // -------------------------

            arr.add( same_unit_obj );
        }

    return arr;
}

// ------------------------------------------------------------------------
}  // namespace autosql
