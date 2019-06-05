#include "JSON.hpp"

namespace autosql
{
// ----------------------------------------------------------------------------

AUTOSQL_INT JSON::data_used_to_int( const DataUsed& _data_used )
{
    switch ( _data_used )
        {
            case DataUsed::not_applicable:
                return 0;

            case DataUsed::same_unit_categorical:
                return 1;

            case DataUsed::same_unit_discrete:
                return 2;

            case DataUsed::same_unit_numerical:
                return 3;

            case DataUsed::x_perip_categorical:
                return 4;

            case DataUsed::x_perip_numerical:
                return 5;

            case DataUsed::x_perip_discrete:
                return 6;

            case DataUsed::x_popul_categorical:
                return 7;

            case DataUsed::x_popul_numerical:
                return 8;

            case DataUsed::x_popul_discrete:
                return 9;

            case DataUsed::x_subfeature:
                return 10;

            case DataUsed::time_stamps_diff:
                return 11;

            default:
                assert( !"Unknown DataUsed!" );
                return 0;
        }
}

// ----------------------------------------------------------------------------

DataUsed JSON::int_to_data_used( const AUTOSQL_INT& _dint )
{
    switch ( _dint )
        {
            case 0:
                return DataUsed::not_applicable;

            case 1:
                return DataUsed::same_unit_categorical;

            case 2:
                return DataUsed::same_unit_discrete;

            case 3:
                return DataUsed::same_unit_numerical;

            case 4:
                return DataUsed::x_perip_categorical;

            case 5:
                return DataUsed::x_perip_numerical;

            case 6:
                return DataUsed::x_perip_discrete;

            case 7:
                return DataUsed::x_popul_categorical;

            case 8:
                return DataUsed::x_popul_numerical;

            case 9:
                return DataUsed::x_popul_discrete;

            case 10:
                return DataUsed::x_subfeature;

            case 11:
                return DataUsed::time_stamps_diff;

            default:
                assert( !"Unknown DataUsed!" );
                return DataUsed::not_applicable;
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

            auto& child1 = *obj.AUTOSQL_GET_OBJECT( "first" );

            ColumnToBeAggregated column_to_be_aggregated1;

            column_to_be_aggregated1.ix_column_used =
                child1.AUTOSQL_GET( "ix_column_used" );

            column_to_be_aggregated1.data_used =
                JSON::int_to_data_used( child1.AUTOSQL_GET( "data_used" ) );

            column_to_be_aggregated1.ix_perip_used =
                child1.AUTOSQL_GET( "ix_perip_used" );

            // -------------------------

            auto& child2 = *obj.AUTOSQL_GET_OBJECT( "second" );

            ColumnToBeAggregated column_to_be_aggregated2;

            column_to_be_aggregated2.ix_column_used =
                child2.AUTOSQL_GET( "ix_column_used" );

            column_to_be_aggregated2.data_used =
                JSON::int_to_data_used( child2.AUTOSQL_GET( "data_used" ) );

            column_to_be_aggregated2.ix_perip_used =
                child2.AUTOSQL_GET( "ix_perip_used" );

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
              ColumnToBeAggregated,
              ColumnToBeAggregated>& same_unit : _same_units )
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
}
