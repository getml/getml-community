#include "multirel/descriptors/descriptors.hpp"

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------

SameUnits SameUnits::from_json_obj( const Poco::JSON::Object& _obj ) const
{
    SameUnits same_units;

    same_units.same_units_categorical_ =
        std::make_shared<descriptors::SameUnitsContainer>(
            json_arr_to_same_units(
                *JSON::get_array( _obj, "same_units_categorical_" ) ) );

    same_units.same_units_discrete_ =
        std::make_shared<descriptors::SameUnitsContainer>(
            json_arr_to_same_units(
                *JSON::get_array( _obj, "same_units_discrete_" ) ) );

    same_units.same_units_numerical_ =
        std::make_shared<descriptors::SameUnitsContainer>(
            json_arr_to_same_units(
                *JSON::get_array( _obj, "same_units_numerical_" ) ) );

    return same_units;
}

// ----------------------------------------------------------------------------

bool SameUnits::is_ts(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const descriptors::SameUnitsContainer& _same_units,
    const size_t _col ) const
{
    assert_true( _col < _same_units.size() );

    const auto ix = std::get<0>( _same_units.at( _col ) ).ix_column_used;

    std::string name;

    std::string unit;

    switch ( std::get<0>( _same_units.at( _col ) ).data_used )
        {
            case enums::DataUsed::x_perip_discrete:
                name = _peripheral.discrete_name( ix );
                unit = _peripheral.discrete_unit( ix );
                break;

            case enums::DataUsed::x_perip_numerical:
                name = _peripheral.numerical_name( ix );
                unit = _peripheral.numerical_unit( ix );
                break;

            case enums::DataUsed::x_popul_discrete:
                name = _population.discrete_name( ix );
                unit = _population.discrete_unit( ix );
                break;

            case enums::DataUsed::x_popul_numerical:
                name = _population.numerical_name( ix );
                unit = _population.numerical_unit( ix );
                break;

            default:
                assert_true( !"is_ts: enums::DataUsed not known!" );
                break;
        }

    const auto not_contains_rowid =
        name.find( "$GETML_ROWID" ) == std::string::npos;

    const auto contains_time_stamp =
        unit.find( "time stamp" ) != std::string::npos;

    return contains_time_stamp && not_contains_rowid;
}

// ----------------------------------------------------------------------------

descriptors::SameUnitsContainer SameUnits::json_arr_to_same_units(
    const Poco::JSON::Array& _json_arr ) const
{
    descriptors::SameUnitsContainer same_units;

    for ( size_t j = 0; j < _json_arr.size(); ++j )
        {
            // -------------------------

            auto ptr = _json_arr.getObject( static_cast<unsigned int>( j ) );

            assert_true( ptr );

            auto& obj = *ptr;

            // -------------------------

            auto child1 = *JSON::get_object( obj, "first" );

            descriptors::ColumnToBeAggregated column_to_be_aggregated1;

            column_to_be_aggregated1.ix_column_used =
                JSON::get_value<size_t>( child1, "ix_column_used" );

            column_to_be_aggregated1.data_used = JSON::int_to_data_used(
                JSON::get_value<int>( child1, "data_used" ) );

            column_to_be_aggregated1.ix_perip_used =
                JSON::get_value<Int>( child1, "ix_perip_used" );

            // -------------------------

            auto child2 = *JSON::get_object( obj, "second" );

            descriptors::ColumnToBeAggregated column_to_be_aggregated2;

            column_to_be_aggregated2.ix_column_used =
                JSON::get_value<size_t>( child2, "ix_column_used" );

            column_to_be_aggregated2.data_used = JSON::int_to_data_used(
                JSON::get_value<int>( child2, "data_used" ) );

            column_to_be_aggregated2.ix_perip_used =
                JSON::get_value<Int>( child2, "ix_perip_used" );

            // -------------------------

            same_units.push_back( std::make_tuple(
                column_to_be_aggregated1, column_to_be_aggregated2 ) );

            // -------------------------
        }

    return same_units;
}

// ----------------------------------------------------------------------------

Poco::JSON::Array SameUnits::same_units_to_json_arr(
    const descriptors::SameUnitsContainer& _same_units ) const
{
    Poco::JSON::Array arr;

    for ( const std::tuple<
              descriptors::ColumnToBeAggregated,
              descriptors::ColumnToBeAggregated>& same_unit : _same_units )
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

// ----------------------------------------------------------------------------

Poco::JSON::Object SameUnits::to_json_obj() const
{
    Poco::JSON::Object obj;

    obj.set(
        "same_units_categorical_",
        same_units_to_json_arr( *same_units_categorical_ ) );

    obj.set(
        "same_units_discrete_",
        same_units_to_json_arr( *same_units_discrete_ ) );

    obj.set(
        "same_units_numerical_",
        same_units_to_json_arr( *same_units_numerical_ ) );

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel
