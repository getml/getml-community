#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

std::vector<descriptors::SameUnitsContainer>
SameUnitIdentifier::get_same_units_categorical(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>
        unit_map;

    debug_log( "identify_same_units: Adding outputs (categorical)..." );

    for ( size_t j = 0; j < _population_table.num_categoricals(); ++j )
        {
            add_to_unit_map(
                enums::DataUsed::x_popul_categorical,
                -1,  // -1 signifies that this is the population table
                j,
                _population_table.categorical_col( j ),
                &unit_map );
        }

    for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
        {
            debug_log( "identify_same_units: Adding inputs (categorical)..." );

            for ( size_t j = 0; j < _peripheral_tables[i].num_categoricals();
                  ++j )
                {
                    add_to_unit_map(
                        enums::DataUsed::x_perip_categorical,
                        static_cast<Int>( i ),
                        static_cast<Int>( j ),
                        _peripheral_tables[i].categorical_col( j ),
                        &unit_map );
                }
        }

    debug_log( "identify_same_units: To containers (categorical)..." );

    std::vector<descriptors::SameUnitsContainer> same_units_categorical(
        _peripheral_tables.size() );

    unit_map_to_same_unit_container( unit_map, &same_units_categorical );

    return same_units_categorical;
}

// ----------------------------------------------------------------------------

std::vector<descriptors::SameUnitsContainer>
SameUnitIdentifier::get_same_units_discrete(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>
        unit_map;

    debug_log( "identify_same_units: Adding outputs (discrete)..." );

    for ( size_t j = 0; j < _population_table.num_discretes(); ++j )
        {
            add_to_unit_map(
                enums::DataUsed::x_popul_discrete,
                -1,  // -1 signifies that this is the population table
                j,
                _population_table.discrete_col( j ),
                &unit_map );
        }

    for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
        {
            debug_log( "identify_same_units: Adding inputs (discrete)..." );

            for ( size_t j = 0; j < _peripheral_tables[i].num_discretes(); ++j )
                {
                    add_to_unit_map(
                        enums::DataUsed::x_perip_discrete,
                        static_cast<Int>( i ),
                        static_cast<Int>( j ),
                        _peripheral_tables[i].discrete_col( j ),
                        &unit_map );
                }
        }

    debug_log( "identify_same_units: To containers (discrete)..." );

    std::vector<descriptors::SameUnitsContainer> same_units_discrete(
        _peripheral_tables.size() );

    unit_map_to_same_unit_container( unit_map, &same_units_discrete );

    return same_units_discrete;
}

// ----------------------------------------------------------------------------

std::vector<descriptors::SameUnitsContainer>
SameUnitIdentifier::get_same_units_numerical(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>
        unit_map;

    debug_log( "identify_same_units: Adding outputs (numerical)..." );

    for ( size_t j = 0; j < _population_table.num_numericals(); ++j )
        {
            add_to_unit_map(
                enums::DataUsed::x_popul_numerical,
                -1,  // -1 signifies that this is the population table
                j,
                _population_table.numerical_col( j ),
                &unit_map );
        }

    for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
        {
            debug_log( "identify_same_units: Adding inputs (numerical)..." );

            for ( size_t j = 0; j < _peripheral_tables[i].num_numericals();
                  ++j )
                {
                    add_to_unit_map(
                        enums::DataUsed::x_perip_numerical,
                        static_cast<Int>( i ),
                        static_cast<Int>( j ),
                        _peripheral_tables[i].numerical_col( j ),
                        &unit_map );
                }
        }

    debug_log( "identify_same_units: To containers (numerical)..." );

    std::vector<descriptors::SameUnitsContainer> same_units_numerical(
        _peripheral_tables.size() );

    unit_map_to_same_unit_container( unit_map, &same_units_numerical );

    return same_units_numerical;
}

// ----------------------------------------------------------------------------

std::vector<descriptors::SameUnits> SameUnitIdentifier::identify_same_units(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    // --------------------------------------------------------------

    const auto same_units_categorical =
        get_same_units_categorical( _peripheral_tables, _population_table );

    const auto same_units_discrete =
        get_same_units_discrete( _peripheral_tables, _population_table );

    const auto same_units_numerical =
        get_same_units_numerical( _peripheral_tables, _population_table );

    // --------------------------------------------------------------
    // Turn this into a SameUnit object

    std::vector<descriptors::SameUnits> same_units( _peripheral_tables.size() );

    for ( size_t i = 0; i < same_units.size(); ++i )
        {
            same_units[i].same_units_categorical_ =
                std::make_shared<descriptors::SameUnitsContainer>(
                    same_units_categorical[i] );

            same_units[i].same_units_discrete_ =
                std::make_shared<descriptors::SameUnitsContainer>(
                    same_units_discrete[i] );

            same_units[i].same_units_numerical_ =
                std::make_shared<descriptors::SameUnitsContainer>(
                    same_units_numerical[i] );
        }

    // --------------------------------------------------------------

    return same_units;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void SameUnitIdentifier::unit_map_to_same_unit_container(
    const std::map<std::string, std::vector<descriptors::ColumnToBeAggregated>>
        &_unit_map,
    std::vector<descriptors::SameUnitsContainer> *_same_units )
{
    for ( auto &unit_pair : _unit_map )
        {
            const auto &unit_vector = unit_pair.second;

            for ( size_t ix1 = 0; ix1 < unit_vector.size(); ++ix1 )
                {
                    for ( size_t ix2 = 0; ix2 < ix1; ++ix2 )
                        {
                            // Combinations between two different peripheral
                            // tables make to sense.
                            bool combination_makes_no_sense =
                                ( ( unit_vector[ix1].ix_perip_used !=
                                    unit_vector[ix2].ix_perip_used ) &&
                                  ( unit_vector[ix1].ix_perip_used != -1 ) &&
                                  ( unit_vector[ix2].ix_perip_used != -1 ) );

                            // Combinations where both columns are in the
                            // population table make no sense.
                            combination_makes_no_sense =
                                combination_makes_no_sense ||
                                ( ( unit_vector[ix1].ix_perip_used ==
                                    unit_vector[ix2].ix_perip_used ) &&
                                  ( unit_vector[ix1].ix_perip_used == -1 ) );

                            if ( combination_makes_no_sense )
                                {
                                    continue;
                                }

                            auto new_tuple = std::make_tuple(
                                unit_vector[ix1], unit_vector[ix2] );

                            // From the way the unit maps and the
                            // same_unit_containers are constructed, ix1 can
                            // never be the population table.
                            assert( unit_vector[ix1].ix_perip_used != -1 );

                            ( *_same_units )[unit_vector[ix1].ix_perip_used]
                                .push_back( new_tuple );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

}  // namespace ensemble
}  // namespace autosql
