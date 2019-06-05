#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

std::vector<descriptors::SameUnits> SameUnitIdentifier::identify_same_units(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    // --------------------------------------------------------------

    std::vector<AUTOSQL_SAME_UNITS_CONTAINER> same_units_categorical(
        _peripheral_tables.size() );

    std::vector<AUTOSQL_SAME_UNITS_CONTAINER> same_units_discrete(
        _peripheral_tables.size() );

    std::vector<AUTOSQL_SAME_UNITS_CONTAINER> same_units_numerical(
        _peripheral_tables.size() );

    // --------------------------------------------------------------
    // Let's begin with same_units_categorical
    {
        std::map<std::string, std::vector<ColumnToBeAggregated>>
            unit_map;

        debug_message( "identify_same_units: Adding outputs (categorical)..." );

        // Categorical population
        add_to_unit_map(
            DataUsed::x_popul_categorical,
            -1,  // -1 signifies that this is the population table
            _population_table.categorical(),
            unit_map );

        // Categorical peripheral
        for ( AUTOSQL_SIZE ix_perip_used = 0;
              ix_perip_used < _peripheral_tables.size();
              ++ix_perip_used )
            {
                debug_message(
                    "identify_same_units: Adding inputs (categorical)..." );

                add_to_unit_map(
                    DataUsed::x_perip_categorical,
					static_cast<AUTOSQL_INT>(ix_perip_used),
                    _peripheral_tables[ix_perip_used].categorical(),
                    unit_map );
            }

        debug_message( "identify_same_units: To containers (categorical)..." );

        unit_map_to_same_unit_container( unit_map, same_units_categorical );
    }

    // --------------------------------------------------------------
    // Same thing for numerical
    {
        std::map<std::string, std::vector<ColumnToBeAggregated>>
            unit_map;

        debug_message( "identify_same_units: Adding outputs (numerical)..." );

        // numerical population
        add_to_unit_map(
            DataUsed::x_popul_numerical,
            -1,  // -1 signifies that this is the population table
            _population_table.numerical(),
            unit_map );

        // numerical peripheral
        for ( AUTOSQL_SIZE ix_perip_used = 0;
              ix_perip_used < _peripheral_tables.size();
              ++ix_perip_used )
            {
                debug_message(
                    "identify_same_units: Adding inputs (numerical)..." );

                add_to_unit_map(
                    DataUsed::x_perip_numerical,
					static_cast<AUTOSQL_INT>(ix_perip_used),
                    _peripheral_tables[ix_perip_used].numerical(),
                    unit_map );
            }

        debug_message( "identify_same_units: To containers (numerical)..." );

        unit_map_to_same_unit_container( unit_map, same_units_numerical );
    }

    // --------------------------------------------------------------
    // Same thing for discrete
    {
        std::map<std::string, std::vector<ColumnToBeAggregated>>
            unit_map;

        debug_message( "identify_same_units: Adding outputs (discrete)..." );

        // discrete population
        add_to_unit_map(
            DataUsed::x_popul_discrete,
            -1,  // -1 signifies that this is the population table
            _population_table.discrete(),
            unit_map );

        // discrete peripheral
        for ( AUTOSQL_SIZE ix_perip_used = 0;
              ix_perip_used < _peripheral_tables.size();
              ++ix_perip_used )
            {
                debug_message(
                    "identify_same_units: Adding inputs (discrete)..." );

                add_to_unit_map(
                    DataUsed::x_perip_discrete,
					static_cast<AUTOSQL_INT>(ix_perip_used),
                    _peripheral_tables[ix_perip_used].discrete(),
                    unit_map );
            }

        debug_message( "identify_same_units: To containers (discrete)..." );

        unit_map_to_same_unit_container( unit_map, same_units_discrete );
    }

    // --------------------------------------------------------------
    // Turn this into a SameUnit object

    std::vector<descriptors::SameUnits> same_units( _peripheral_tables.size() );

    for ( size_t i = 0; i < same_units.size(); ++i )
        {
            same_units[i].same_units_categorical =
                std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
                    same_units_categorical[i] );

            same_units[i].same_units_discrete =
                std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
                    same_units_discrete[i] );

            same_units[i].same_units_numerical =
                std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
                    same_units_numerical[i] );
        }

    return same_units;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void SameUnitIdentifier::unit_map_to_same_unit_container(
    const std::map<std::string, std::vector<ColumnToBeAggregated>>
        &_unit_map,
    std::vector<AUTOSQL_SAME_UNITS_CONTAINER> &_same_units )
{
    for ( auto &unit_pair : _unit_map )
        {
            const auto &unit_vector = unit_pair.second;

            for ( AUTOSQL_SIZE ix1 = 0; ix1 < unit_vector.size(); ++ix1 )
                {
                    for ( AUTOSQL_SIZE ix2 = 0; ix2 < ix1; ++ix2 )
                        {
                            // Combinations between two different peripheral
                            // tables make to sense
                            bool combination_makes_no_sense =
                                ( ( unit_vector[ix1].ix_perip_used !=
                                    unit_vector[ix2].ix_perip_used ) &&
                                  ( unit_vector[ix1].ix_perip_used != -1 ) &&
                                  ( unit_vector[ix2].ix_perip_used != -1 ) );

                            // Combinations where both columns are in the
                            // population table make no sense
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

                            _same_units[unit_vector[ix1].ix_perip_used]
                                .push_back( new_tuple );
                        }
                }
        }
}

// ------------------------------------------------------------------------
}
}
