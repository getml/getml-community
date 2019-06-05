#ifdef AUTOSQL_MULTINODE_MPI

#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ----------------------------------------------------------------------------

AUTOSQL_INT
MPIutils::calculate_join_key_used_popul(
    std::vector<std::map<AUTOSQL_INT, std::vector<AUTOSQL_INT>>>& _keys_maps )
{
    AUTOSQL_INT join_key_used_popul = 0;

    for ( size_t i = 0; i < _keys_maps.size(); ++i )
        {
            if ( _keys_maps[i].size() < _keys_maps[join_key_used_popul].size() )
                {
                    join_key_used_popul = i;
                }
        }

    return join_key_used_popul;
}

// ------------------------------------------------------------------------

containers::Matrix<AUTOSQL_INT> MPIutils::create_original_order(
    AUTOSQL_INT _nrows )
{
    auto original_order = containers::Matrix<AUTOSQL_INT>( _nrows, 1 );

    for ( AUTOSQL_INT i = 0; i < original_order.nrows(); ++i )
        {
            original_order[i] = i;
        }

    return original_order;
}

// ----------------------------------------------------------------------------

std::vector<containers::Matrix<AUTOSQL_INT>> MPIutils::rearrange_join_keys_root(
    std::vector<containers::Matrix<AUTOSQL_INT>>& _join_keys )
{
    std::vector<containers::Matrix<AUTOSQL_INT>> join_keys_output;

    for ( auto& join_key : _join_keys )
        {
            auto join_key_output = join_key.gather_root();

            join_key_output.name() = join_key.name();

            join_key_output.colnames() = join_key.colnames();

            join_keys_output.push_back( join_key_output );
        }

    return join_keys_output;
}

// ----------------------------------------------------------------------------

Rearranged MPIutils::rearrange_tables_root(
    std::vector<containers::DataFrame> _peripheral_tables_raw,
    containers::DataFrame _population_table_raw,
    decisiontrees::DecisionTreeEnsemble& _model )
{
    // ----------------------------------------------------------------
    // Create abstractions over the peripheral_tables and the population
    // table - for convenience

    debug_message( "rearrange: Create abstractions" );

    std::vector<containers::DataFrame> peripheral_tables;

    containers::DataFrame population_table;

    _model.prepare_tables(
        _peripheral_tables_raw,
        _population_table_raw,
        peripheral_tables,
        population_table );

    // ------------------------------------------------
    // Gather join keys at the root process

    debug_message( "rearrange: Gather join keys" );

    std::vector<containers::Matrix<AUTOSQL_INT>> join_keys_popul =
        population_table.join_keys();

    join_keys_popul = MPIutils::rearrange_join_keys_root( join_keys_popul );

    std::vector<containers::Matrix<AUTOSQL_INT>> join_keys_perip(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        join_keys_perip.begin(),
        []( containers::DataFrame& df ) { return df.join_key(); } );

    join_keys_perip = MPIutils::rearrange_join_keys_root( join_keys_perip );

    // ------------------------------------------------
    // The keys_maps assign a process id to each join key

    debug_message( "rearrange: Assign process id" );

    std::vector<std::map<AUTOSQL_INT, std::vector<AUTOSQL_INT>>> keys_maps =
        scatter_keys( join_keys_popul );

    // ------------------------------------------------
    // Rearrange x_perip_categorical

    debug_message( "Rearrange x_perip_categorical" );

    std::vector<containers::Matrix<AUTOSQL_INT>> x_perip_categorical(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_categorical.begin(),
        []( containers::DataFrame& df ) { return df.categorical(); } );

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            x_perip_categorical[i] = rearrange(
                x_perip_categorical[i], join_keys_perip[i], keys_maps[i] );
        }

    // ------------------------------------------------
    // Rearrange x_perip_numerical

    debug_message( "Rearrange x_perip_numerical" );

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> x_perip_numerical(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_numerical.begin(),
        []( containers::DataFrame& df ) { return df.numerical(); } );

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            x_perip_numerical[i] = rearrange(
                x_perip_numerical[i], join_keys_perip[i], keys_maps[i] );
        }

    // ------------------------------------------------
    // Rearrange x_perip_discrete

    debug_message( "Rearrange x_perip_discrete" );

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> x_perip_discrete(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_discrete.begin(),
        []( containers::DataFrame& df ) { return df.discrete(); } );

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            x_perip_discrete[i] = rearrange(
                x_perip_discrete[i], join_keys_perip[i], keys_maps[i] );
        }

    // ------------------------------------------------
    // Rearrange time_stamps_perip

    debug_message( "Rearrange time_stamps_perip" );

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> time_stamps_perip(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        time_stamps_perip.begin(),
        []( containers::DataFrame& df ) { return df.time_stamps(); } );

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            time_stamps_perip[i] = rearrange(
                time_stamps_perip[i], join_keys_perip[i], keys_maps[i] );
        }

    // ------------------------------------------------
    // We need to use one join key to rearrange the population table -
    // this determines which one

    debug_message( "Find join_key_used_popul" );

    AUTOSQL_INT join_key_used_popul =
        MPIutils::calculate_join_key_used_popul( keys_maps );

    auto join_key_used_popul_copy = join_keys_popul[join_key_used_popul];

    // ------------------------------------------------
    // Rearrange x_popul_categorical

    debug_message( "Rearrange x_popul_categorical" );

    auto x_popul_categorical = rearrange(
        population_table.categorical(),
        join_keys_popul[join_key_used_popul],
        keys_maps[join_key_used_popul] );

    // ------------------------------------------------
    // Rearrange x_popul_numerical

    debug_message( "Rearrange x_popul_numerical" );

    auto x_popul_numerical = rearrange(
        population_table.numerical(),
        join_keys_popul[join_key_used_popul],
        keys_maps[join_key_used_popul] );

    // ------------------------------------------------
    // Rearrange x_popul_discrete

    debug_message( "Rearrange x_popul_discrete" );

    auto x_popul_discrete = rearrange(
        population_table.discrete(),
        join_keys_popul[join_key_used_popul],
        keys_maps[join_key_used_popul] );

    // ------------------------------------------------
    // Rearrange time_stamps_popul

    debug_message( "Rearrange time_stamps_popul" );

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> time_stamps_popul(
        population_table.time_stamps_all().size() );

    std::transform(
        population_table.time_stamps_all().begin(),
        population_table.time_stamps_all().end(),
        time_stamps_popul.begin(),
        [&join_key_used_popul, &join_keys_popul, &keys_maps](
            containers::Matrix<AUTOSQL_FLOAT>& ts ) {

            return rearrange(
                ts,
                join_keys_popul[join_key_used_popul],
                keys_maps[join_key_used_popul] );

        } );

    // ------------------------------------------------
    // Get the targets

    debug_message( "Rearrange targets" );

    auto targets = rearrange(
        population_table.targets(),
        join_keys_popul[join_key_used_popul],
        keys_maps[join_key_used_popul] );

    // ------------------------------------------------
    // Create and scatter original_order -  we need it to
    // recreate the prediction

    debug_message( "Create and scatter original_order" );

    auto original_order = create_original_order( join_keys_popul[0].nrows() );

    original_order = scatter_by_key<containers::Matrix<AUTOSQL_INT>>(
        original_order,
        join_key_used_popul_copy,
        keys_maps[join_key_used_popul] );

    // ------------------------------------------------
    // Scatter join_keys_perip

    debug_message( "Scatter join_keys_perip" );

    for ( size_t i = 0; i < join_keys_perip.size(); ++i )
        {
            auto jk_name = join_keys_perip[i].name();

            auto jk_colnames = join_keys_perip[i].colnames();

            join_keys_perip[i] = scatter_by_key<containers::Matrix<AUTOSQL_INT>>(
                join_keys_perip[i], join_keys_perip[i], keys_maps[i] );

            join_keys_perip[i].name() = jk_name;

            join_keys_perip[i].colnames() = jk_colnames;
        }

    // ------------------------------------------------
    // Scatter join_keys_popul

    debug_message( "Scatter join_keys_popul" );

    for ( size_t i = 0; i < join_keys_popul.size(); ++i )
        {
            auto jk_name = join_keys_popul[i].name();

            auto jk_colnames = join_keys_popul[i].colnames();

            join_keys_popul[i] = scatter_by_key<containers::Matrix<AUTOSQL_INT>>(
                join_keys_popul[i],
                join_key_used_popul_copy,
                keys_maps[join_key_used_popul] );

            join_keys_popul[i].name() = jk_name;

            join_keys_popul[i].colnames() = jk_colnames;
        }

    // ------------------------------------------------
    // Condense information in struct and return

    debug_message( "Condense information in struct" );

    auto rearranged = Rearranged( peripheral_tables.size() );

    // -------

    rearranged.original_order = original_order;

    // -------

    {
        rearranged.population_table.categorical() = x_popul_categorical;

        rearranged.population_table.discrete() = x_popul_discrete;

        rearranged.population_table.join_keys() = join_keys_popul;

        rearranged.population_table.numerical() = x_popul_numerical;

        rearranged.population_table.targets() = targets;

        rearranged.population_table.time_stamps_all() = time_stamps_popul;
    }

    // -------

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            rearranged.peripheral_tables[i].join_keys().resize( 1 );

            rearranged.peripheral_tables[i].join_key( 0 ) = join_keys_perip[i];

            // -------

            rearranged.peripheral_tables[i].categorical() =
                x_perip_categorical[i];

            rearranged.peripheral_tables[i].discrete() = x_perip_discrete[i];

            rearranged.peripheral_tables[i].numerical() = x_perip_numerical[i];

            rearranged.peripheral_tables[i].targets() =
                containers::Matrix<AUTOSQL_FLOAT>(
                    x_perip_categorical[i].nrows(), 0 );

            rearranged.peripheral_tables[i].time_stamps_all().resize( 1 );

            rearranged.peripheral_tables[i].time_stamps( 0 ) =
                time_stamps_perip[i];

            // -------

            rearranged.peripheral_tables[i].set_join_key_used( 0 );

            rearranged.peripheral_tables[i].set_time_stamps_used( 0 );
        }

    // ---------------------------------------------------------------------------------
    // Create sample containers maps on peripheral tables

    debug_message( "Create index" );

    for ( auto& peripheral_table : rearranged.peripheral_tables )
        {
            peripheral_table.create_indices();
        }

    // ---------------------------------------------------------------------------------

    debug_message( "Done rearranging" );

    return rearranged;
}

// ----------------------------------------------------------------------------

Rearranged MPIutils::rearrange_tables(
    std::vector<containers::DataFrame> _peripheral_tables_raw,
    containers::DataFrame _population_table_raw,
    decisiontrees::DecisionTreeEnsemble& _model )
{
    // ----------------------------------------------------------------
    // Create abstractions over the peripheral_tables and the population
    // table - for convenience

    std::vector<containers::DataFrame> peripheral_tables;

    containers::DataFrame population_table;

    _model.prepare_tables(
        _peripheral_tables_raw,
        _population_table_raw,
        peripheral_tables,
        population_table );

    // ------------------------------------------------
    // Gather join keys at the root process

    std::vector<containers::Matrix<AUTOSQL_INT>> join_keys_popul =
        population_table.join_keys();

    rearrange_join_keys( join_keys_popul );

    std::vector<containers::Matrix<AUTOSQL_INT>> join_keys_perip(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        join_keys_perip.begin(),
        []( containers::DataFrame& df ) { return df.join_key( 0 ); } );

    rearrange_join_keys( join_keys_perip );

    // ------------------------------------------------
    // Rearrange x_perip_categorical

    std::vector<containers::Matrix<AUTOSQL_INT>> x_perip_categorical(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_categorical.begin(),
        []( containers::DataFrame& df ) { return df.categorical(); } );

    std::transform(
        x_perip_categorical.begin(),
        x_perip_categorical.end(),
        x_perip_categorical.begin(),
        []( containers::Matrix<AUTOSQL_INT>& mat ) { return rearrange( mat ); } );

    // ------------------------------------------------
    // Rearrange x_perip_categorical

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> x_perip_numerical(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_numerical.begin(),
        []( containers::DataFrame& df ) { return df.numerical(); } );

    std::transform(
        x_perip_numerical.begin(),
        x_perip_numerical.end(),
        x_perip_numerical.begin(),
        []( containers::Matrix<AUTOSQL_FLOAT>& mat ) {
            return rearrange( mat );
        } );

    // ------------------------------------------------
    // Rearrange x_perip_discrete

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> x_perip_discrete(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        x_perip_discrete.begin(),
        []( containers::DataFrame& df ) { return df.discrete(); } );

    std::transform(
        x_perip_discrete.begin(),
        x_perip_discrete.end(),
        x_perip_discrete.begin(),
        []( containers::Matrix<AUTOSQL_FLOAT>& mat ) {
            return rearrange( mat );
        } );

    // ------------------------------------------------
    // Rearrange time_stamps_perip

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> time_stamps_perip(
        peripheral_tables.size() );

    std::transform(
        peripheral_tables.begin(),
        peripheral_tables.end(),
        time_stamps_perip.begin(),
        []( containers::DataFrame& df ) { return df.time_stamps(); } );

    std::transform(
        time_stamps_perip.begin(),
        time_stamps_perip.end(),
        time_stamps_perip.begin(),
        []( containers::Matrix<AUTOSQL_FLOAT>& mat ) {
            return rearrange( mat );
        } );

    // ------------------------------------------------
    // Rearrange x_popul_categorical

    auto x_popul_categorical = rearrange( population_table.categorical() );

    // ------------------------------------------------
    // Rearrange x_popul_numerical

    auto x_popul_numerical = rearrange( population_table.numerical() );

    // ------------------------------------------------
    // Rearrange x_popul_discrete

    auto x_popul_discrete = rearrange( population_table.discrete() );

    // ------------------------------------------------
    // Rearrange time_stamps_popul

    std::vector<containers::Matrix<AUTOSQL_FLOAT>> time_stamps_popul(
        population_table.time_stamps_all().size() );

    std::transform(
        population_table.time_stamps_all().begin(),
        population_table.time_stamps_all().end(),
        time_stamps_popul.begin(),
        []( containers::Matrix<AUTOSQL_FLOAT>& ts ) { return rearrange( ts ); } );

    // ------------------------------------------------
    // Get the targets

    auto targets = rearrange( population_table.targets() );

    // ------------------------------------------------
    // Create and scatter original_order -  we need it to
    // recreate the prediction

    auto original_order = scatter_by_key<containers::Matrix<AUTOSQL_INT>>();

    // ------------------------------------------------
    // Scatter join_keys_perip

    std::for_each(
        join_keys_perip.begin(),
        join_keys_perip.end(),
        []( containers::Matrix<AUTOSQL_INT>& jk ) {
            jk = scatter_by_key<containers::Matrix<AUTOSQL_INT>>();
        } );

    // ------------------------------------------------
    // Scatter join_keys_popul

    std::for_each(
        join_keys_popul.begin(),
        join_keys_popul.end(),
        []( containers::Matrix<AUTOSQL_INT>& jk ) {
            jk = scatter_by_key<containers::Matrix<AUTOSQL_INT>>();
        } );

    // ------------------------------------------------
    // Condense information in struct and return

    auto rearranged = Rearranged( peripheral_tables.size() );

    // -------

    rearranged.original_order = original_order;

    // -------

    {
        rearranged.population_table.categorical() = x_popul_categorical;

        rearranged.population_table.discrete() = x_popul_discrete;

        rearranged.population_table.join_keys() = join_keys_popul;

        rearranged.population_table.numerical() = x_popul_numerical;

        rearranged.population_table.targets() = targets;

        rearranged.population_table.time_stamps_all() = time_stamps_popul;
    }

    // -------

    for ( AUTOSQL_SIZE i = 0; i < peripheral_tables.size(); ++i )
        {
            rearranged.peripheral_tables[i].join_keys().resize( 1 );

            rearranged.peripheral_tables[i].join_key( 0 ) = join_keys_perip[i];

            // -------

            rearranged.peripheral_tables[i].categorical() =
                x_perip_categorical[i];

            rearranged.peripheral_tables[i].discrete() = x_perip_discrete[i];

            rearranged.peripheral_tables[i].numerical() = x_perip_numerical[i];

            rearranged.peripheral_tables[i].targets() =
                containers::Matrix<AUTOSQL_FLOAT>(
                    x_perip_categorical[i].nrows(), 0 );

            rearranged.peripheral_tables[i].time_stamps_all().resize( 1 );

            rearranged.peripheral_tables[i].time_stamps( 0 ) =
                time_stamps_perip[i];

            // -------

            rearranged.peripheral_tables[i].set_join_key_used( 0 );

            rearranged.peripheral_tables[i].set_time_stamps_used( 0 );
        }

    // ---------------------------------------------------------------------------------
    // Create sample containers maps on peripheral tables

    for ( auto& peripheral_table : rearranged.peripheral_tables )
        {
            peripheral_table.create_indices();
        }

    // ---------------------------------------------------------------------------------

    return rearranged;
}

// ----------------------------------------------------------------------------

std::vector<std::map<AUTOSQL_INT, std::vector<AUTOSQL_INT>>>
MPIutils::scatter_keys( std::vector<containers::Matrix<AUTOSQL_INT>> _keys )
{
    // ---------------------------------------------------------------------

    boost::mpi::communicator comm_world;

    auto num_processes = comm_world.size();

    if ( _keys.size() == 0 )
        {
            throw std::invalid_argument( "You must provide at least one key!" );
        }

    for ( auto& key : _keys )
        {
            if ( key.nrows() != _keys[0].nrows() )
                {
                    throw std::invalid_argument(
                        "All keys must have the same number of rows!" );
                }
        }

    // ---------------------------------------------------------------------
    // Map a process id for each individual key

    std::vector<std::map<AUTOSQL_INT, AUTOSQL_INT>> keys_maps_temp(
        _keys.size() );

    for ( size_t ix_key = 0; ix_key < _keys.size(); ++ix_key )
        {
            auto& key = _keys[ix_key];

            auto& key_map = keys_maps_temp[ix_key];

            for ( AUTOSQL_INT i = 0; i < key.nrows(); ++i )
                {
                    auto it = key_map.find( key[i] );

                    if ( it == key_map.end() )
                        {
                            AUTOSQL_INT process_rank =
                                static_cast<AUTOSQL_INT>( key_map.size() ) %
                                num_processes;

                            key_map[key[i]] = process_rank;
                        }
                }
        }

    // ---------------------------------------------------------------------
    // Identify the map in keys_maps_temp with the least amount of entries
    // The idea is that most of the time, keys are hierarchical: A customer_id
    // can be associated
    // with several transaction_ids, but any transaction_id can only be
    // associated with one
    // customer_id

    size_t ix_min_keys_map = 0;

    for ( size_t i = 0; i < keys_maps_temp.size(); ++i )
        {
            if ( keys_maps_temp[i].size() <
                 keys_maps_temp[ix_min_keys_map].size() )
                {
                    ix_min_keys_map = i;
                }
        }

    // ---------------------------------------------------------------------
    // Now build the actual keys_maps, mapping the process id of the
    // min_keys_map to each sample

    std::vector<std::map<AUTOSQL_INT, std::vector<AUTOSQL_INT>>> keys_maps(
        _keys.size() );

    std::map<AUTOSQL_INT, AUTOSQL_INT>& min_key_map =
        keys_maps_temp[ix_min_keys_map];

    containers::Matrix<AUTOSQL_INT>& min_key = _keys[ix_min_keys_map];

    for ( AUTOSQL_INT i = 0; i < _keys[0].nrows(); ++i )
        {
            AUTOSQL_INT process_rank = min_key_map[min_key[i]];

            for ( size_t ix_key = 0; ix_key < _keys.size(); ++ix_key )
                {
                    std::vector<AUTOSQL_INT>& p_ranks =
                        keys_maps[ix_key][_keys[ix_key][i]];

                    // Insert process_ranks into p_ranks, if not already
                    // contained
                    auto it = std::find(
                        p_ranks.begin(), p_ranks.end(), process_rank );

                    if ( it == p_ranks.end() )
                        {
                            p_ranks.push_back( process_rank );
                        }
                }
        }

    // ---------------------------------------------------------------------

    return keys_maps;
}

// ----------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_MULTINODE_MPI
