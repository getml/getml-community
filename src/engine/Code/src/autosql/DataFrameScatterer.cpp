#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ----------------------------------------------------------------------------

const std::vector<AUTOSQL_INT> DataFrameScatterer::build_thread_nums(
    const std::map<AUTOSQL_INT, AUTOSQL_INT>& _min_keys_map,
    const containers::Matrix<AUTOSQL_INT>& _min_join_key )
{
    std::vector<AUTOSQL_INT> thread_nums( _min_join_key.nrows() );

    for ( AUTOSQL_INT i = 0; i < _min_join_key.nrows(); ++i )
        {
            auto it = _min_keys_map.find( _min_join_key[i] );

            assert( it != _min_keys_map.end() );

            thread_nums[i] = it->second;
        }

    return thread_nums;
}

// ----------------------------------------------------------------------------

const std::vector<AUTOSQL_INT> DataFrameScatterer::build_thread_nums(
    const std::vector<containers::Matrix<AUTOSQL_INT> >& _keys,
    const AUTOSQL_INT _num_threads )
{
    check_plausibility( _keys, _num_threads );

    AUTOSQL_INT ix_min_keys;

    std::map<AUTOSQL_INT, AUTOSQL_INT> min_keys_map;

    scatter_keys( _keys, _num_threads, ix_min_keys, min_keys_map );

    return build_thread_nums( min_keys_map, _keys[ix_min_keys] );
}

// ----------------------------------------------------------------------------

void DataFrameScatterer::check_plausibility(
    const std::vector<containers::Matrix<AUTOSQL_INT> >& _keys,
    const AUTOSQL_INT _num_threads )
{
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

    if ( _num_threads <= 0 )
        {
            throw std::invalid_argument(
                "Number of threads must be positive!" );
        }
}

// ----------------------------------------------------------------------------

containers::DataFrameView DataFrameScatterer::scatter_data_frame(
    const containers::DataFrame& _df,
    const std::vector<AUTOSQL_INT>& _thread_nums,
    const AUTOSQL_INT _thread_num )
{
    assert( static_cast<size_t>( _df.nrows() ) == _thread_nums.size() );

    std::vector<AUTOSQL_INT> indices;

    for ( AUTOSQL_INT i = 0; i < _df.nrows(); ++i )
        {
            if ( _thread_nums[i] == _thread_num )
                {
                    indices.push_back( i );
                }
        }

    return containers::DataFrameView( _df, indices );
}

// ----------------------------------------------------------------------------

void DataFrameScatterer::scatter_keys(
    const std::vector<containers::Matrix<AUTOSQL_INT> >& _keys,
    const AUTOSQL_INT _num_threads,
    AUTOSQL_INT& _ix_min_keys_map,
    std::map<AUTOSQL_INT, AUTOSQL_INT>& _min_keys_map )
{
    // ---------------------------------------------------------------------------
    // Map a process id for each individual key

    std::vector<std::map<AUTOSQL_INT, AUTOSQL_INT> > keys_maps_temp(
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
                            AUTOSQL_INT rank =
                                static_cast<AUTOSQL_INT>( key_map.size() ) %
                                _num_threads;

                            key_map[key[i]] = rank;
                        }
                }
        }

    // ---------------------------------------------------------------------------
    // Identify the map in keys_maps_temp with the least amount of entries
    // The idea is that most of the time, keys are hierarchical: A customer_id
    // can be associated
    // with several transaction_ids, but any transaction_id can only be
    // associated with one customer_id

    _ix_min_keys_map = 0;

    for ( size_t i = 0; i < keys_maps_temp.size(); ++i )
        {
            if ( keys_maps_temp[i].size() <
                 keys_maps_temp[_ix_min_keys_map].size() )
                {
                    _ix_min_keys_map = static_cast<AUTOSQL_INT>( i );
                }
        }

    // ---------------------------------------------------------------------------

    _min_keys_map = keys_maps_temp[_ix_min_keys_map];

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}
}