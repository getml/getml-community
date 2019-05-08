#include "relboost/utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

const std::vector<size_t> DataFrameScatterer::build_thread_nums(
    const std::map<RELBOOST_INT, size_t>& _min_keys_map,
    const containers::Column<RELBOOST_INT>& _min_join_key )
{
    std::vector<size_t> thread_nums( _min_join_key.nrows_ );

    for ( size_t i = 0; i < _min_join_key.nrows_; ++i )
        {
            auto it = _min_keys_map.find( _min_join_key[i] );

            assert( it != _min_keys_map.end() );

            thread_nums[i] = it->second;
        }

    return thread_nums;
}

// ----------------------------------------------------------------------------

const std::vector<size_t> DataFrameScatterer::build_thread_nums(
    const std::vector<containers::Column<RELBOOST_INT>>& _keys,
    const size_t _num_threads )
{
    check_plausibility( _keys, _num_threads );

    size_t ix_min_keys = 0;

    std::map<RELBOOST_INT, size_t> min_keys_map;

    scatter_keys( _keys, _num_threads, &ix_min_keys, &min_keys_map );

    return build_thread_nums( min_keys_map, _keys[ix_min_keys] );
}

// ----------------------------------------------------------------------------

void DataFrameScatterer::check_plausibility(
    const std::vector<containers::Column<RELBOOST_INT>>& _keys,
    const size_t _num_threads )
{
    if ( _keys.size() == 0 )
        {
            throw std::invalid_argument(
                "You must provide at least one join key!" );
        }

    for ( auto& key : _keys )
        {
            if ( key.nrows_ != _keys[0].nrows_ )
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
    const std::vector<size_t>& _thread_nums,
    const size_t _thread_num )
{
    assert( _df.nrows() == _thread_nums.size() );

    auto indices = std::make_shared<std::vector<size_t>>( 0 );

    for ( size_t i = 0; i < _thread_nums.size(); ++i )
        {
            if ( _thread_nums[i] == _thread_num )
                {
                    indices->push_back( i );
                }
        }

    return containers::DataFrameView( _df, indices );
}

// ----------------------------------------------------------------------------

void DataFrameScatterer::scatter_keys(
    const std::vector<containers::Column<RELBOOST_INT>>& _keys,
    const size_t _num_threads,
    size_t* _ix_min_keys_map,
    std::map<RELBOOST_INT, size_t>* _min_keys_map )
{
    // ---------------------------------------------------------------------------
    // Map a thread id for each individual key.

    std::vector<std::map<RELBOOST_INT, size_t>> keys_maps_temp( _keys.size() );

    for ( size_t ix_key = 0; ix_key < _keys.size(); ++ix_key )
        {
            auto& key = _keys[ix_key];

            auto& key_map = keys_maps_temp[ix_key];

            for ( auto k : key )
                {
                    auto it = key_map.find( k );

                    if ( it == key_map.end() )
                        {
                            auto rank = key_map.size() % _num_threads;

                            key_map[k] = rank;
                        }
                }
        }

    // ---------------------------------------------------------------------------
    // Identify the map in keys_maps_temp with the least amount of entries
    // The idea is that more often than not, keys are hierarchical: A
    // customer_id can be associatedwith several transaction_ids, but any
    // transaction_id can only be associated with one customer_id

    *_ix_min_keys_map = 0;

    for ( size_t i = 0; i < keys_maps_temp.size(); ++i )
        {
            if ( keys_maps_temp[i].size() <
                 keys_maps_temp[*_ix_min_keys_map].size() )
                {
                    *_ix_min_keys_map = i;
                }
        }

    // ---------------------------------------------------------------------------

    *_min_keys_map = keys_maps_temp[*_ix_min_keys_map];

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost