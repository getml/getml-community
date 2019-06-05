#include "aggregations/aggregations.hpp"

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

const SQLNET_FLOAT AggregationIndex::get_count( const SQLNET_INT _ix_agg ) const
{
    assert( _ix_agg >= 0 );
    assert( _ix_agg < output_table_.nrows() );

    auto it =
        input_table_.df().index()->find( output_table_.join_key( _ix_agg ) );

    assert( it != input_table_.df().index()->end() );

    SQLNET_FLOAT count = 0.0;

    const SQLNET_FLOAT time_stamp_output = output_table_.time_stamp( _ix_agg );

    const auto ptr = input_table_.df().upper_time_stamps();

    for ( auto ix_input : it->second )
        {
            SQLNET_FLOAT upper_time_stamp = NAN;

            if ( ptr != nullptr )
                {
                    upper_time_stamp = ( *ptr )[ix_input];
                }

            const bool use_this_sample =
                ( !use_timestamps_ ||
                  time_stamp_output_in_range(
                      input_table_.df().time_stamps()[ix_input],
                      upper_time_stamp,
                      time_stamp_output ) );

            if ( use_this_sample )
                {
                    ++count;
                }
        }

    return count;
}

// ----------------------------------------------------------------------------

const std::vector<SQLNET_INT> AggregationIndex::transform(
    const SQLNET_INT _ix_input ) const
{
    assert( _ix_input >= 0 );
    assert( _ix_input < input_table_.nrows() );

    auto it =
        output_table_.df().index()->find( input_table_.join_key( _ix_input ) );

    if ( it == output_table_.df().index()->end() )
        {
            return std::vector<SQLNET_INT>();
        }

    const SQLNET_FLOAT time_stamp_input = input_table_.time_stamp( _ix_input );

    const SQLNET_FLOAT upper_time_stamp =
        input_table_.upper_time_stamp( _ix_input );

    std::vector<SQLNET_INT> indices;

    for ( auto ix_agg : it->second )
        {
            assert( ix_agg >= 0 );
            assert( ix_agg < output_table_.df().nrows() );

            const bool use_this_sample =
                ( !use_timestamps_ ||
                  time_stamp_output_in_range(
                      time_stamp_input,
                      upper_time_stamp,
                      output_table_.df().time_stamps()[ix_agg] ) );

            if ( use_this_sample )
                {
                    auto ix_agg_tr = transform_ix_agg( ix_agg );

                    if ( ix_agg_tr != -1 )
                        {
                            assert(
                                ix_agg ==
                                ( *output_table_.get_indices() )[ix_agg_tr] );

                            indices.push_back( ix_agg_tr );
                        }
                }
        }

    return indices;
}

// ----------------------------------------------------------------------------

SQLNET_INT AggregationIndex::transform_ix_agg( const SQLNET_INT _ix_agg ) const
{
    assert( output_map_->size() > 0 );

    const auto it = output_map_->find( _ix_agg );

    if ( it != output_map_->end() )
        {
            return it->second;
        }
    else
        {
            return -1;
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql
