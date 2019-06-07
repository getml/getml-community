#include "autosql/aggregations/aggregations.hpp"

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

const AUTOSQL_FLOAT AggregationIndex::get_count(
    const AUTOSQL_INT _ix_agg ) const
{
    assert( _ix_agg >= 0 );
    assert( _ix_agg < output_table_.nrows() );

    assert( input_table_.df().has( output_table_.join_key( _ix_agg ) ) );

    auto it = input_table_.df().find( output_table_.join_key( _ix_agg ) );

    AUTOSQL_FLOAT count = 0.0;

    const AUTOSQL_FLOAT time_stamp_output = output_table_.time_stamp( _ix_agg );

    for ( auto ix_input : it->second )
        {
            const bool use_this_sample =
                ( !use_timestamps_ ||
                  time_stamp_output_in_range(
                      input_table_.df().time_stamp( ix_input ),
                      input_table_.df().upper_time_stamp( ix_input ),
                      time_stamp_output ) );

            if ( use_this_sample )
                {
                    ++count;
                }
        }

    return count;
}

// ----------------------------------------------------------------------------

const std::vector<AUTOSQL_INT> AggregationIndex::transform(
    const AUTOSQL_INT _ix_input ) const
{
    assert( _ix_input >= 0 );
    assert( _ix_input < input_table_.nrows() );

    if ( !output_table_.df().has( input_table_.join_key( _ix_input ) ) )
        {
            return std::vector<AUTOSQL_INT>();
        }

    auto it = output_table_.df().find( input_table_.join_key( _ix_input ) );

    const AUTOSQL_FLOAT time_stamp_input = input_table_.time_stamp( _ix_input );

    const AUTOSQL_FLOAT upper_time_stamp =
        input_table_.upper_time_stamp( _ix_input );

    std::vector<AUTOSQL_INT> indices;

    for ( auto ix_agg : it->second )
        {
            assert( ix_agg >= 0 );
            assert( ix_agg < output_table_.df().nrows() );

            const bool use_this_sample =
                ( !use_timestamps_ ||
                  time_stamp_output_in_range(
                      time_stamp_input,
                      upper_time_stamp,
                      output_table_.df().time_stamp( ix_agg ) ) );

            if ( use_this_sample )
                {
                    auto ix_agg_tr = transform_ix_agg( ix_agg );

                    if ( ix_agg_tr != -1 )
                        {
                            assert( ix_agg == output_table_.rows()[ix_agg_tr] );

                            indices.push_back( ix_agg_tr );
                        }
                }
        }

    return indices;
}

// ----------------------------------------------------------------------------

AUTOSQL_INT AggregationIndex::transform_ix_agg(
    const AUTOSQL_INT _ix_agg ) const
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
