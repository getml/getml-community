#ifndef AUTOSQL_AGGREGATIONS_AGGREGATIONINDEX_HPP_
#define AUTOSQL_AGGREGATIONS_AGGREGATIONINDEX_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// Needed by the IntermediateAggregation to map ix_input to ix_aggregated.
class AggregationIndex
{
   public:
    AggregationIndex(
        const containers::DataFrameView& _input_table,
        const containers::DataFrameView& _output_table,
        const std::shared_ptr<const std::map<SQLNET_INT, SQLNET_INT>>&
            _output_map,
        const bool _use_timestamps )
        : input_table_( _input_table ),
          output_map_( _output_map ),
          output_table_( _output_table ),
          use_timestamps_( _use_timestamps )
    {
    }

    AggregationIndex() = default;

    ~AggregationIndex() = default;

    // ------------------------------------------------------------

    /// Returns the number of elements in input_table_ that are
    /// linked to the element of the output_table_ designated by
    /// _ix_agg
    const SQLNET_FLOAT get_count( const SQLNET_INT _ix_agg ) const;

    /// Maps _ix_input to all indices
    const std::vector<SQLNET_INT> transform( const SQLNET_INT _ix_input ) const;

    /// Transform ix_agg using the output map
    SQLNET_INT transform_ix_agg( const SQLNET_INT _ix_agg ) const;

    // ------------------------------------------------------------

   private:
    /// Checks whether _time_stamp_output is within the range defined by
    /// _time_stamp_input and _upper_time_stamp.
    bool time_stamp_output_in_range(
        const SQLNET_FLOAT _time_stamp_input,
        const SQLNET_FLOAT _upper_time_stamp,
        const SQLNET_FLOAT _time_stamp_output ) const
    {
        return (
            ( _time_stamp_input <= _time_stamp_output ) &&
            ( std::isnan( _upper_time_stamp ) ||
              _time_stamp_output < _upper_time_stamp ) );
    }

    // ------------------------------------------------------------

   private:
    /// Data frame that is aggregated (the right table)
    const containers::DataFrameView input_table_;

    /// Maps the indices of the underlying DataFrame to the indices of the
    /// DataFrameView (in effect reversing the indices in the DataFrameView)
    /// for the output table.
    const std::shared_ptr<const std::map<SQLNET_INT, SQLNET_INT>> output_map_;

    /// Data frame on which the input table is joined (the left table)
    const containers::DataFrameView output_table_;

    /// Whether we want to use timestamps
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_AGGREGATIONINDEX_HPP_
