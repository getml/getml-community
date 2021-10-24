#ifndef RELMT_AGGREGATIONS_AGGREGATIONINDEX_HPP_
#define RELMT_AGGREGATIONS_AGGREGATIONINDEX_HPP_

namespace relmt
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// Needed by the intermediate aggregation to map ix_input to ix_aggregated.
class AggregationIndex
{
   public:
    AggregationIndex(
        const containers::DataFrameView& _input_table,
        const containers::DataFrameView& _output_table,
        const std::shared_ptr<const std::map<Int, Int>>& _input_map,
        const std::shared_ptr<const std::map<Int, Int>>& _output_map )
        : input_map_( _input_map ),
          input_table_( _input_table ),
          output_map_( _output_map ),
          output_table_( _output_table )
    {
    }

    ~AggregationIndex() = default;

    // ------------------------------------------------------------

   public:
    /// Returns the number of elements in input_table_ that are
    /// linked to the element of the output_table_ designated by
    /// _ix_agg
    const Float get_count( const Int _ix_agg ) const;

    /// Matches the sample weights returned by the parent to make "sample
    /// weights" for the subfeatures.
    std::shared_ptr<std::vector<Float>> make_sample_weights(
        const std::shared_ptr<const std::vector<Float>> _sample_weights_parent )
        const;

    /// Maps _ix_input to all indices
    const std::vector<Int> transform( const size_t _ix_input ) const;

    /// Transform ix_agg using the output map
    Int transform_ix_agg(
        const Int _ix_agg, const std::map<Int, Int>& _rows_map ) const;

    // ------------------------------------------------------------

   public:
    /// Returns the number of rows in the output table.
    size_t nrows() const { return output_table_.nrows(); }

    // ------------------------------------------------------------

   private:
    /// Checks whether _time_stamp_output is within the range defined by
    /// _time_stamp_input and _upper_time_stamp.
    bool time_stamp_output_in_range(
        const Float _time_stamp_input,
        const Float _upper_time_stamp,
        const Float _time_stamp_output ) const
    {
        return (
            ( _time_stamp_input <= _time_stamp_output ) &&
            ( std::isnan( _upper_time_stamp ) ||
              _time_stamp_output < _upper_time_stamp ) );
    }

    // ------------------------------------------------------------

   private:
    /// Maps the indices of the underlying DataFrame to the row indices of the
    /// DataFrameView (in effect reversing the row indices in the DataFrameView)
    /// for the INPUT table.
    const std::shared_ptr<const std::map<Int, Int>> input_map_;

    /// Data frame that is aggregated (the right table)
    const containers::DataFrameView input_table_;

    /// Maps the indices of the underlying DataFrame to the row indices of the
    /// DataFrameView (in effect reversing the row indices in the DataFrameView)
    /// for the OUTPUT table.
    const std::shared_ptr<const std::map<Int, Int>> output_map_;

    /// Data frame on which the input table is joined (the left table)
    const containers::DataFrameView output_table_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

#endif  // RELMT_AGGREGATIONS_AGGREGATIONINDEX_HPP_

