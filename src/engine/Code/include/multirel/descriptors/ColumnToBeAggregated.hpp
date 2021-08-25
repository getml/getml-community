#ifndef MULTIREL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_
#define MULTIREL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------

struct ColumnToBeAggregated
{
    // ------------------------------------------------------

    ColumnToBeAggregated()
        : ix_column_used( 0 ),
          data_used( enums::DataUsed::not_applicable ),
          ix_perip_used( 0 ){};

    ColumnToBeAggregated(
        size_t _ix_column_used, enums::DataUsed _data_used, Int _ix_perip_used )
        : ix_column_used( _ix_column_used ),
          data_used( _data_used ),
          ix_perip_used( _ix_perip_used ){};

    ColumnToBeAggregated( const Poco::JSON::Object& _obj )
    {
        *this = from_json_obj( _obj );
    };

    ~ColumnToBeAggregated() = default;

    // ------------------------------------------------------

    /// Reconstructs a SameUnits object from a JSON object.
    ColumnToBeAggregated from_json_obj( const Poco::JSON::Object& _obj ) const
    {
        ColumnToBeAggregated col;

        col.ix_column_used = JSON::get_value<size_t>( _obj, "column_" );

        col.data_used =
            JSON::int_to_data_used( JSON::get_value<int>( _obj, "data_" ) );

        col.ix_perip_used = JSON::get_value<Int>( _obj, "input_" );

        return col;
    }

    // ------------------------------------------------------

    /// Transforms the SameUnits into a JSON object.
    Poco::JSON::Object to_json_obj() const
    {
        Poco::JSON::Object obj;

        obj.set( "column_", ix_column_used );

        obj.set( "data_", JSON::data_used_to_int( data_used ) );

        obj.set( "input_", ix_perip_used );

        return obj;
    }

    // ------------------------------------------------------

    size_t ix_column_used;

    enums::DataUsed data_used;

    Int ix_perip_used;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel

#endif  // MULTIREL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_
