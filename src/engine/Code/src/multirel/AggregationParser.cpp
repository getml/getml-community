#include "multirel/aggregations/aggregations.hpp"

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::shared_ptr<aggregations::AbstractAggregation>
AggregationParser::parse_aggregation(
    const std::string& _aggregation,
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const descriptors::SameUnitsContainer& _same_units_numerical,
    const descriptors::SameUnitsContainer& _same_units_discrete )
{
    if ( _aggregation == "AVG" )
        {
            return make_aggregation<aggregations::AggregationType::Avg>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "COUNT" )
        {
            return make_aggregation<aggregations::AggregationType::Count>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountDistinct>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "COUNT MINUS COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountMinusCountDistinct>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "MAX" )
        {
            return make_aggregation<aggregations::AggregationType::Max>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "MEDIAN" )
        {
            return make_aggregation<aggregations::AggregationType::Median>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "MIN" )
        {
            return make_aggregation<aggregations::AggregationType::Min>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "SKEWNESS" )
        {
            return make_aggregation<aggregations::AggregationType::Skewness>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "STDDEV" )
        {
            return make_aggregation<aggregations::AggregationType::Stddev>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "SUM" )
        {
            return make_aggregation<aggregations::AggregationType::Sum>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else if ( _aggregation == "VAR" )
        {
            return make_aggregation<aggregations::AggregationType::Var>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    else
        {
            std::string warning_message = "Aggregation of type '";
            warning_message.append( _aggregation );
            warning_message.append( "' not known!" );

            throw std::invalid_argument( warning_message );
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel