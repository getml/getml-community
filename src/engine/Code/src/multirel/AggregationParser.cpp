#include "multirel/aggregations/aggregations.hpp"

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::shared_ptr<aggregations::AbstractAggregation>
AggregationParser::parse_aggregation(
    const std::string& _aggregation,
    const enums::Mode _mode,
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const descriptors::SameUnitsContainer& _same_units_numerical,
    const descriptors::SameUnitsContainer& _same_units_discrete )
{
    if ( _mode == enums::Mode::fit )
        {
            return parse_aggregation<enums::Mode::fit>(
                _aggregation,
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }
    else
        {
            return parse_aggregation<enums::Mode::transform>(
                _aggregation,
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel
