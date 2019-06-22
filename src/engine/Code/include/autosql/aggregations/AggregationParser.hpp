#ifndef AUTOSQL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define AUTOSQL_AGGREGATIONS_AGGREGATIONPARSER_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

class AggregationParser
{
   public:
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    static std::shared_ptr<aggregations::AbstractAggregation> parse_aggregation(
        const std::string& _aggregation,
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::SameUnitsContainer& _same_units_discrete );

   private:
    /// Actually creates the aggregation based on the AggType and other
    /// information.
    template <typename AggType>
    static std::shared_ptr<AbstractAggregation> make_aggregation(
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::SameUnitsContainer& _same_units_discrete );
};

// ----------------------------------------------------------------------------

}  // namespace aggregations
}  // namespace autosql

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <typename AggType>
std::shared_ptr<AbstractAggregation> AggregationParser::make_aggregation(
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const descriptors::SameUnitsContainer& _same_units_numerical,
    const descriptors::SameUnitsContainer& _same_units_discrete )
{
    // ------------------------------------------------------------------------

    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_numerical:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_perip_numerical,
                    false>>();

                break;

            case enums::DataUsed::x_perip_discrete:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_perip_discrete,
                    false>>();

                break;

            case enums::DataUsed::time_stamps_diff:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::time_stamps_diff,
                    true>>();

                break;

            case enums::DataUsed::same_unit_numerical:

                {
                    const enums::DataUsed data_used2 =
                        std::get<1>( _same_units_numerical[_ix_column_used] )
                            .data_used;

                    if ( data_used2 == enums::DataUsed::x_popul_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_numerical,
                                true>>();
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_numerical,
                                false>>();
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AbstractAggregation>();
                        }
                }

                break;

            case enums::DataUsed::same_unit_discrete:

                {
                    const enums::DataUsed data_used2 =
                        std::get<1>( _same_units_discrete[_ix_column_used] )
                            .data_used;

                    if ( data_used2 == enums::DataUsed::x_popul_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_discrete,
                                true>>();
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_discrete,
                                false>>();
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AbstractAggregation>();
                        }
                }

                break;

            case enums::DataUsed::x_perip_categorical:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_perip_categorical,
                    false>>();

                break;

            case enums::DataUsed::x_subfeature:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_subfeature,
                    false>>();

                break;

            case enums::DataUsed::not_applicable:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::not_applicable,
                    false>>();

                break;

            default:

                assert( !"Unknown enums::DataUsed in make_aggregation(...)!" );

                return std::shared_ptr<aggregations::AbstractAggregation>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
