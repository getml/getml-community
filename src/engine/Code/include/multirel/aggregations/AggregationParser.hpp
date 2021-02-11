#ifndef MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

class AggregationParser
{
   public:
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    /// This is a separate method to make sure that the aggregations are
    /// actually compiled in the aggregation module.
    static std::shared_ptr<aggregations::AbstractAggregation> parse_aggregation(
        const std::string& _aggregation,
        const enums::Mode _mode,
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::SameUnitsContainer& _same_units_discrete );

   private:
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    template <enums::Mode _mode>
    static std::shared_ptr<aggregations::AbstractAggregation> parse_aggregation(
        const std::string& _aggregation,
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::SameUnitsContainer& _same_units_discrete );

    /// Actually creates the aggregation based on the AggType and other
    /// information.
    template <typename AggType, enums::Mode _mode>
    static std::shared_ptr<AbstractAggregation> make_aggregation(
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::SameUnitsContainer& _same_units_discrete );
};

// ----------------------------------------------------------------------------

}  // namespace aggregations
}  // namespace multirel

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <typename AggType, enums::Mode _mode>
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
                    _mode,
                    false>>();

                break;

            case enums::DataUsed::x_perip_discrete:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_perip_discrete,
                    _mode,
                    false>>();

                break;

            case enums::DataUsed::time_stamps_diff:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::time_stamps_diff,
                    _mode,
                    true>>();

                break;

            case enums::DataUsed::same_unit_numerical:
            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const enums::DataUsed data_used2 =
                        std::get<1>( _same_units_numerical[_ix_column_used] )
                            .data_used;

                    if ( data_used2 == enums::DataUsed::x_popul_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_numerical,
                                _mode,
                                true>>();
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_numerical,
                                _mode,
                                false>>();
                        }
                    else
                        {
                            assert_true( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AbstractAggregation>();
                        }
                }

                break;

            case enums::DataUsed::same_unit_discrete:
            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const enums::DataUsed data_used2 =
                        std::get<1>( _same_units_discrete[_ix_column_used] )
                            .data_used;

                    if ( data_used2 == enums::DataUsed::x_popul_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_discrete,
                                _mode,
                                true>>();
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                enums::DataUsed::same_unit_discrete,
                                _mode,
                                false>>();
                        }
                    else
                        {
                            assert_true( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AbstractAggregation>();
                        }
                }

                break;

            case enums::DataUsed::x_perip_categorical:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_perip_categorical,
                    _mode,
                    false>>();

                break;

            case enums::DataUsed::x_subfeature:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::x_subfeature,
                    _mode,
                    false>>();

                break;

            case enums::DataUsed::not_applicable:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    enums::DataUsed::not_applicable,
                    _mode,
                    false>>();

                break;

            default:

                assert_true(
                    !"Unknown enums::DataUsed in make_aggregation(...)!" );

                return std::shared_ptr<aggregations::AbstractAggregation>();
        }
}

// ----------------------------------------------------------------------------

template <enums::Mode _mode>
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
            return make_aggregation<aggregations::AggregationType::Avg, _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "COUNT" )
        {
            return make_aggregation<
                aggregations::AggregationType::Count,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountDistinct,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "COUNT MINUS COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountMinusCountDistinct,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "FIRST" )
        {
            return make_aggregation<
                aggregations::AggregationType::First,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "LAST" )
        {
            return make_aggregation<aggregations::AggregationType::Last, _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "MAX" )
        {
            return make_aggregation<aggregations::AggregationType::Max, _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "MEDIAN" )
        {
            return make_aggregation<
                aggregations::AggregationType::Median,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "MIN" )
        {
            return make_aggregation<aggregations::AggregationType::Min, _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "SKEWNESS" )
        {
            return make_aggregation<
                aggregations::AggregationType::Skewness,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "STDDEV" )
        {
            return make_aggregation<
                aggregations::AggregationType::Stddev,
                _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "SUM" )
        {
            return make_aggregation<aggregations::AggregationType::Sum, _mode>(
                _data_used,
                _ix_column_used,
                _same_units_numerical,
                _same_units_discrete );
        }

    if ( _aggregation == "VAR" )
        {
            return make_aggregation<aggregations::AggregationType::Var, _mode>(
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

#endif  // MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
