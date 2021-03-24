#ifndef MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <typename BaseAggregationType, typename ParamsType>
class AggregationParser
{
   public:
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    static std::shared_ptr<BaseAggregationType> parse_aggregation(
        const std::string& _aggregation, const ParamsType& _params );

   private:
    /// Creates the aggregation.
    template <class AggType, enums::DataUsed data_used_, bool is_population_>
    static std::shared_ptr<BaseAggregationType> make_shared_ptr(
        const ParamsType& _params )
    {
        static_assert(
            std::is_same<BaseAggregationType, AbstractFitAggregation>() ||
                std::is_same<
                    BaseAggregationType,
                    AbstractTransformAggregation>(),
            "Unsupported AbstractAggregation" );

        if constexpr ( std::is_same<
                           BaseAggregationType,
                           AbstractFitAggregation>() )
            {
                return std::make_shared<
                    FitAggregation<AggType, data_used_, is_population_>>(
                    _params );
            }

        if constexpr ( std::is_same<
                           BaseAggregationType,
                           AbstractTransformAggregation>() )
            {
                return std::make_shared<
                    TransformAggregation<AggType, data_used_, is_population_>>(
                    _params );
            }
    };

    /// Actually creates the aggregation based on the AggType and other
    /// information.
    template <typename AggType>
    static std::shared_ptr<BaseAggregationType> make_aggregation(
        const ParamsType& _params )
    {
        switch ( _params.column_to_be_aggregated.data_used )
            {
                case enums::DataUsed::x_perip_numerical:

                    return make_shared_ptr<
                        AggType,
                        enums::DataUsed::x_perip_numerical,
                        false>( _params );

                case enums::DataUsed::x_perip_discrete:

                    return make_shared_ptr<
                        AggType,
                        enums::DataUsed::x_perip_discrete,
                        false>( _params );

                case enums::DataUsed::time_stamps_diff:

                    return make_shared_ptr<
                        AggType,
                        enums::DataUsed::time_stamps_diff,
                        true>( _params );

                case enums::DataUsed::same_unit_numerical:
                case enums::DataUsed::same_unit_numerical_ts:
                    {
                        const auto ix_column_used =
                            _params.column_to_be_aggregated.ix_column_used;

                        assert_true(
                            ix_column_used <
                            _params.same_units_numerical.size() );

                        const auto data_used2 =
                            std::get<1>( _params.same_units_numerical.at(
                                             ix_column_used ) )
                                .data_used;

                        if ( data_used2 == enums::DataUsed::x_popul_numerical )
                            {
                                return make_shared_ptr<
                                    AggType,
                                    enums::DataUsed::same_unit_numerical,
                                    true>( _params );
                            }

                        if ( data_used2 == enums::DataUsed::x_perip_numerical )
                            {
                                return make_shared_ptr<
                                    AggType,
                                    enums::DataUsed::same_unit_numerical,
                                    false>( _params );
                            }

                        assert_true(
                            !"Unknown data_used2 in make_aggregation(...)!" );

                        return std::shared_ptr<BaseAggregationType>();
                    }

                case enums::DataUsed::same_unit_discrete:
                case enums::DataUsed::same_unit_discrete_ts:
                    {
                        const auto ix_column_used =
                            _params.column_to_be_aggregated.ix_column_used;

                        assert_true(
                            ix_column_used <
                            _params.same_units_discrete.size() );

                        const enums::DataUsed data_used2 =
                            std::get<1>( _params.same_units_discrete.at(
                                             ix_column_used ) )
                                .data_used;

                        if ( data_used2 == enums::DataUsed::x_popul_discrete )
                            {
                                return make_shared_ptr<
                                    AggType,
                                    enums::DataUsed::same_unit_discrete,
                                    true>( _params );
                            }

                        if ( data_used2 == enums::DataUsed::x_perip_discrete )
                            {
                                return make_shared_ptr<
                                    AggType,
                                    enums::DataUsed::same_unit_discrete,
                                    false>( _params );
                            }

                        assert_true(
                            !"Unknown data_used2 in make_aggregation(...)!" );

                        return std::shared_ptr<BaseAggregationType>();
                    }

                case enums::DataUsed::x_perip_categorical:

                    return make_shared_ptr<
                        AggType,
                        enums::DataUsed::x_perip_categorical,
                        false>( _params );

                case enums::DataUsed::x_subfeature:

                    return make_shared_ptr<
                        AggType,
                        enums::DataUsed::x_subfeature,
                        false>( _params );

                case enums::DataUsed::not_applicable:

                    assert_true( !"Only COUNT does not return!" );

                    return std::shared_ptr<BaseAggregationType>();

                default:

                    assert_true(
                        !"Unknown enums::DataUsed in make_aggregation(...)!" );

                    return std::shared_ptr<BaseAggregationType>();
            }
    }
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename BaseAggregationType, typename ParamsType>
std::shared_ptr<BaseAggregationType>
AggregationParser<BaseAggregationType, ParamsType>::parse_aggregation(
    const std::string& _aggregation, const ParamsType& _params )
{
    if ( _aggregation == AggregationType::Avg::type() )
        {
            return make_aggregation<AggregationType::Avg>( _params );
        }

    if ( _aggregation == AggregationType::Count::type() )
        {
            assert_true(
                _params.column_to_be_aggregated.data_used ==
                enums::DataUsed::not_applicable );

            return make_shared_ptr<
                AggregationType::Count,
                enums::DataUsed::not_applicable,
                true>( _params );
        }

    if ( _aggregation == AggregationType::CountDistinct::type() )
        {
            return make_aggregation<AggregationType::CountDistinct>( _params );
        }

    if ( _aggregation == AggregationType::CountMinusCountDistinct::type() )
        {
            return make_aggregation<AggregationType::CountMinusCountDistinct>(
                _params );
        }

    if ( _aggregation == AggregationType::First::type() )
        {
            return make_aggregation<AggregationType::First>( _params );
        }

    if ( _aggregation == AggregationType::Last::type() )
        {
            return make_aggregation<AggregationType::Last>( _params );
        }

    if ( _aggregation == AggregationType::Max::type() )
        {
            return make_aggregation<AggregationType::Max>( _params );
        }

    if ( _aggregation == AggregationType::Median::type() )
        {
            return make_aggregation<AggregationType::Median>( _params );
        }

    if ( _aggregation == AggregationType::Min::type() )
        {
            return make_aggregation<AggregationType::Min>( _params );
        }

    if ( _aggregation == AggregationType::Skewness::type() )
        {
            return make_aggregation<AggregationType::Skewness>( _params );
        }

    if ( _aggregation == AggregationType::Stddev::type() )
        {
            return make_aggregation<AggregationType::Stddev>( _params );
        }

    if ( _aggregation == AggregationType::Sum::type() )
        {
            return make_aggregation<AggregationType::Sum>( _params );
        }

    if ( _aggregation == AggregationType::Var::type() )
        {
            return make_aggregation<AggregationType::Var>( _params );
        }

    const auto warning_message =
        "Aggregation of type '" + _aggregation + "' not known!";

    throw std::invalid_argument( warning_message );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
