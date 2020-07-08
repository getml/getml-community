#include "multirel/decisiontrees/decisiontrees.hpp"

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

void ValueToBeAggregatedCreator::create(
    const DecisionTreeImpl &_impl,
    const descriptors::ColumnToBeAggregated &_column_to_be_aggregated,
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    aggregations::AbstractAggregation *_aggregation )
{
    // ------------------------------------------------------------------------

    const auto ix_column_used = _column_to_be_aggregated.ix_column_used;

    switch ( _column_to_be_aggregated.data_used )
        {
            case enums::DataUsed::x_perip_numerical:

                debug_log( "x_perip_numerical" );

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.numerical_col( ix_column_used ) );

                break;

            case enums::DataUsed::x_perip_discrete:

                debug_log( "x_perip_discrete" );

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.discrete_col( ix_column_used ) );

                break;

            case enums::DataUsed::time_stamps_diff:

                debug_log( "time_stamps_diff" );

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.time_stamp_col() );

                _aggregation->set_value_to_be_compared(
                    _population.time_stamp_col() );

                break;

            case enums::DataUsed::same_unit_numerical:
            case enums::DataUsed::same_unit_numerical_ts:

                debug_log( "same_unit_numerical" );

                create_same_unit_numerical(
                    _impl,
                    ix_column_used,
                    _population,
                    _peripheral,
                    _aggregation );

                break;

            case enums::DataUsed::same_unit_discrete:
            case enums::DataUsed::same_unit_discrete_ts:

                debug_log( "same_unit_discrete" );

                create_same_unit_discrete(
                    _impl,
                    ix_column_used,
                    _population,
                    _peripheral,
                    _aggregation );

                break;

            case enums::DataUsed::x_perip_categorical:

                debug_log( "x_perip_categorical" );

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.categorical_col( ix_column_used ) );

                break;

            case enums::DataUsed::x_subfeature:

                debug_log( "x_subfeature" );

                assert_true( ix_column_used < _subfeatures.size() );

                _aggregation->set_value_to_be_aggregated(
                    _subfeatures[ix_column_used] );

                break;

            case enums::DataUsed::not_applicable:

                debug_log( "not_applicable" );

                break;

            default:

                assert_true( !"Unknown enums::DataUsed in column_to_be_aggregated(...)!" );
        }

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void ValueToBeAggregatedCreator::create_same_unit_discrete(
    const DecisionTreeImpl &_impl,
    size_t _ix_column_used,
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    aggregations::AbstractAggregation *_aggregation )
{
    // -----------------------------------------------------------------

    assert_true( _impl.same_units_discrete().size() > _ix_column_used );

    // -----------------------------------------------------------------

    const auto data_used1 =
        std::get<0>( _impl.same_units_discrete().at( _ix_column_used ) )
            .data_used;

    const auto data_used2 =
        std::get<1>( _impl.same_units_discrete().at( _ix_column_used ) )
            .data_used;

    const auto ix_column_used1 =
        std::get<0>( _impl.same_units_discrete().at( _ix_column_used ) )
            .ix_column_used;

    const auto ix_column_used2 =
        std::get<1>( _impl.same_units_discrete().at( _ix_column_used ) )
            .ix_column_used;

    // -----------------------------------------------------------------

    if ( data_used1 == enums::DataUsed::x_perip_discrete )
        {
            assert_true( _peripheral.num_discretes() > ix_column_used1 );

            _aggregation->set_value_to_be_aggregated(
                _peripheral.discrete_col( ix_column_used1 ) );
        }
    else
        {
            assert_true(
                !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
        }

    // -----------------------------------------------------------------

    if ( data_used2 == enums::DataUsed::x_popul_discrete )
        {
            assert_true( _population.num_discretes() > ix_column_used2 );

            _aggregation->set_value_to_be_compared(
                _population.discrete_col( ix_column_used2 ) );
        }
    else if ( data_used2 == enums::DataUsed::x_perip_discrete )
        {
            assert_true( _peripheral.num_discretes() > ix_column_used2 );

            _aggregation->set_value_to_be_compared(
                _peripheral.discrete_col( ix_column_used2 ) );
        }
    else
        {
            assert_true(
                !"Unknown data_used2 in set_value_to_be_compared(...)!" );
        }

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void ValueToBeAggregatedCreator::create_same_unit_numerical(
    const DecisionTreeImpl &_impl,
    size_t _ix_column_used,
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    aggregations::AbstractAggregation *_aggregation )
{
    // -----------------------------------------------------------------

    assert_true(
        static_cast<Int>( _impl.same_units_numerical().size() ) >
        _ix_column_used );

    // -----------------------------------------------------------------

    const auto data_used1 =
        std::get<0>( _impl.same_units_numerical().at( _ix_column_used ) )
            .data_used;

    const auto data_used2 =
        std::get<1>( _impl.same_units_numerical().at( _ix_column_used ) )
            .data_used;

    const auto ix_column_used1 =
        std::get<0>( _impl.same_units_numerical().at( _ix_column_used ) )
            .ix_column_used;

    const auto ix_column_used2 =
        std::get<1>( _impl.same_units_numerical().at( _ix_column_used ) )
            .ix_column_used;

    // -----------------------------------------------------------------

    if ( data_used1 == enums::DataUsed::x_perip_numerical )
        {
            assert_true( _peripheral.num_numericals() > ix_column_used1 );

            debug_log( "set_value_to_be_aggregated" );

            _aggregation->set_value_to_be_aggregated(
                _peripheral.numerical_col( ix_column_used1 ) );
        }
    else
        {
            assert_true(
                !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
        }

    // -----------------------------------------------------------------

    if ( data_used2 == enums::DataUsed::x_popul_numerical )
        {
            assert_true( _population.num_numericals() > ix_column_used2 );

            const auto col = _population.numerical_col( ix_column_used2 );

            _aggregation->set_value_to_be_compared( col );
        }
    else if ( data_used2 == enums::DataUsed::x_perip_numerical )
        {
            assert_true( _peripheral.num_numericals() > ix_column_used2 );

            _aggregation->set_value_to_be_compared(
                _peripheral.numerical_col( ix_column_used2 ) );
        }
    else
        {
            assert_true(
                !"Unknown data_used2 in set_value_to_be_compared(...)!" );
        }

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel
