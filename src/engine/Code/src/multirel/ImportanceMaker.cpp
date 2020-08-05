#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

void ImportanceMaker::add(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const enums::DataUsed _data_used,
    const size_t _column,
    const descriptors::SameUnits& _same_units,
    const Float _value )
{
    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_categorical:
                {
                    assert_true( _column < _input.num_categoricals() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.categorical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::x_popul_categorical:
                {
                    assert_true( _column < _output.num_categoricals() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.categorical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::x_perip_discrete:
                {
                    assert_true( _column < _input.num_discretes() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.discrete_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::x_popul_discrete:
                {
                    assert_true( _column < _output.num_discretes() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.discrete_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::x_perip_numerical:
                {
                    assert_true( _column < _input.num_numericals() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.numerical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::x_popul_numerical:
                {
                    assert_true( _column < _output.num_numericals() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.numerical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::same_unit_categorical:
                {
                    assert_true( _same_units.same_units_categorical_ );
                    assert_true(
                        _column < _same_units.same_units_categorical_->size() );

                    const auto& upair =
                        _same_units.same_units_categorical_->at( _column );

                    add( _input,
                         _output,
                         std::get<0>( upair ).data_used,
                         std::get<0>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    add( _input,
                         _output,
                         std::get<1>( upair ).data_used,
                         std::get<1>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    return;
                }

            case enums::DataUsed::same_unit_discrete_ts:
            case enums::DataUsed::same_unit_discrete:
                {
                    assert_true( _same_units.same_units_discrete_ );
                    assert_true(
                        _column < _same_units.same_units_discrete_->size() );

                    const auto& upair =
                        _same_units.same_units_discrete_->at( _column );

                    add( _input,
                         _output,
                         std::get<0>( upair ).data_used,
                         std::get<0>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    add( _input,
                         _output,
                         std::get<1>( upair ).data_used,
                         std::get<1>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    return;
                }

            case enums::DataUsed::same_unit_numerical_ts:
            case enums::DataUsed::same_unit_numerical:
                {
                    assert_true( _same_units.same_units_numerical_ );
                    assert_true(
                        _column < _same_units.same_units_numerical_->size() );

                    const auto& upair =
                        _same_units.same_units_numerical_->at( _column );

                    add( _input,
                         _output,
                         std::get<0>( upair ).data_used,
                         std::get<0>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    add( _input,
                         _output,
                         std::get<1>( upair ).data_used,
                         std::get<1>( upair ).ix_column_used,
                         _same_units,
                         _value * 0.5 );

                    return;
                }

            case enums::DataUsed::time_stamps_diff:
            case enums::DataUsed::time_stamps_window:
                {
                    const auto desc1 = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.time_stamps_name() );

                    const auto desc2 = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.time_stamps_name() );

                    add_to_importances( desc1, _value * 0.5 );

                    add_to_importances( desc2, _value * 0.5 );

                    return;
                }

            case enums::DataUsed::x_subfeature:
                add_to_importance_factors( _column, _value );
                return;

            case enums::DataUsed::not_applicable:
                return;

            default:
                assert_true( false && "Unknown data_used_" );
                return;
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel
