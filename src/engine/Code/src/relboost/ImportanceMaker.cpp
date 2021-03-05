#include "relboost/utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

void ImportanceMaker::add(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const enums::DataUsed _data_used,
    const size_t _column,
    const size_t _column_input,
    const Float _value )
{
    switch ( _data_used )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _column < _input.num_categoricals() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.categorical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _column < _output.num_categoricals() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.categorical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::discrete_input_is_nan:
            case enums::DataUsed::discrete_input:
                {
                    assert_true( _column < _input.num_discretes() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.discrete_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::discrete_output_is_nan:
            case enums::DataUsed::discrete_output:
                {
                    assert_true( _column < _output.num_discretes() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.discrete_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::numerical_input_is_nan:
            case enums::DataUsed::numerical_input:
                {
                    assert_true( _column < _input.num_numericals() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.numerical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::numerical_output_is_nan:
            case enums::DataUsed::numerical_output:
                {
                    assert_true( _column < _output.num_numericals() );

                    const auto desc = helpers::ColumnDescription(
                        population(),
                        _output.name(),
                        _output.numerical_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::same_units_categorical:
                assert_true( _column < _output.num_categoricals() );
                assert_true( _column_input < _input.num_categoricals() );

                add( _input,
                     _output,
                     enums::DataUsed::categorical_output,
                     _column,
                     0,
                     _value * 0.5 );

                add( _input,
                     _output,
                     enums::DataUsed::categorical_input,
                     _column_input,
                     0,
                     _value * 0.5 );

                return;

            case enums::DataUsed::same_units_discrete_is_nan:
            case enums::DataUsed::same_units_discrete_ts:
            case enums::DataUsed::same_units_discrete:
                assert_true( _column < _output.num_discretes() );
                assert_true( _column_input < _input.num_discretes() );

                add( _input,
                     _output,
                     enums::DataUsed::discrete_output,
                     _column,
                     0,
                     _value * 0.5 );

                add( _input,
                     _output,
                     enums::DataUsed::discrete_input,
                     _column_input,
                     0,
                     _value * 0.5 );

                return;

            case enums::DataUsed::same_units_numerical_is_nan:
            case enums::DataUsed::same_units_numerical_ts:
            case enums::DataUsed::same_units_numerical:
                assert_true( _column < _output.num_numericals() );
                assert_true( _column_input < _input.num_numericals() );

                add( _input,
                     _output,
                     enums::DataUsed::numerical_output,
                     _column,
                     0,
                     _value * 0.5 );

                add( _input,
                     _output,
                     enums::DataUsed::numerical_input,
                     _column_input,
                     0,
                     _value * 0.5 );

                return;

            case enums::DataUsed::subfeatures:
                add_to_importance_factors( _column, _value );
                return;

            case enums::DataUsed::text_input:
                {
                    assert_true( _column < _input.num_text() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _input.name(),
                        _input.text_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

            case enums::DataUsed::text_output:
                {
                    assert_true( _column < _output.num_text() );

                    const auto desc = helpers::ColumnDescription(
                        peripheral(),
                        _output.name(),
                        _output.text_name( _column ) );

                    add_to_importances( desc, _value );

                    return;
                }

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

            default:
                assert_true( false && "Unknown data_used_" );
                return;
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
