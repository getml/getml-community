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

                    const std::string name = peripheral() + _input.name() +
                                             "." +
                                             _input.categorical_name( _column );

                    add_to_importances( name, _value );

                    return;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _column < _output.num_categoricals() );

                    const std::string name =
                        population() + _output.name() + "." +
                        _output.categorical_name( _column );

                    add_to_importances( name, _value );

                    return;
                }

            case enums::DataUsed::discrete_input_is_nan:
            case enums::DataUsed::discrete_input:
                {
                    assert_true( _column < _input.num_discretes() );

                    const std::string name = peripheral() + _input.name() +
                                             "." +
                                             _input.discrete_name( _column );

                    add_to_importances( name, _value );

                    return;
                }

            case enums::DataUsed::discrete_output_is_nan:
            case enums::DataUsed::discrete_output:
                {
                    assert_true( _column < _output.num_discretes() );

                    const std::string name = population() + _output.name() +
                                             "." +
                                             _output.discrete_name( _column );

                    add_to_importances( name, _value );

                    return;
                }

            case enums::DataUsed::numerical_input_is_nan:
            case enums::DataUsed::numerical_input:
                {
                    assert_true( _column < _input.num_numericals() );

                    const std::string name = peripheral() + _input.name() +
                                             "." +
                                             _input.numerical_name( _column );

                    add_to_importances( name, _value );

                    return;
                }

            case enums::DataUsed::numerical_output_is_nan:
            case enums::DataUsed::numerical_output:
                {
                    assert_true( _column < _output.num_numericals() );

                    const std::string name =
                        population() + _output.name() + "." +
                        _output.categorical_name( _column );

                    add_to_importances( name, _value );

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

            case enums::DataUsed::time_stamps_diff:
            case enums::DataUsed::time_stamps_window:
                {
                    const std::string name1 = peripheral() + _input.name() +
                                              "." + _input.time_stamps_name();

                    const std::string name2 = population() + _output.name() +
                                              "." + _output.time_stamps_name();

                    add_to_importances( name1, _value * 0.5 );

                    add_to_importances( name2, _value * 0.5 );

                    return;
                }

                // TODO
            case enums::DataUsed::subfeatures:
                assert_true( false && "TODO" );
                return;

            default:
                assert_true( false && "Unknown data_used_" );
                return;
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::add_to_importances(
    const std::string& _name, const Float _value )
{
    const auto it = importances_.find( _name );

    if ( it != importances_.end() )
        {
            it->second += _value;
            return;
        }

    importances_[_name] = _value;
}

// ----------------------------------------------------------------------------

void ImportanceMaker::merge( const std::map<std::string, Float>& _importances )
{
    for ( const auto& [key, value] : _importances )
        {
            add_to_importances( key, value );
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::multiply( const Float _importance_factor )
{
    for ( auto& [_, value] : importances_ )
        {
            value *= _importance_factor;
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::normalize()
{
    Float sum = 0.0;

    for ( const auto& [_, value] : importances_ )
        {
            sum += std::abs( value );
        }

    if ( sum > 0.0 )
        {
            for ( auto& [_, value] : importances_ )
                {
                    value /= sum;
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
