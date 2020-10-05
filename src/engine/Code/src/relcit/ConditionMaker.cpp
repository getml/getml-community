#include "relcit/utils/utils.hpp"

namespace relcit
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_greater(
    const std::vector<strings::String>& _categories,
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const std::string condition =
                        "( t2.\"" + _input.categorical_name( _split.column_ ) +
                        "\" IN " + list_categories( _categories, _split ) +
                        " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const std::string condition =
                        "( t1.\"" + _output.categorical_name( _split.column_ ) +
                        "\" IN " + list_categories( _categories, _split ) +
                        " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2.\"" + _input.discrete_name( _split.column_ ) +
                       "\" > " + std::to_string( _split.critical_value_ ) +
                       " )";

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2.\"" + _input.discrete_name( _split.column_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::discrete_output:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" > " + std::to_string( _split.critical_value_ ) +
                       " )";

            case enums::DataUsed::discrete_output_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::numerical_input:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2.\"" + _input.numerical_name( _split.column_ ) +
                       "\" > " + std::to_string( _split.critical_value_ ) +
                       " )";

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2.\"" + _input.numerical_name( _split.column_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::numerical_output:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" > " + std::to_string( _split.critical_value_ ) +
                       " )";

            case enums::DataUsed::numerical_output_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::same_units_categorical:
                assert_true( _split.column_ < _output.num_categoricals() );
                assert_true( _split.column_input_ < _input.num_categoricals() );
                return "( t1.\"" + _output.categorical_name( _split.column_ ) +
                       "\" = t2.\"" +
                       _input.categorical_name( _split.column_input_ ) + "\" )";

            case enums::DataUsed::same_units_discrete:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" - t2.\"" +
                       _input.discrete_name( _split.column_input_ ) + "\" > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NOT NULL AND t2.\"" +
                       _input.discrete_name( _split.column_input_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::same_units_discrete_ts:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return make_time_stamp_diff(
                    _output.discrete_name( _split.column_ ),
                    _input.discrete_name( _split.column_input_ ),
                    _split.critical_value_,
                    true );

            case enums::DataUsed::same_units_numerical:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" - t2.\"" +
                       _input.numerical_name( _split.column_input_ ) + "\" > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NOT NULL AND t2.\"" +
                       _input.numerical_name( _split.column_input_ ) +
                       "\" IS NOT NULL )";

            case enums::DataUsed::same_units_numerical_ts:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return make_time_stamp_diff(
                    _output.numerical_name( _split.column_ ),
                    _input.numerical_name( _split.column_input_ ),
                    _split.critical_value_,
                    true );

            case enums::DataUsed::subfeatures:
                return "( t2.\"feature_" +
                       std::to_string( peripheral_used_ + 1 ) + "_" +
                       std::to_string( _split.column_ + 1 ) + "\" > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1.\"" + _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() + "\" > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::time_stamps_window:
                return "( t1.\"" + _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() + "\" > " +
                       std::to_string( _split.critical_value_ ) + " AND t1.\"" +
                       _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() + "\" <= " +
                       std::to_string( _split.critical_value_ + lag_ ) + " )";

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_smaller(
    const std::vector<strings::String>& _categories,
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const std::string condition =
                        "( t2.\"" + _input.categorical_name( _split.column_ ) +
                        "\" NOT IN " + list_categories( _categories, _split ) +
                        " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const std::string condition =
                        "( t1.\"" + _output.categorical_name( _split.column_ ) +
                        "\" NOT IN " + list_categories( _categories, _split ) +
                        " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2.\"" + _input.discrete_name( _split.column_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2.\"" + _input.discrete_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2.\"" + _input.discrete_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::discrete_output:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::discrete_output_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::numerical_input:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2.\"" + _input.numerical_name( _split.column_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2.\"" + _input.numerical_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2.\"" + _input.numerical_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::numerical_output:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::numerical_output_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::same_units_categorical:
                assert_true( _split.column_ < _output.num_categoricals() );
                assert_true( _split.column_input_ < _input.num_categoricals() );
                return "( t1.\"" + _output.categorical_name( _split.column_ ) +
                       "\" != t2.\"" +
                       _input.categorical_name( _split.column_input_ ) + "\" )";

            case enums::DataUsed::same_units_discrete:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" - t2.\"" +
                       _input.discrete_name( _split.column_input_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NULL OR t2.\"" +
                       _input.discrete_name( _split.column_input_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1.\"" + _output.discrete_name( _split.column_ ) +
                       "\" IS NULL OR t2.\"" +
                       _input.discrete_name( _split.column_input_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::same_units_discrete_ts:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return make_time_stamp_diff(
                    _output.discrete_name( _split.column_ ),
                    _input.discrete_name( _split.column_input_ ),
                    _split.critical_value_,
                    false );

            case enums::DataUsed::same_units_numerical:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" - t2.\"" +
                       _input.numerical_name( _split.column_input_ ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NULL OR t2.\"" +
                       _input.numerical_name( _split.column_input_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1.\"" + _output.numerical_name( _split.column_ ) +
                       "\" IS NULL OR t2.\"" +
                       _input.numerical_name( _split.column_input_ ) +
                       "\" IS NULL )";

            case enums::DataUsed::same_units_numerical_ts:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return make_time_stamp_diff(
                    _output.numerical_name( _split.column_ ),
                    _input.numerical_name( _split.column_input_ ),
                    _split.critical_value_,
                    false );

            case enums::DataUsed::subfeatures:
                return "( t2.\"feature_" +
                       std::to_string( peripheral_used_ + 1 ) + "_" +
                       std::to_string( _split.column_ + 1 ) +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1.\"" + _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.time_stamps_name() +
                       "\" IS NULL OR t2.\"" + _input.time_stamps_name() +
                       "\" IS NULL )";

            case enums::DataUsed::time_stamps_window:
                return "( t1.\"" + _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() +
                       "\" <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1.\"" + _output.time_stamps_name() + "\" - t2.\"" +
                       _input.time_stamps_name() + "\" > " +
                       std::to_string( _split.critical_value_ + lag_ ) +
                       " OR t1.\"" + _output.time_stamps_name() +
                       "\" IS NULL OR t2.\"" + _input.time_stamps_name() +
                       "\" IS NULL )";

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::list_categories(
    const std::vector<strings::String>& _categories,
    const containers::Split& _split ) const
{
    std::string categories = "( ";

    assert_true( _split.categories_used_begin_ <= _split.categories_used_end_ );

    for ( auto it = _split.categories_used_begin_;
          it != _split.categories_used_end_;
          ++it )
        {
            assert_true( *it < _categories.size() );

            categories += "'" + _categories.at( *it ).str() + "'";

            if ( std::next( it, 1 ) != _split.categories_used_end_ )
                {
                    categories += ", ";
                }
        }

    categories += " )";

    return categories;
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_equation(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const std::vector<Float>& _weights ) const
{
    assert_true(
        _weights.size() ==
        input_scaler().means().size() + output_scaler().means().size() + 1 );

    const auto rescaled_weights = rescale( _weights );

    std::stringstream equation;

    equation << std::scientific;

    size_t i = 1;

    for ( size_t j = 0; j < _input.num_discretes(); ++i, ++j )
        {
            equation << "t2.\"" << _input.discrete_name( j ) << "\" * "
                     << rescaled_weights.at( i ) << " + ";
        }

    for ( size_t j = 0; j < _input.num_numericals(); ++i, ++j )
        {
            equation << "t2.\"" << _input.numerical_name( j ) << "\" * "
                     << rescaled_weights.at( i ) << " + ";
        }

    for ( size_t j = 0; j < _output.num_discretes(); ++i, ++j )
        {
            equation << "t1.\"" << _output.discrete_name( j ) << "\" * "
                     << rescaled_weights.at( i ) << " + ";
        }

    for ( size_t j = 0; j < _output.num_numericals(); ++i, ++j )
        {
            equation << "t1.\"" << _output.numerical_name( j ) << "\" * "
                     << rescaled_weights.at( i ) << " + ";
        }

    for ( size_t j = 0; i < rescaled_weights.size(); ++i, ++j )
        {
            equation << "t2.\"feature_" << peripheral_used_ + 1 << "_" << j + 1
                     << "\" * " << rescaled_weights.at( i ) << " + ";
        }

    equation << rescaled_weights.at( 0 );

    return equation.str();
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_time_stamp_diff(
    const std::string& _ts1,
    const std::string& _ts2,
    const Float _diff,
    const bool _is_greater ) const
{
    constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
    constexpr Float seconds_per_hour = 60.0 * 60.0;
    constexpr Float seconds_per_minute = 60.0;

    const auto abs_diff = std::abs( _diff );

    auto diffstr = make_diffstr( _diff, "seconds" );

    if ( abs_diff >= seconds_per_day )
        {
            diffstr = make_diffstr( _diff / seconds_per_day, "days" );
        }
    else if ( abs_diff >= seconds_per_hour )
        {
            diffstr = make_diffstr( _diff / seconds_per_hour, "hours" );
        }
    else if ( abs_diff >= seconds_per_minute )
        {
            diffstr = make_diffstr( _diff / seconds_per_minute, "minutes" );
        }

    const auto comparison =
        _is_greater ? std::string( " > " ) : std::string( " <= " );

    const auto condition = "datetime( t1.\"" + _ts1 + "\" )" + comparison +
                           "datetime( t2.\"" + _ts2 + "\", " + diffstr + " )";

    if ( _is_greater )
        {
            return "( " + condition + " )";
        }

    return "( " + condition + " OR t1.\"" + _ts1 + "\" IS NULL OR t2.\"" +
           _ts2 + "\" IS NULL )";
}

// ----------------------------------------------------------------------------

std::vector<Float> ConditionMaker::rescale(
    const std::vector<Float>& _weights ) const
{
    assert_true(
        input_scaler().means().size() +
        input_scaler().inverse_stddev().size() );

    assert_true(
        output_scaler().means().size() +
        output_scaler().inverse_stddev().size() );

    assert_true(
        _weights.size() ==
        input_scaler().means().size() + output_scaler().means().size() + 1 );

    std::vector<Float> rescaled_weights = _weights;

    size_t i = 1;

    for ( size_t j = 0; j < input_scaler().means().size(); ++i, ++j )
        {
            rescaled_weights.at( i ) *= input_scaler().inverse_stddev().at( j );

            rescaled_weights.at( 0 ) -= input_scaler().means().at( j ) *
                                        input_scaler().inverse_stddev().at( j );
        }

    for ( size_t j = 0; j < output_scaler().means().size(); ++i, ++j )
        {
            rescaled_weights.at( i ) *=
                output_scaler().inverse_stddev().at( j );

            rescaled_weights.at( 0 ) -=
                output_scaler().means().at( j ) *
                output_scaler().inverse_stddev().at( j );
        }

    return rescaled_weights;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relcit
