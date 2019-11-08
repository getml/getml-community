#include "relboost/utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_greater(
    const containers::Schema& _input,
    const containers::Schema& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const std::string condition =
                        "( t2." + _input.categorical_name( _split.column_ ) +
                        " NOT IN " + list_categories( _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const std::string condition =
                        "( t1." + _output.categorical_name( _split.column_ ) +
                        " NOT IN " + list_categories( _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2." + _input.discrete_name( _split.column_ ) +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2." + _input.discrete_name( _split.column_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::discrete_output:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::discrete_output_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::numerical_input:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2." + _input.numerical_name( _split.column_ ) +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2." + _input.numerical_name( _split.column_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::numerical_output:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::numerical_output_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::same_units_categorical:
                assert_true( _split.column_ < _output.num_categoricals() );
                assert_true( _split.column_input_ < _input.num_categoricals() );
                return "( t1." + _output.categorical_name( _split.column_ ) +
                       " = t2." +
                       _input.categorical_name( _split.column_input_ ) + " )";

            case enums::DataUsed::same_units_discrete:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " - t2." + _input.discrete_name( _split.column_input_ ) +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " IS NOT NULL AND t2." +
                       _input.discrete_name( _split.column_input_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::same_units_numerical:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " - t2." +
                       _input.numerical_name( _split.column_input_ ) + " > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " IS NOT NULL AND t2." +
                       _input.numerical_name( _split.column_input_ ) +
                       " IS NOT NULL )";

            case enums::DataUsed::subfeatures:
                return "( t2.feature_" +
                       std::to_string( peripheral_used_ + 1 ) + "_" +
                       std::to_string( _split.column_ + 1 ) + " > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1." + _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() + " > " +
                       std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::time_stamps_window:
                return "( t1." + _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() + " > " +
                       std::to_string( _split.critical_value_ ) + " AND t1." +
                       _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() + " <= " +
                       std::to_string( _split.critical_value_ + lag_ ) + " )";

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_smaller(
    const containers::Schema& _input,
    const containers::Schema& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const std::string condition =
                        "( t2." + _input.categorical_name( _split.column_ ) +
                        " IN " + list_categories( _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const std::string condition =
                        "( t1." + _output.categorical_name( _split.column_ ) +
                        " IN " + list_categories( _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2." + _input.discrete_name( _split.column_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2." + _input.discrete_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::discrete_input_is_nan:
                assert_true( _split.column_ < _input.num_discretes() );
                return "( t2." + _input.discrete_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::discrete_output:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.discrete_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::discrete_output_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::numerical_input:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2." + _input.numerical_name( _split.column_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2." + _input.numerical_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::numerical_input_is_nan:
                assert_true( _split.column_ < _input.num_numericals() );
                return "( t2." + _input.numerical_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::numerical_output:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.numerical_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::numerical_output_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " IS NULL )";

            case enums::DataUsed::same_units_categorical:
                assert_true( _split.column_ < _output.num_categoricals() );
                assert_true( _split.column_input_ < _input.num_categoricals() );
                return "( t1." + _output.categorical_name( _split.column_ ) +
                       " != t2." +
                       _input.categorical_name( _split.column_input_ ) + " )";

            case enums::DataUsed::same_units_discrete:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " - t2." + _input.discrete_name( _split.column_input_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.discrete_name( _split.column_ ) +
                       " IS NULL OR t2." +
                       _input.discrete_name( _split.column_input_ ) +
                       " IS NULL )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert_true( _split.column_ < _output.num_discretes() );
                assert_true( _split.column_input_ < _input.num_discretes() );
                return "( t1." + _output.discrete_name( _split.column_ ) +
                       " IS NULL OR t2." +
                       _input.discrete_name( _split.column_input_ ) +
                       " IS NULL )";

            case enums::DataUsed::same_units_numerical:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " - t2." +
                       _input.numerical_name( _split.column_input_ ) +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.numerical_name( _split.column_ ) +
                       " IS NULL OR t2." +
                       _input.numerical_name( _split.column_input_ ) +
                       " IS NULL )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert_true( _split.column_ < _output.num_numericals() );
                assert_true( _split.column_input_ < _input.num_numericals() );
                return "( t1." + _output.numerical_name( _split.column_ ) +
                       " IS NULL OR t2." +
                       _input.numerical_name( _split.column_input_ ) +
                       " IS NULL )";

            case enums::DataUsed::subfeatures:
                return "( t2.feature_" +
                       std::to_string( peripheral_used_ + 1 ) + "_" +
                       std::to_string( _split.column_ + 1 ) +
                       " <= " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1." + _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.time_stamps_name() +
                       " IS NULL OR t2." + _input.time_stamps_name() +
                       " IS NULL )";

            case enums::DataUsed::time_stamps_window:
                return "( t1." + _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.time_stamps_name() + " - t2." +
                       _input.time_stamps_name() + " > " +
                       std::to_string( _split.critical_value_ + lag_ ) +
                       " OR t1." + _output.time_stamps_name() +
                       " IS NULL OR t2." + _input.time_stamps_name() +
                       " IS NULL )";

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::list_categories(
    const containers::Split& _split ) const
{
    std::string categories = "( ";

    assert_true( _split.categories_used_begin_ <= _split.categories_used_end_ );

    for ( auto it = _split.categories_used_begin_;
          it != _split.categories_used_end_;
          ++it )
        {
            categories += "'" + encoding( *it ).str() + "'";

            if ( std::next( it, 1 ) != _split.categories_used_end_ )
                {
                    categories += ", ";
                }
        }

    categories += " )";

    return categories;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
