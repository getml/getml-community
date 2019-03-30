#include "utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_greater(
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert(
                        _split.column_ < _input.categorical_.colnames_.size() );

                    std::string condition = "( ";

                    assert(
                        _split.categories_used_begin_ <=
                        _split.categories_used_end_ );

                    for ( auto it = _split.categories_used_begin_;
                          it != _split.categories_used_end_;
                          ++it )
                        {
                            if ( it != _split.categories_used_begin_ )
                                {
                                    condition += " OR ";
                                }

                            condition +=
                                "t2." +
                                _input.categorical_.colnames_[_split.column_] +
                                " = '" + encoding( *it ) + "'";
                        }

                    condition += " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert(
                        _split.column_ <
                        _output.categorical_.colnames_.size() );

                    std::string condition = "( ";

                    assert(
                        _split.categories_used_begin_ <=
                        _split.categories_used_end_ );

                    for ( auto it = _split.categories_used_begin_;
                          it != _split.categories_used_end_;
                          ++it )
                        {
                            if ( it != _split.categories_used_begin_ )
                                {
                                    condition += " OR ";
                                }

                            condition +=
                                "t1." +
                                _output.categorical_.colnames_[_split.column_] +
                                " = '" + encoding( *it ) + "'";
                        }

                    condition += " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert( _split.column_ < _input.discrete_.colnames_.size() );
                return "( t2." + _input.discrete_.colnames_[_split.column_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::discrete_input_is_nan:
                assert( _split.column_ < _input.discrete_.colnames_.size() );
                return "( t2." + _input.discrete_.colnames_[_split.column_] +
                       " IS NOT NULL )";

            case enums::DataUsed::discrete_output:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::discrete_output_is_nan:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NOT NULL )";

            case enums::DataUsed::numerical_input:
                assert( _split.column_ < _input.numerical_.colnames_.size() );
                return "( t2." + _input.numerical_.colnames_[_split.column_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::numerical_input_is_nan:
                assert( _split.column_ < _input.numerical_.colnames_.size() );
                return "( t2." + _input.numerical_.colnames_[_split.column_] +
                       " IS NOT NULL )";

            case enums::DataUsed::numerical_output:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::numerical_output_is_nan:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " IS NOT NULL )";

            case enums::DataUsed::same_units_categorical:
                assert(
                    _split.column_ < _output.categorical_.colnames_.size() );
                assert(
                    _split.column_input_ <
                    _input.categorical_.colnames_.size() );
                return "( t1." +
                       _output.categorical_.colnames_[_split.column_] +
                       " = t2." +
                       _input.categorical_.colnames_[_split.column_input_] +
                       " )";

            case enums::DataUsed::same_units_discrete:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " - t2." +
                       _input.discrete_.colnames_[_split.column_input_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NOT NULL AND t2." +
                       _input.discrete_.colnames_[_split.column_input_] +
                       " IS NOT NULL )";

            case enums::DataUsed::same_units_numerical:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " - t2." +
                       _input.numerical_.colnames_[_split.column_input_] +
                       " > " + std::to_string( _split.critical_value_ ) + " )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " IS NOT NULL AND t2." +
                       _input.numerical_.colnames_[_split.column_input_] +
                       " IS NOT NULL )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1." + _output.time_stamps_[0].colnames_[0] +
                       " - t2." + _input.time_stamps_[0].colnames_[0] + " > " +
                       std::to_string( _split.critical_value_ ) + " )";

            default:
                assert( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_smaller(
    const containers::DataFrame& _input,
    const containers::DataFrame& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert(
                        _split.column_ < _input.categorical_.colnames_.size() );

                    std::string condition = "( ";

                    assert(
                        _split.categories_used_begin_ <=
                        _split.categories_used_end_ );

                    for ( auto it = _split.categories_used_begin_;
                          it != _split.categories_used_end_;
                          ++it )
                        {
                            if ( it != _split.categories_used_begin_ )
                                {
                                    condition += " AND ";
                                }

                            condition +=
                                "t2." +
                                _input.categorical_.colnames_[_split.column_] +
                                " != '" + encoding( *it ) + "'";
                        }

                    condition += " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert(
                        _split.column_ <
                        _output.categorical_.colnames_.size() );

                    std::string condition = "( ";

                    assert(
                        _split.categories_used_begin_ <=
                        _split.categories_used_end_ );

                    for ( auto it = _split.categories_used_begin_;
                          it != _split.categories_used_end_;
                          ++it )
                        {
                            if ( it != _split.categories_used_begin_ )
                                {
                                    condition += " AND ";
                                }

                            condition +=
                                "t1." +
                                _output.categorical_.colnames_[_split.column_] +
                                " != '" + encoding( *it ) + "'";
                        }

                    condition += " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                assert( _split.column_ < _input.discrete_.colnames_.size() );
                return "( t2." + _input.discrete_.colnames_[_split.column_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2." + _input.discrete_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::discrete_input_is_nan:
                assert( _split.column_ < _input.discrete_.colnames_.size() );
                return "( t2." + _input.discrete_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::discrete_output:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::discrete_output_is_nan:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::numerical_input:
                assert( _split.column_ < _input.numerical_.colnames_.size() );
                return "( t2." + _input.numerical_.colnames_[_split.column_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t2." + _input.numerical_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::numerical_input_is_nan:
                assert( _split.column_ < _input.numerical_.colnames_.size() );
                return "( t2." + _input.numerical_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::numerical_output:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." +
                       _output.numerical_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::numerical_output_is_nan:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " IS NULL )";

            case enums::DataUsed::same_units_categorical:
                assert(
                    _split.column_ < _output.categorical_.colnames_.size() );
                assert(
                    _split.column_input_ <
                    _input.categorical_.colnames_.size() );
                return "( t1." +
                       _output.categorical_.colnames_[_split.column_] +
                       " != t2." +
                       _input.categorical_.colnames_[_split.column_input_] +
                       " )";

            case enums::DataUsed::same_units_discrete:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " - t2." +
                       _input.discrete_.colnames_[_split.column_input_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NULL OR t2." +
                       _input.discrete_.colnames_[_split.column_input_] +
                       " IS NULL )";

            case enums::DataUsed::same_units_discrete_is_nan:
                assert( _split.column_ < _output.discrete_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.discrete_.colnames_.size() );
                return "( t1." + _output.discrete_.colnames_[_split.column_] +
                       " IS NULL OR t2." +
                       _input.discrete_.colnames_[_split.column_input_] +
                       " IS NULL )";

            case enums::DataUsed::same_units_numerical:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " - t2." +
                       _input.numerical_.colnames_[_split.column_input_] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." +
                       _output.numerical_.colnames_[_split.column_] +
                       " IS NULL OR t2." +
                       _input.numerical_.colnames_[_split.column_input_] +
                       " IS NULL )";

            case enums::DataUsed::same_units_numerical_is_nan:
                assert( _split.column_ < _output.numerical_.colnames_.size() );
                assert(
                    _split.column_input_ < _input.numerical_.colnames_.size() );
                return "( t1." + _output.numerical_.colnames_[_split.column_] +
                       " IS NULL OR t2." +
                       _input.numerical_.colnames_[_split.column_input_] +
                       " IS NULL )";

            case enums::DataUsed::time_stamps_diff:
                return "( t1." + _output.time_stamps_[0].colnames_[0] +
                       " - t2." + _input.time_stamps_[0].colnames_[0] +
                       " <= " + std::to_string( _split.critical_value_ ) +
                       " OR t1." + _output.time_stamps_[0].colnames_[0] +
                       " IS NULL OR t2." + _input.time_stamps_[0].colnames_[0] +
                       " IS NULL )";

            default:
                assert( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
