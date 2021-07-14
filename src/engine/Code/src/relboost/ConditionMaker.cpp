
#include "relboost/utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_greater(
    const std::vector<strings::String>& _categories,
    const VocabForDf& _vocab_popul,
    const VocabForDf& _vocab_perip,
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const auto colname = make_colname(
                        _input.categorical_name( _split.column_ ), "t2" );

                    const std::string condition =
                        "( " + colname + " IN " +
                        list_categories( _categories, _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const auto colname = make_colname(
                        _output.categorical_name( _split.column_ ), "t1" );

                    const std::string condition =
                        "( " + colname + " IN " +
                        list_categories( _categories, _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                {
                    assert_true( _split.column_ < _input.num_discretes() );

                    const auto colname = make_colname(
                        _input.discrete_name( _split.column_ ), "t2" );

                    return "( " + colname + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::discrete_input_is_nan:
                {
                    assert_true( _split.column_ < _input.num_discretes() );

                    const auto colname = make_colname(
                        _input.discrete_name( _split.column_ ), "t2" );

                    return "( " + colname + " IS NOT NULL )";
                }

            case enums::DataUsed::discrete_output:
                {
                    assert_true( _split.column_ < _output.num_discretes() );

                    const auto colname = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    return "( " + colname + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::discrete_output_is_nan:
                {
                    assert_true( _split.column_ < _output.num_discretes() );

                    const auto colname = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    return "( " + colname + " IS NOT NULL )";
                }

            case enums::DataUsed::numerical_input:
                {
                    assert_true( _split.column_ < _input.num_numericals() );

                    const auto colname = make_colname(
                        _input.numerical_name( _split.column_ ), "t2" );

                    return "( " + colname + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::numerical_input_is_nan:
                {
                    assert_true( _split.column_ < _input.num_numericals() );

                    const auto colname = make_colname(
                        _input.numerical_name( _split.column_ ), "t2" );

                    return "( " + colname + " IS NOT NULL )";
                }

            case enums::DataUsed::numerical_output:
                {
                    assert_true( _split.column_ < _output.num_numericals() );

                    const auto colname = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    return "( " + colname + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::numerical_output_is_nan:
                {
                    assert_true( _split.column_ < _output.num_numericals() );

                    const auto colname = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    return "( " + colname + " IS NOT NULL )";
                }

            case enums::DataUsed::same_units_categorical:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );
                    assert_true(
                        _split.column_input_ < _input.num_categoricals() );

                    const auto colname1 = make_colname(
                        _output.categorical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.categorical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " = " + colname2 + " )";
                }

            case enums::DataUsed::same_units_discrete:
            case enums::DataUsed::same_units_discrete_ts:
                {
                    assert_true( _split.column_ < _output.num_discretes() );
                    assert_true(
                        _split.column_input_ < _input.num_discretes() );

                    const auto colname1 = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.discrete_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " - " + colname2 + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::same_units_discrete_is_nan:
                {
                    assert_true( _split.column_ < _output.num_discretes() );
                    assert_true(
                        _split.column_input_ < _input.num_discretes() );

                    const auto colname1 = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.discrete_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " IS NOT NULL AND " + colname2 +
                           " IS NOT NULL )";
                }

            case enums::DataUsed::same_units_numerical:
            case enums::DataUsed::same_units_numerical_ts:
                {
                    assert_true( _split.column_ < _output.num_numericals() );
                    assert_true(
                        _split.column_input_ < _input.num_numericals() );

                    const auto colname1 = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.numerical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " - " + colname2 + " > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::same_units_numerical_is_nan:
                {
                    assert_true( _split.column_ < _output.num_numericals() );
                    assert_true(
                        _split.column_input_ < _input.num_numericals() );

                    const auto colname1 = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.numerical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " IS NOT NULL AND " + colname2 +
                           " IS NOT NULL )";
                }

            case enums::DataUsed::subfeatures:
                {
                    const auto number =
                        helpers::SQLGenerator::make_subfeature_identifier(
                            _feature_prefix, peripheral_used_ );

                    return "( f_" + number + ".\"feature_" + number + "_" +
                           std::to_string( _split.column_ + 1 ) + "\" > " +
                           std::to_string( _split.critical_value_ ) + " )";
                }

            case enums::DataUsed::text_input:
                {
                    assert_true( _vocab_perip.size() == _input.num_text() );
                    assert_true( _split.column_ < _input.num_text() );
                    assert_true( _vocab_perip.at( _split.column_ ) );

                    const auto colname = make_colname(
                        _input.text_name( _split.column_ ), "t2" );

                    return list_words(
                        *_vocab_perip.at( _split.column_ ),
                        _split,
                        colname,
                        true );
                }

            case enums::DataUsed::text_output:
                {
                    assert_msg(
                        _vocab_popul.size() == _output.num_text(),
                        "_vocab_popul.size(): " +
                            std::to_string( _vocab_popul.size() ) +
                            ", _output.num_text(): " +
                            std::to_string( _output.num_text() ) );

                    assert_true( _split.column_ < _output.num_text() );
                    assert_true( _vocab_popul.at( _split.column_ ) );

                    const auto colname = make_colname(
                        _output.text_name( _split.column_ ), "t1" );

                    return list_words(
                        *_vocab_popul.at( _split.column_ ),
                        _split,
                        colname,
                        true );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    return make_time_stamp_window(
                        _input, _output, _split.critical_value_, true );
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::condition_smaller(
    const std::vector<strings::String>& _categories,
    const VocabForDf& _vocab_popul,
    const VocabForDf& _vocab_perip,
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const containers::Split& _split ) const
{
    switch ( _split.data_used_ )
        {
            case enums::DataUsed::categorical_input:
                {
                    assert_true( _split.column_ < _input.num_categoricals() );

                    const auto colname = make_colname(
                        _input.categorical_name( _split.column_ ), "t2" );

                    const std::string condition =
                        "( " + colname + " NOT IN " +
                        list_categories( _categories, _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::categorical_output:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );

                    const auto colname = make_colname(
                        _output.categorical_name( _split.column_ ), "t1" );

                    const std::string condition =
                        "( " + colname + " NOT IN " +
                        list_categories( _categories, _split ) + " )";

                    return condition;
                }

            case enums::DataUsed::discrete_input:
                {
                    assert_true( _split.column_ < _input.num_discretes() );

                    const auto colname = make_colname(
                        _input.discrete_name( _split.column_ ), "t2" );

                    return "( " + colname +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname + " IS NULL )";
                }

            case enums::DataUsed::discrete_input_is_nan:
                {
                    assert_true( _split.column_ < _input.num_discretes() );

                    const auto colname = make_colname(
                        _input.discrete_name( _split.column_ ), "t2" );

                    return "( " + colname + " IS NULL )";
                }

            case enums::DataUsed::discrete_output:
                {
                    assert_true( _split.column_ < _output.num_discretes() );

                    const auto colname = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    return "( " + colname +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname + " IS NULL )";
                }

            case enums::DataUsed::discrete_output_is_nan:
                {
                    assert_true( _split.column_ < _output.num_discretes() );

                    const auto colname = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    return "( " + colname + " IS NULL )";
                }

            case enums::DataUsed::numerical_input:
                {
                    assert_true( _split.column_ < _input.num_numericals() );

                    const auto colname = make_colname(
                        _input.numerical_name( _split.column_ ), "t2" );

                    return "( " + colname +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname + " IS NULL )";
                }

            case enums::DataUsed::numerical_input_is_nan:
                {
                    assert_true( _split.column_ < _input.num_numericals() );

                    const auto colname = make_colname(
                        _input.numerical_name( _split.column_ ), "t2" );

                    return "( " + colname + " IS NULL )";
                }

            case enums::DataUsed::numerical_output:
                {
                    assert_true( _split.column_ < _output.num_numericals() );

                    const auto colname = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    return "( " + colname +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname + " IS NULL )";
                }

            case enums::DataUsed::numerical_output_is_nan:
                {
                    assert_true( _split.column_ < _output.num_numericals() );

                    const auto colname = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    return "( " + colname + " IS NOT NULL )";
                }

            case enums::DataUsed::same_units_categorical:
                {
                    assert_true( _split.column_ < _output.num_categoricals() );
                    assert_true(
                        _split.column_input_ < _input.num_categoricals() );

                    const auto colname1 = make_colname(
                        _output.categorical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.categorical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " != " + colname2 + " )";
                }

            case enums::DataUsed::same_units_discrete:
            case enums::DataUsed::same_units_discrete_ts:
                {
                    assert_true( _split.column_ < _output.num_discretes() );
                    assert_true(
                        _split.column_input_ < _input.num_discretes() );

                    const auto colname1 = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.discrete_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " - " + colname2 +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname1 + " IS NULL OR " + colname2 +
                           " IS NULL )";
                }

            case enums::DataUsed::same_units_discrete_is_nan:
                {
                    assert_true( _split.column_ < _output.num_discretes() );
                    assert_true(
                        _split.column_input_ < _input.num_discretes() );

                    const auto colname1 = make_colname(
                        _output.discrete_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.discrete_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " IS NULL OR " + colname2 +
                           " IS NULL )";
                }

            case enums::DataUsed::same_units_numerical:
            case enums::DataUsed::same_units_numerical_ts:
                {
                    assert_true( _split.column_ < _output.num_numericals() );
                    assert_true(
                        _split.column_input_ < _input.num_numericals() );

                    const auto colname1 = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.numerical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " - " + colname2 +
                           " <= " + std::to_string( _split.critical_value_ ) +
                           " OR " + colname1 + " IS NULL OR " + colname2 +
                           " IS NULL )";
                }

            case enums::DataUsed::same_units_numerical_is_nan:
                {
                    assert_true( _split.column_ < _output.num_numericals() );
                    assert_true(
                        _split.column_input_ < _input.num_numericals() );

                    const auto colname1 = make_colname(
                        _output.numerical_name( _split.column_ ), "t1" );

                    const auto colname2 = make_colname(
                        _input.numerical_name( _split.column_input_ ), "t2" );

                    return "( " + colname1 + " IS NULL OR " + colname2 +
                           " IS NULL )";
                }

            case enums::DataUsed::subfeatures:
                {
                    const auto number =
                        helpers::SQLGenerator::make_subfeature_identifier(
                            _feature_prefix, peripheral_used_ );

                    return "( f_" + number + ".\"feature_" + number + "_" +
                           std::to_string( _split.column_ + 1 ) +
                           "\" <= " + std::to_string( _split.critical_value_ ) +
                           " )";
                }

            case enums::DataUsed::text_input:
                {
                    assert_true( _vocab_perip.size() == _input.num_text() );
                    assert_true( _split.column_ < _input.num_text() );
                    assert_true( _vocab_perip.at( _split.column_ ) );

                    const auto colname = make_colname(
                        _input.text_name( _split.column_ ), "t2" );

                    return list_words(
                        *_vocab_perip.at( _split.column_ ),
                        _split,
                        colname,
                        false );
                }

            case enums::DataUsed::text_output:
                {
                    assert_true( _vocab_popul.size() == _output.num_text() );
                    assert_true( _split.column_ < _output.num_text() );
                    assert_true( _vocab_popul.at( _split.column_ ) );

                    const auto colname = make_colname(
                        _output.text_name( _split.column_ ), "t1" );

                    return list_words(
                        *_vocab_popul.at( _split.column_ ),
                        _split,
                        colname,
                        false );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    return make_time_stamp_window(
                        _input, _output, _split.critical_value_, false );
                }

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
            assert_msg(
                *it < _categories.size(),
                "*it: " + std::to_string( *it ) + ", _categories.size(): " +
                    std::to_string( _categories.size() ) );

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

std::string ConditionMaker::list_words(
    const std::vector<strings::String>& _vocabulary,
    const containers::Split& _split,
    const std::string& _name,
    const bool _is_greater ) const
{
    std::stringstream words;

    words << "( ";

    assert_true( _split.categories_used_begin_ <= _split.categories_used_end_ );

    const std::string and_or_or = _is_greater ? " OR " : " AND ";

    const std::string comparison = _is_greater ? " > 0 " : " == 0 ";

    for ( auto it = _split.categories_used_begin_;
          it != _split.categories_used_end_;
          ++it )
        {
            assert_true( *it < _vocabulary.size() );

            words << "( contains( " + _name + ", '" +
                         _vocabulary.at( *it ).str() + "' )";

            words << comparison << ")";

            if ( std::next( it, 1 ) != _split.categories_used_end_ )
                {
                    words << and_or_or;
                }
        }

    words << " )";

    return words.str();
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_colname(
    const std::string& _colname, const std::string& _alias ) const
{
    if ( _colname.find( helpers::Macros::fast_prop_feature() ) !=
         std::string::npos )
        {
            const auto stripped = helpers::StringReplacer::replace_all(
                _colname, helpers::Macros::fast_prop_feature(), "" );

            const auto pos = stripped.rfind( "_" );

            assert_true( pos != std::string::npos );

            const auto alias = "p_" + stripped.substr( 0, pos );

            return alias + ".\"" +
                   helpers::StringReplacer::replace_all(
                       _colname,
                       helpers::Macros::fast_prop_feature(),
                       "feature_" ) +
                   "\"";
        }

    return _alias + ".\"" + helpers::SQLGenerator::make_colname( _colname ) +
           "\"";
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_time_stamp_diff(
    const std::string& _ts1,
    const std::string& _ts2,
    const Float _diff,
    const bool _is_greater ) const
{
    const auto diffstr =
        helpers::SQLGenerator::make_time_stamp_diff( _diff, false );

    const auto colname1 =
        helpers::SQLGenerator::make_relative_time( _ts1, "t1" );

    const auto colname2 =
        helpers::SQLGenerator::make_relative_time( _ts2 + diffstr, "t2" );

    const auto condition =
        make_time_stamp_diff( colname1, colname2, _is_greater );

    if ( _is_greater )
        {
            return "( " + condition + " )";
        }

    return "( " + condition + " OR " + colname1 + " IS NULL OR " + colname2 +
           " IS NULL )";
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_time_stamp_diff(
    const std::string& _colname1,
    const std::string& _colname2,
    const bool _is_greater ) const
{
    const auto comparison =
        _is_greater ? std::string( " > " ) : std::string( " <= " );

    return _colname1 + comparison + _colname2;
}

// ----------------------------------------------------------------------------

std::string ConditionMaker::make_time_stamp_window(
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const Float _diff,
    const bool _is_greater ) const
{
    const auto colname1 = _output.time_stamps_name();

    const auto colname2 = _input.time_stamps_name();

    const bool is_rowid =
        ( colname1.find( helpers::Macros::rowid() ) != std::string::npos );

    const auto diffstr1 =
        helpers::SQLGenerator::make_time_stamp_diff( _diff, is_rowid );

    const auto diffstr2 =
        helpers::SQLGenerator::make_time_stamp_diff( _diff + lag_, is_rowid );

    const auto condition1 = make_time_stamp_diff(
        helpers::SQLGenerator::make_relative_time( colname1, "t1" ),
        helpers::SQLGenerator::make_relative_time( colname2 + diffstr1, "t2" ),
        _is_greater );

    const auto condition2 = make_time_stamp_diff(
        helpers::SQLGenerator::make_relative_time( colname1, "t1" ),
        helpers::SQLGenerator::make_relative_time( colname2 + diffstr2, "t2" ),
        !_is_greater );

    if ( _is_greater )
        {
            return "( " + condition1 + " AND " + condition2 + " )";
        }

    return "( " + condition1 + " OR " + condition2 + " OR " + colname1 +
           " IS NULL OR " + colname2 + " IS NULL )";
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
