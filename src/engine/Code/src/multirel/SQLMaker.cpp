#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string SQLMaker::condition_greater(
    const std::vector<strings::String>& _categories,
    const VocabForDf& _vocab_popul,
    const VocabForDf& _vocab_perip,
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const descriptors::Split& _split,
    const bool _add_null ) const
{
    switch ( _split.data_used )
        {
            case enums::DataUsed::x_perip_categorical:
            case enums::DataUsed::x_popul_categorical:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    return "( " + name + " NOT IN " +
                           list_categories( _categories, _split ) + " )";
                }

            case enums::DataUsed::x_perip_discrete:
            case enums::DataUsed::x_popul_discrete:
            case enums::DataUsed::x_perip_numerical:
            case enums::DataUsed::x_popul_numerical:
            case enums::DataUsed::x_subfeature:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    const auto null_condition =
                        _add_null ? " OR " + name + " IS NULL "
                                  : std::string( "" );

                    return "( " + name + " > " +
                           std::to_string( _split.critical_value ) +
                           null_condition + " )";
                }

            case enums::DataUsed::same_unit_categorical:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_categorical_,
                        _split.column_used );

                    return "( " + name1 + " != " + name2 + " )";
                }

            case enums::DataUsed::same_unit_discrete:
            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 + " > " +
                           std::to_string( _split.critical_value ) + " )";
                }

            case enums::DataUsed::same_unit_numerical:
            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 + " > " +
                           std::to_string( _split.critical_value ) + " )";
                }

            case enums::DataUsed::x_popul_text:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    assert_true( _split.column_used < _vocab_popul.size() );
                    assert_true( _vocab_popul.at( _split.column_used ) );

                    return list_words(
                        *_vocab_popul.at( _split.column_used ),
                        _split,
                        name,
                        true );
                }

            case enums::DataUsed::x_perip_text:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    assert_true( _split.column_used < _vocab_perip.size() );
                    assert_true( _vocab_perip.at( _split.column_used ) );

                    return list_words(
                        *_vocab_perip.at( _split.column_used ),
                        _split,
                        name,
                        true );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    return make_time_stamp_window(
                        _input, _output, _split.critical_value, true );
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::condition_smaller(
    const std::vector<strings::String>& _categories,
    const VocabForDf& _vocab_popul,
    const VocabForDf& _vocab_perip,
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const descriptors::Split& _split,
    const bool _add_null ) const
{
    switch ( _split.data_used )
        {
            case enums::DataUsed::x_perip_categorical:
            case enums::DataUsed::x_popul_categorical:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    return "( " + name + " IN " +
                           list_categories( _categories, _split ) + " )";
                }

            case enums::DataUsed::x_perip_discrete:
            case enums::DataUsed::x_popul_discrete:
            case enums::DataUsed::x_perip_numerical:
            case enums::DataUsed::x_popul_numerical:
            case enums::DataUsed::x_subfeature:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    const auto null_condition =
                        _add_null ? " OR " + name + " IS NULL "
                                  : std::string( "" );

                    return "( " + name +
                           " <= " + std::to_string( _split.critical_value ) +
                           null_condition + " )";
                }

            case enums::DataUsed::same_unit_categorical:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_categorical_,
                        _split.column_used );

                    return "( " + name1 + " = " + name2 + " )";
                }

            case enums::DataUsed::same_unit_discrete:
            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 +
                           " <= " + std::to_string( _split.critical_value ) +
                           " )";
                }

            case enums::DataUsed::same_unit_numerical:
            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 +
                           " <= " + std::to_string( _split.critical_value ) +
                           " )";
                }

            case enums::DataUsed::x_popul_text:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    assert_true( _split.column_used < _vocab_popul.size() );
                    assert_true( _vocab_popul.at( _split.column_used ) );

                    return list_words(
                        *_vocab_popul.at( _split.column_used ),
                        _split,
                        name,
                        false );
                }

            case enums::DataUsed::x_perip_text:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _split.column_used,
                        _split.data_used );

                    assert_true( _split.column_used < _vocab_perip.size() );
                    assert_true( _vocab_perip.at( _split.column_used ) );

                    return list_words(
                        *_vocab_perip.at( _split.column_used ),
                        _split,
                        name,
                        false );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    return make_time_stamp_window(
                        _input, _output, _split.critical_value, false );
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

std::string SQLMaker::get_name(
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used ) const
{
    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_categorical:
                assert_true( _column_used < _input.num_categoricals() );
                return make_colname(
                    _input.categorical_name( _column_used ), "t2" );

            case enums::DataUsed::x_popul_categorical:
                assert_true( _column_used < _output.num_categoricals() );
                return make_colname(
                    _output.categorical_name( _column_used ), "t1" );

            case enums::DataUsed::x_perip_discrete:
                assert_true( _column_used < _input.num_discretes() );
                return make_colname(
                    _input.discrete_name( _column_used ), "t2" );

            case enums::DataUsed::x_popul_discrete:
                assert_true( _column_used < _output.num_discretes() );
                return make_colname(
                    _output.discrete_name( _column_used ), "t1" );

            case enums::DataUsed::x_perip_numerical:
                assert_true( _column_used < _input.num_numericals() );
                return make_colname(
                    _input.numerical_name( _column_used ), "t2" );

            case enums::DataUsed::x_popul_numerical:
                assert_true( _column_used < _output.num_numericals() );
                return make_colname(
                    _output.numerical_name( _column_used ), "t1" );

            case enums::DataUsed::x_perip_text:
                assert_true( _column_used < _input.num_text() );
                return make_colname( _input.text_name( _column_used ), "t2" );

            case enums::DataUsed::x_popul_text:
                assert_true( _column_used < _output.num_text() );
                return make_colname( _output.text_name( _column_used ), "t1" );

            case enums::DataUsed::x_subfeature:
                {
                    const auto number =
                        helpers::SQLGenerator::make_subfeature_identifier(
                            _feature_prefix, peripheral_used_ );

                    return "f_" + number + ".\"feature_" + number + "_" +
                           std::to_string( _column_used + 1 ) + "\"";
                }

            default:
                assert_true( false && "Unknown DataUsed!" );
        }

    return "";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::get_ts_name(
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used,
    const std::string& _diffstr ) const
{
    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_discrete:
                assert_true( _column_used < _input.num_discretes() );
                return helpers::SQLGenerator::make_relative_time(
                    _input.discrete_name( _column_used ) + _diffstr, "t2" );

            case enums::DataUsed::x_popul_discrete:
                assert_true( _column_used < _output.num_discretes() );
                return helpers::SQLGenerator::make_relative_time(
                    _output.discrete_name( _column_used ) + _diffstr, "t1" );

            case enums::DataUsed::x_perip_numerical:
                assert_true( _column_used < _input.num_numericals() );
                return helpers::SQLGenerator::make_relative_time(
                    _input.numerical_name( _column_used ) + _diffstr, "t2" );

            case enums::DataUsed::x_popul_numerical:
                assert_true( _column_used < _output.num_numericals() );
                return helpers::SQLGenerator::make_relative_time(
                    _output.numerical_name( _column_used ) + _diffstr, "t1" );

            default:
                assert_true( false && "Unknown DataUsed!" );
        }

    return "";
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> SQLMaker::get_names(
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
    const size_t _column_used ) const
{
    assert_true( _same_units );

    assert_true( _column_used < _same_units->size() );

    const auto same_unit = _same_units->at( _column_used );

    const auto name1 = get_name(
        _feature_prefix,
        _input,
        _output,
        std::get<0>( same_unit ).ix_column_used,
        std::get<0>( same_unit ).data_used );

    const auto name2 = get_name(
        _feature_prefix,
        _input,
        _output,
        std::get<1>( same_unit ).ix_column_used,
        std::get<1>( same_unit ).data_used );

    return std::make_pair( name1, name2 );
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> SQLMaker::get_ts_names(
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
    const size_t _column_used ) const
{
    assert_true( _same_units );

    assert_true( _column_used < _same_units->size() );

    const auto same_unit = _same_units->at( _column_used );

    const auto name1 = get_ts_name(
        _input,
        _output,
        std::get<0>( same_unit ).ix_column_used,
        std::get<0>( same_unit ).data_used,
        "" );

    const auto name2 = get_ts_name(
        _input,
        _output,
        std::get<1>( same_unit ).ix_column_used,
        std::get<1>( same_unit ).data_used,
        "" );

    return std::make_pair( name1, name2 );
}

// ----------------------------------------------------------------------------

std::string SQLMaker::list_categories(
    const std::vector<strings::String>& _categories,
    const descriptors::Split& _split ) const
{
    std::string categories = "( ";

    assert_true( _split.categories_used_begin <= _split.categories_used_end );

    for ( auto it = _split.categories_used_begin;
          it != _split.categories_used_end;
          ++it )
        {
            assert_true( *it < _categories.size() );

            categories += "'" + _categories.at( *it ).str() + "'";

            if ( std::next( it, 1 ) != _split.categories_used_end )
                {
                    categories += ", ";
                }
        }

    categories += " )";

    return categories;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::list_words(
    const std::vector<strings::String>& _vocabulary,
    const descriptors::Split& _split,
    const std::string& _name,
    const bool _is_greater ) const
{
    std::stringstream words;

    words << "( ";

    assert_true( _split.categories_used_begin <= _split.categories_used_end );

    const std::string and_or_or = _is_greater ? " AND " : " OR ";

    const std::string comparison = _is_greater ? " == 0 " : " > 0 ";

    for ( auto it = _split.categories_used_begin;
          it != _split.categories_used_end;
          ++it )
        {
            assert_true( *it < _vocabulary.size() );

            words << "( contains( " + _name + ", '" +
                         _vocabulary.at( *it ).str() + "' )";

            words << comparison << ")";

            if ( std::next( it, 1 ) != _split.categories_used_end )
                {
                    words << and_or_or;
                }
        }

    words << " )";

    return words.str();
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_colname(
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

std::string SQLMaker::make_time_stamp_diff(
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
    const size_t _column_used,
    const Float _diff,
    const bool _is_greater ) const
{
    const auto diffstr =
        helpers::SQLGenerator::make_time_stamp_diff( _diff, false );

    const auto same_unit = _same_units->at( _column_used );

    const auto colname1 = get_ts_name(
        _input,
        _output,
        std::get<0>( same_unit ).ix_column_used,
        std::get<0>( same_unit ).data_used,
        diffstr );

    const auto colname2 = get_ts_name(
        _input,
        _output,
        std::get<1>( same_unit ).ix_column_used,
        std::get<1>( same_unit ).data_used,
        "" );

    return make_time_stamp_diff( colname2, colname1, _is_greater );
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_time_stamp_diff(
    const std::string& _colname1,
    const std::string& _colname2,
    const bool _is_greater ) const
{
    const auto comparison =
        _is_greater ? std::string( " > " ) : std::string( " <= " );

    const auto condition = _colname1 + comparison + _colname2;

    return "( " + condition + " )";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_time_stamp_window(
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const Float _diff,
    const bool _is_greater ) const
{
    const auto name1 = _output.time_stamps_name();

    const auto name2 = _input.time_stamps_name();

    const bool is_rowid =
        ( name1.find( helpers::Macros::rowid() ) != std::string::npos );

    const auto diffstr1 =
        helpers::SQLGenerator::make_time_stamp_diff( _diff - lag_, is_rowid );

    const auto diffstr2 =
        helpers::SQLGenerator::make_time_stamp_diff( _diff, is_rowid );

    const auto condition1 = make_time_stamp_diff(
        helpers::SQLGenerator::make_relative_time( name1, "t1" ),
        helpers::SQLGenerator::make_relative_time( name2 + diffstr1, "t2" ),
        !_is_greater );

    const auto condition2 = make_time_stamp_diff(
        helpers::SQLGenerator::make_relative_time( name1, "t1" ),
        helpers::SQLGenerator::make_relative_time( name2 + diffstr2, "t2" ),
        _is_greater );

    if ( _is_greater )
        {
            return "( " + condition1 + " OR " + condition2 + " )";
        }

    return "( " + condition1 + " AND " + condition2 + " )";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_statement(
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used,
    const std::string& _agg_type ) const
{
    const auto value = value_to_be_aggregated(
        _feature_prefix, _input, _output, _column_used, _data_used );

    std::string select;

    if ( _agg_type == "COUNT DISTINCT" )
        {
            select += "COUNT( DISTINCT ";
        }
    else if ( _agg_type == "COUNT MINUS COUNT DISTINCT" )
        {
            select += "COUNT( " + value + " ) - COUNT( DISTINCT ";
        }
    else
        {
            select += _agg_type;

            select += "( ";
        }

    select += value;

    if ( _agg_type == "FIRST" || _agg_type == "LAST" )
        {
            select += ", " + helpers::SQLGenerator::make_epoch_time(
                                 _input.time_stamps_name(), "t2" );
        }

    select += " )";

    return select;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::value_to_be_aggregated(
    const std::string& _feature_prefix,
    const helpers::Schema& _input,
    const helpers::Schema& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used ) const
{
    switch ( _data_used )
        {
            case enums::DataUsed::not_applicable:
                return "*";

            case enums::DataUsed::x_perip_categorical:
            case enums::DataUsed::x_perip_discrete:
            case enums::DataUsed::x_perip_numerical:
            case enums::DataUsed::x_subfeature:
                {
                    return get_name(
                        _feature_prefix,
                        _input,
                        _output,
                        _column_used,
                        _data_used );
                }

            case enums::DataUsed::same_unit_discrete:
            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _column_used );

                    return name2 + " - " + name1;
                }

            case enums::DataUsed::same_unit_numerical:
            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _feature_prefix,
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _column_used );

                    return name2 + " - " + name1;
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel
