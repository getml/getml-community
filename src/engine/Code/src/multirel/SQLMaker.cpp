#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string SQLMaker::condition_greater(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const descriptors::Split& _split ) const
{
    switch ( _split.data_used )
        {
            case enums::DataUsed::x_perip_categorical:
            case enums::DataUsed::x_popul_categorical:
                {
                    const auto name = get_name(
                        _input, _output, _split.column_used, _split.data_used );

                    return "( " + name + " NOT IN " +
                           list_categories( _split ) + " )";
                }

            case enums::DataUsed::x_perip_discrete:
            case enums::DataUsed::x_popul_discrete:
            case enums::DataUsed::x_perip_numerical:
            case enums::DataUsed::x_popul_numerical:
            case enums::DataUsed::x_subfeature:
                {
                    const auto name = get_name(
                        _input, _output, _split.column_used, _split.data_used );

                    return "( " + name + " > " +
                           std::to_string( _split.critical_value ) + " )";
                }

            case enums::DataUsed::same_unit_categorical:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_categorical_,
                        _split.column_used );

                    return "( " + name1 + " != " + name2 + " )";
                }

            case enums::DataUsed::same_unit_discrete:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 + " > " +
                           std::to_string( _split.critical_value ) + " )";
                }

            case enums::DataUsed::same_unit_numerical:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 + " > " +
                           std::to_string( _split.critical_value ) + " )";
                }

            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return make_time_stamp_diff(
                        name1, name2, _split.critical_value, true );
                }

            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return make_time_stamp_diff(
                        name1, name2, _split.critical_value, true );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    const auto name1 =
                        "t1.\"" + _output.time_stamps_name() + "\"";

                    const auto name2 =
                        "t2.\"" + _input.time_stamps_name() + "\"";

                    const auto condition1 = make_time_stamp_diff(
                        name1, name2, _split.critical_value - lag_, false );

                    const auto condition2 = make_time_stamp_diff(
                        name1, name2, _split.critical_value, true );

                    return "( " + condition1 + " OR " + condition2 + " )";
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::condition_smaller(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const descriptors::Split& _split ) const
{
    switch ( _split.data_used )
        {
            case enums::DataUsed::x_perip_categorical:
            case enums::DataUsed::x_popul_categorical:
                {
                    const auto name = get_name(
                        _input, _output, _split.column_used, _split.data_used );

                    return "( " + name + " IN " + list_categories( _split ) +
                           " )";
                }

            case enums::DataUsed::x_perip_discrete:
            case enums::DataUsed::x_popul_discrete:
            case enums::DataUsed::x_perip_numerical:
            case enums::DataUsed::x_popul_numerical:
            case enums::DataUsed::x_subfeature:
                {
                    const auto name = get_name(
                        _input, _output, _split.column_used, _split.data_used );

                    return "( " + name +
                           " <= " + std::to_string( _split.critical_value ) +
                           " )";
                }

            case enums::DataUsed::same_unit_categorical:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_categorical_,
                        _split.column_used );

                    return "( " + name1 + " == " + name2 + " )";
                }

            case enums::DataUsed::same_unit_discrete:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 +
                           " <= " + std::to_string( _split.critical_value ) +
                           " )";
                }

            case enums::DataUsed::same_unit_numerical:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return "( " + name2 + " - " + name1 +
                           " <= " + std::to_string( _split.critical_value ) +
                           " )";
                }

            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _split.column_used );

                    return make_time_stamp_diff(
                        name1, name2, _split.critical_value, false );
                }

            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _split.column_used );

                    return make_time_stamp_diff(
                        name1, name2, _split.critical_value, false );
                }

            case enums::DataUsed::time_stamps_window:
                {
                    const auto name1 =
                        "t1.\"" + _output.time_stamps_name() + "\"";

                    const auto name2 =
                        "t2.\"" + _input.time_stamps_name() + "\"";

                    const auto condition1 = make_time_stamp_diff(
                        name1, name2, _split.critical_value - lag_, true );

                    const auto condition2 = make_time_stamp_diff(
                        name1, name2, _split.critical_value, false );

                    return "( " + condition1 + " AND " + condition2 + " )";
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::get_name(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used ) const
{
    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_categorical:
                assert_true( _column_used < _input.num_categoricals() );
                return "t2.\"" + _input.categorical_name( _column_used ) + "\"";

            case enums::DataUsed::x_popul_categorical:
                assert_true( _column_used < _output.num_categoricals() );
                return "t1.\"" + _output.categorical_name( _column_used ) +
                       "\"";

            case enums::DataUsed::x_perip_discrete:
                assert_true( _column_used < _input.num_discretes() );
                return "t2.\"" + _input.discrete_name( _column_used ) + "\"";

            case enums::DataUsed::x_popul_discrete:
                assert_true( _column_used < _output.num_discretes() );
                return "t1.\"" + _output.discrete_name( _column_used ) + "\"";

            case enums::DataUsed::x_perip_numerical:
                assert_true( _column_used < _input.num_numericals() );
                return "t2.\"" + _input.numerical_name( _column_used ) + "\"";

            case enums::DataUsed::x_popul_numerical:
                assert_true( _column_used < _output.num_numericals() );
                return "t1.\"" + _output.numerical_name( _column_used ) + "\"";

            case enums::DataUsed::x_subfeature:
                return "t2.\"feature_" +
                       std::to_string( peripheral_used_ + 1 ) + "_" +
                       std::to_string( _column_used + 1 ) + "\"";

            default:
                assert_true( false && "Unknown DataUsed!" );
        }

    return "";
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> SQLMaker::get_names(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
    const size_t _column_used ) const
{
    assert_true( _same_units );

    assert_true( _column_used < _same_units->size() );

    const auto same_unit = _same_units->at( _column_used );

    const auto name1 = get_name(
        _input,
        _output,
        std::get<0>( same_unit ).ix_column_used,
        std::get<0>( same_unit ).data_used );

    const auto name2 = get_name(
        _input,
        _output,
        std::get<1>( same_unit ).ix_column_used,
        std::get<1>( same_unit ).data_used );

    return std::make_pair( name1, name2 );
}

// ----------------------------------------------------------------------------

std::string SQLMaker::list_categories( const descriptors::Split& _split ) const
{
    std::string categories = "( ";

    assert_true( _split.categories_used_begin <= _split.categories_used_end );

    for ( auto it = _split.categories_used_begin;
          it != _split.categories_used_end;
          ++it )
        {
            categories += "'" + encoding( *it ).str() + "'";

            if ( std::next( it, 1 ) != _split.categories_used_end )
                {
                    categories += ", ";
                }
        }

    categories += " )";

    return categories;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_time_stamp_diff(
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

    const auto condition = "datetime( " + _ts2 + " )" + comparison +
                           "datetime( " + _ts1 + ", " + diffstr + " )";

    return "( " + condition + " )";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_statement(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
    const size_t _column_used,
    const enums::DataUsed& _data_used,
    const std::string& _agg_type ) const
{
    std::string select;

    if ( _agg_type == "COUNT DISTINCT" )
        {
            select += "COUNT( DISTINCT ";
        }
    else if ( _agg_type == "COUNT MINUS COUNT DISTINCT" )
        {
            select += "COUNT( * ) - COUNT( DISTINCT ";
        }
    else
        {
            select += _agg_type;

            select += "( ";
        }

    select +=
        value_to_be_aggregated( _input, _output, _column_used, _data_used );

    select += " )";

    return select;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::value_to_be_aggregated(
    const containers::Placeholder& _input,
    const containers::Placeholder& _output,
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
                        _input, _output, _column_used, _data_used );
                }

            case enums::DataUsed::same_unit_discrete:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _column_used );

                    return name2 + " - " + name1;
                }

            case enums::DataUsed::same_unit_numerical:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _column_used );

                    return name2 + " - " + name1;
                }

            case enums::DataUsed::same_unit_discrete_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_discrete_,
                        _column_used );

                    return "( julianday( " + name2 + " ) - julianday( " +
                           name1 + " ) ) * 86400.0";
                }

            case enums::DataUsed::same_unit_numerical_ts:
                {
                    const auto [name1, name2] = get_names(
                        _input,
                        _output,
                        same_units_.same_units_numerical_,
                        _column_used );

                    return "( julianday( " + name2 + " ) - julianday( " +
                           name1 + " ) ) * 86400.0";
                }
            case enums::DataUsed::time_stamps_diff:
                {
                    const auto name1 =
                        "t1.\"" + _output.time_stamps_name() + "\"";

                    const auto name2 =
                        "t2.\"" + _input.time_stamps_name() + "\"";

                    return "( julianday( " + name1 + " ) - julianday( " +
                           name2 + " ) ) * 86400.0";
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel
