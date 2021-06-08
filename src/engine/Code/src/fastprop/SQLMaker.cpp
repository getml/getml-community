#include "fastprop/containers/containers.hpp"

namespace fastprop
{
namespace containers
{
// ----------------------------------------------------------------------------

std::string SQLMaker::condition(
    const std::vector<strings::String>& _categories,
    const std::string& _feature_prefix,
    const Condition& _condition,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    switch ( _condition.data_used_ )
        {
            case enums::DataUsed::categorical:
                {
                    const auto name = get_name(
                        _feature_prefix,
                        _condition.data_used_,
                        _condition.peripheral_,
                        _condition.input_col_,
                        _condition.output_col_,
                        _input,
                        _output );

                    assert_true(
                        _condition.category_used_ < _categories.size() );

                    const auto category =
                        _categories.at( _condition.category_used_ ).str();

                    return name + " = '" + category + "'";
                }

            case enums::DataUsed::lag:
                {
                    const auto col1 = helpers::SQLGenerator::make_epoch_time(
                        _output.time_stamps_name(), "t1" );

                    const auto col2 = helpers::SQLGenerator::make_epoch_time(
                        _input.time_stamps_name(), "t2" );

                    return "( " + col2 + " + " +
                           std::to_string( _condition.bound_upper_ ) + " > " +
                           col1 + " AND " + col2 + " + " +
                           std::to_string( _condition.bound_lower_ ) +
                           " <= " + col1 + " )";
                }

            case enums::DataUsed::same_units_categorical:
                {
                    const auto [name1, name2] = get_same_units(
                        _condition.data_used_,
                        _condition.input_col_,
                        _condition.output_col_,
                        _input,
                        _output );

                    return name1 + " = " + name2;
                }

            default:
                assert_true( false && "Unknown DataUsed!" );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::get_name(
    const std::string& _feature_prefix,
    const enums::DataUsed _data_used,
    const size_t _peripheral,
    const size_t _input_col,
    const size_t _output_col,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    const auto make_colname = []( const std::string& _colname ) -> std::string {
        return "t2.\"" + helpers::SQLGenerator::make_colname( _colname ) + "\"";
    };

    switch ( _data_used )
        {
            case enums::DataUsed::categorical:
                assert_true( _input_col < _input.num_categoricals() );
                return make_colname( _input.categorical_name( _input_col ) );

            case enums::DataUsed::discrete:
                assert_true( _input_col < _input.num_discretes() );
                return make_colname( _input.discrete_name( _input_col ) );

            case enums::DataUsed::numerical:
                assert_true( _input_col < _input.num_numericals() );
                return make_colname( _input.numerical_name( _input_col ) );

            case enums::DataUsed::subfeatures:
                {
                    const auto number = _feature_prefix +
                                        std::to_string( _peripheral + 1 ) +
                                        "_" + std::to_string( _input_col + 1 );

                    return "COALESCE( f_" + number + ".\"feature_" + number +
                           "\", 0.0 )";
                }

            case enums::DataUsed::text:
                assert_true( _input_col < _input.num_text() );
                return make_colname( _input.text_name( _input_col ) );

            default:
                assert_true( false && "Unknown DataUsed!" );
        }

    return "";
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> SQLMaker::get_same_units(
    const enums::DataUsed _data_used,
    const size_t _input_col,
    const size_t _output_col,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    const auto make_colname = []( const std::string& _colname,
                                  const std::string& _alias ) -> std::string {
        return _alias + ".\"" +
               helpers::SQLGenerator::make_colname( _colname ) + "\"";
    };

    switch ( _data_used )
        {
            case enums::DataUsed::same_units_categorical:
                {
                    assert_true( _output_col < _output.num_categoricals() );

                    assert_true( _input_col < _input.num_categoricals() );

                    const auto name1 = make_colname(
                        _output.categorical_name( _output_col ), "t1" );

                    const auto name2 = make_colname(
                        _input.categorical_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            case enums::DataUsed::same_units_discrete:
                {
                    assert_true( _output_col < _output.num_discretes() );

                    assert_true( _input_col < _input.num_discretes() );

                    const auto name1 = make_colname(
                        _output.discrete_name( _output_col ), "t1" );

                    const auto name2 = make_colname(
                        _input.discrete_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            case enums::DataUsed::same_units_discrete_ts:
                {
                    assert_true( _output_col < _output.num_discretes() );

                    assert_true( _input_col < _input.num_discretes() );

                    const auto name1 = helpers::SQLGenerator::make_epoch_time(
                        _output.discrete_name( _output_col ), "t1" );

                    const auto name2 = helpers::SQLGenerator::make_epoch_time(
                        _input.discrete_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            case enums::DataUsed::same_units_numerical:
                {
                    assert_true( _output_col < _output.num_numericals() );

                    assert_true( _input_col < _input.num_numericals() );

                    const auto name1 = make_colname(
                        _output.numerical_name( _output_col ), "t1" );

                    const auto name2 = make_colname(
                        _input.numerical_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            case enums::DataUsed::same_units_numerical_ts:
                {
                    assert_true( _output_col < _output.num_numericals() );

                    assert_true( _input_col < _input.num_numericals() );

                    const auto name1 = helpers::SQLGenerator::make_epoch_time(
                        _output.numerical_name( _output_col ), "t1" );

                    const auto name2 = helpers::SQLGenerator::make_epoch_time(
                        _input.numerical_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return std::make_pair( "", "" );
        }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_avg_time_between( const helpers::Schema& _input )
{
    assert_true( _input.num_time_stamps() > 0 );

    const auto ts_name =
        "t2.\"" +
        helpers::SQLGenerator::make_colname( _input.time_stamps_name() ) + "\"";

    return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + ts_name + " ) - MIN ( " +
           ts_name + " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_additional_argument(
    const enums::Aggregation& _aggregation,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    if ( _aggregation == enums::Aggregation::first ||
         _aggregation == enums::Aggregation::last )
        {
            return helpers::SQLGenerator::make_epoch_time(
                _input.time_stamps_name(), "t2" );
        }

    return helpers::SQLGenerator::make_epoch_time(
               _output.time_stamps_name(), "t1" ) +
           " - " +
           helpers::SQLGenerator::make_epoch_time(
               _input.time_stamps_name(), "t2" );
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_statement(
    const std::vector<strings::String>& _categories,
    const std::string& _feature_prefix,
    const AbstractFeature& _abstract_feature,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    constexpr auto AVG_TIME_BETWEEN =
        enums::Parser<enums::Aggregation>::AVG_TIME_BETWEEN;

    constexpr auto COUNT_DISTINCT =
        enums::Parser<enums::Aggregation>::COUNT_DISTINCT;

    constexpr auto COUNT_MINUS_COUNT_DISTINCT =
        enums::Parser<enums::Aggregation>::COUNT_MINUS_COUNT_DISTINCT;

    const auto agg_type = enums::Parser<enums::Aggregation>::to_str(
        _abstract_feature.aggregation_ );

    if ( agg_type == AVG_TIME_BETWEEN )
        {
            return select_avg_time_between( _input );
        }

    auto value = value_to_be_aggregated(
        _categories, _feature_prefix, _abstract_feature, _input, _output );

    if ( is_first_last( _abstract_feature.aggregation_ ) )
        {
            value +=
                ", " + make_additional_argument(
                           _abstract_feature.aggregation_, _input, _output );
        }

    std::string select;

    if ( agg_type == COUNT_DISTINCT )
        {
            select += "COUNT( DISTINCT ";
        }
    else if ( agg_type == COUNT_MINUS_COUNT_DISTINCT )
        {
            select += "COUNT( " + value + "  ) - COUNT( DISTINCT ";
        }
    else
        {
            select +=
                helpers::StringReplacer::replace_all( agg_type, " ", "_" );

            select += "( ";
        }

    select += value;

    select += " )";

    return select;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::value_to_be_aggregated(
    const std::vector<strings::String>& _categories,
    const std::string& _feature_prefix,
    const AbstractFeature& _abstract_feature,
    const helpers::Schema& _input,
    const helpers::Schema& _output )
{
    switch ( _abstract_feature.data_used_ )
        {
            case enums::DataUsed::categorical:
                if ( _abstract_feature.categorical_value_ ==
                     AbstractFeature::NO_CATEGORICAL_VALUE )
                    {
                        return get_name(
                            _feature_prefix,
                            _abstract_feature.data_used_,
                            _abstract_feature.peripheral_,
                            _abstract_feature.input_col_,
                            _abstract_feature.output_col_,
                            _input,
                            _output );
                    }
                else
                    {
                        const auto name = get_name(
                            _feature_prefix,
                            _abstract_feature.data_used_,
                            _abstract_feature.peripheral_,
                            _abstract_feature.input_col_,
                            _abstract_feature.output_col_,
                            _input,
                            _output );

                        assert_true(
                            _abstract_feature.categorical_value_ <
                            _categories.size() );

                        const auto category =
                            _categories
                                .at( _abstract_feature.categorical_value_ )
                                .str();

                        return "CASE WHEN " + name + " = '" + category +
                               "' THEN 1 ELSE 0 END";
                    }

            case enums::DataUsed::discrete:
            case enums::DataUsed::numerical:
            case enums::DataUsed::subfeatures:
                return get_name(
                    _feature_prefix,
                    _abstract_feature.data_used_,
                    _abstract_feature.peripheral_,
                    _abstract_feature.input_col_,
                    _abstract_feature.output_col_,
                    _input,
                    _output );

            case enums::DataUsed::not_applicable:
                return "*";

            case enums::DataUsed::same_units_categorical:
                {
                    const auto [name1, name2] = get_same_units(
                        _abstract_feature.data_used_,
                        _abstract_feature.input_col_,
                        _abstract_feature.output_col_,
                        _input,
                        _output );

                    return "CASE WHEN " + name1 + " = " + name2 +
                           " THEN 1 ELSE 0 END";
                }

            case enums::DataUsed::same_units_discrete:
            case enums::DataUsed::same_units_discrete_ts:
            case enums::DataUsed::same_units_numerical:
            case enums::DataUsed::same_units_numerical_ts:
                {
                    const auto [name1, name2] = get_same_units(
                        _abstract_feature.data_used_,
                        _abstract_feature.input_col_,
                        _abstract_feature.output_col_,
                        _input,
                        _output );

                    return name1 + " - " + name2;
                }

            default:
                assert_true( false && "Unknown data_used_" );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop
