#include "dfs/containers/containers.hpp"

namespace dfs
{
namespace containers
{
// ----------------------------------------------------------------------------

std::string SQLMaker::condition(
    const std::string& _feature_prefix,
    const Condition& _condition,
    const Placeholder& _input,
    const Placeholder& _output )
{
    switch ( _condition.data_used_ )
        {
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
    const Placeholder& _input,
    const Placeholder& _output )
{
    switch ( _data_used )
        {
            case enums::DataUsed::categorical:
                assert_true( _input_col < _input.num_categoricals() );
                return helpers::SQLGenerator::edit_colname(
                    _input.categorical_name( _input_col ), "t2" );

            case enums::DataUsed::discrete:
                assert_true( _input_col < _input.num_discretes() );
                return helpers::SQLGenerator::edit_colname(
                    _input.discrete_name( _input_col ), "t2" );

            case enums::DataUsed::numerical:
                assert_true( _input_col < _input.num_numericals() );
                return helpers::SQLGenerator::edit_colname(
                    _input.numerical_name( _input_col ), "t2" );

            case enums::DataUsed::subfeatures:
                {
                    const auto number =
                        helpers::SQLGenerator::make_subfeature_identifier(
                            _feature_prefix, _peripheral, _input_col );

                    return "COALESCE( f_" + number + ".\"feature_" + number +
                           "\", 0.0 )";
                }

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
    const Placeholder& _input,
    const Placeholder& _output )
{
    switch ( _data_used )
        {
            case enums::DataUsed::same_units_categorical:
                {
                    assert_true( _output_col < _output.num_categoricals() );

                    assert_true( _input_col < _input.num_categoricals() );

                    const auto name1 = helpers::SQLGenerator::edit_colname(
                        _output.categorical_name( _output_col ), "t1" );

                    const auto name2 = helpers::SQLGenerator::edit_colname(
                        _input.categorical_name( _input_col ), "t2" );

                    return std::make_pair( name1, name2 );
                }

            case enums::DataUsed::same_units_discrete:
                {
                    assert_true( _output_col < _output.num_discretes() );

                    assert_true( _input_col < _input.num_discretes() );

                    const auto name1 = helpers::SQLGenerator::edit_colname(
                        _output.discrete_name( _output_col ), "t1" );

                    const auto name2 = helpers::SQLGenerator::edit_colname(
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

                    const auto name1 = helpers::SQLGenerator::edit_colname(
                        _output.numerical_name( _output_col ), "t1" );

                    const auto name2 = helpers::SQLGenerator::edit_colname(
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

std::string SQLMaker::select_statement(
    const std::string& _feature_prefix,
    const AbstractFeature& _abstract_feature,
    const Placeholder& _input,
    const Placeholder& _output )
{
    const auto agg_type = enums::Parser<enums::Aggregation>::to_str(
        _abstract_feature.aggregation_ );

    std::string select;

    if ( agg_type == "COUNT DISTINCT" )
        {
            select += "COUNT( DISTINCT ";
        }
    else if ( agg_type == "COUNT MINUS COUNT DISTINCT" )
        {
            select += "COUNT( * ) - COUNT( DISTINCT ";
        }
    else
        {
            select += agg_type;

            select += "( ";
        }

    select += value_to_be_aggregated(
        _feature_prefix, _abstract_feature, _input, _output );

    select += " )";

    return select;
}

// ----------------------------------------------------------------------------

std::string SQLMaker::value_to_be_aggregated(
    const std::string& _feature_prefix,
    const AbstractFeature& _abstract_feature,
    const Placeholder& _input,
    const Placeholder& _output )
{
    switch ( _abstract_feature.data_used_ )
        {
            case enums::DataUsed::categorical:
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
}  // namespace dfs
