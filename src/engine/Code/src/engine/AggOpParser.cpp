#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

Float AggOpParser::categorical_aggregation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const std::string& _type,
    const Poco::JSON::Object& _json_col )
{
    const auto vec = CatOpParser::parse(
        _categories, _join_keys_encoding, {_df}, _json_col );

    if ( _type == "count_categorical" )
        {
            return utils::ColumnOperators::count_categorical( vec );
        }
    else if ( _type == "count_distinct" )
        {
            return utils::ColumnOperators::count_distinct( vec );
        }
    else
        {
            throw std::invalid_argument(
                "Aggregation '" + _type +
                "' not recognized for a categorical column." );

            return 0.0;
        }
}

// ----------------------------------------------------------------------------

Float AggOpParser::aggregate(
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _aggregation )
{
    const auto type = JSON::get_value<std::string>( _aggregation, "type_" );

    const auto json_col = *JSON::get_object( _aggregation, "col_" );

    if ( type == "count_categorical" || type == "count_distinct" )
        {
            return categorical_aggregation(
                *_categories, *_join_keys_encoding, _df, type, json_col );
        }
    else
        {
            return numerical_aggregation(
                *_categories, *_join_keys_encoding, _df, type, json_col );
        }
}

// ----------------------------------------------------------------------------

Float AggOpParser::numerical_aggregation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const std::string& _type,
    const Poco::JSON::Object& _json_col )
{
    const auto col = NumOpParser::parse(
        _categories, _join_keys_encoding, {_df}, _json_col );

    if ( _type == "assert_equal" )
        {
            return utils::ColumnOperators::assert_equal(
                col.begin(), col.end() );
        }
    else if ( _type == "avg" )
        {
            return utils::ColumnOperators::avg( col.begin(), col.end() );
        }
    else if ( _type == "count" )
        {
            return utils::ColumnOperators::count( col.begin(), col.end() );
        }
    else if ( _type == "max" )
        {
            return utils::ColumnOperators::max( col.begin(), col.end() );
        }
    else if ( _type == "median" )
        {
            return utils::ColumnOperators::median( col.begin(), col.end() );
        }
    else if ( _type == "min" )
        {
            return utils::ColumnOperators::min( col.begin(), col.end() );
        }
    else if ( _type == "stddev" )
        {
            return utils::ColumnOperators::stddev( col.begin(), col.end() );
        }
    else if ( _type == "sum" )
        {
            return utils::ColumnOperators::sum( col.begin(), col.end() );
        }
    else if ( _type == "var" )
        {
            return utils::ColumnOperators::var( col.begin(), col.end() );
        }
    else
        {
            throw std::invalid_argument(
                "Aggregation '" + _type +
                "' not recognized for a numerical column." );

            return 0.0;
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
