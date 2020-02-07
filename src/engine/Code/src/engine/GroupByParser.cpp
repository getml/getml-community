#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::categorical_aggregation(
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index )
{
    const auto vec =
        CatOpParser( categories_, join_keys_encoding_, {df()}, df().nrows() )
            .parse( _json_col );

    if ( _type == "count_categorical" )
        {
            const auto count_categorical =
                []( const std::vector<std::string>& vec ) {
                    return utils::ColumnOperators::count_categorical( vec );
                };

            return aggregate( _unique, _index, vec, _as, count_categorical );
        }
    else if ( _type == "count_distinct" )
        {
            const auto count_distinct =
                []( const std::vector<std::string>& vec ) {
                    return utils::ColumnOperators::count_distinct( vec );
                };

            return aggregate( _unique, _index, vec, _as, count_distinct );
        }
    else
        {
            throw std::invalid_argument(
                "Aggregation '" + _type +
                "' not recognized for a categorical column." );

            return containers::Column<Float>();
        }
}

// ----------------------------------------------------------------------------

std::pair<const containers::DataFrameIndex, const containers::Column<Int>>
GroupByParser::find_index( const std::string& _join_key_name )
{
    for ( size_t i = 0; i < df().num_join_keys(); ++i )
        {
            if ( df().join_key( i ).name() == _join_key_name )
                {
                    const auto index = df().index( i );

                    auto unique =
                        containers::Column<Int>( index.map()->size() );

                    unique.set_name( _join_key_name );

                    size_t j = 0;

                    for ( const auto& [k, v] : *index.map() )
                        {
                            unique[j++] = k;
                        }

                    return std::pair( index, unique );
                }
        }

    throw std::invalid_argument(
        "DataFrame '" + df().name() + "' has no join key named '" +
        _join_key_name + "'." );
}

// ----------------------------------------------------------------------------

containers::DataFrame GroupByParser::group_by(
    const std::string& _name,
    const std::string& _join_key_name,
    const Poco::JSON::Array& _aggregations )
{
    auto result =
        containers::DataFrame( _name, categories_, join_keys_encoding_ );

    const auto [index, unique] = find_index( _join_key_name );

    result.add_int_column( unique, "join_key" );

    for ( size_t i = 0; i < _aggregations.size(); ++i )
        {
            const auto agg = _aggregations.getObject( i );

            if ( !agg )
                {
                    throw std::invalid_argument(
                        "Error while parsing JSON: One of the aggregations is "
                        "not in proper format." );
                }

            const auto type = JSON::get_value<std::string>( *agg, "type_" );

            const auto as = JSON::get_value<std::string>( *agg, "as_" );

            const auto json_col = *JSON::get_object( *agg, "col_" );

            if ( type == "count_categorical" || type == "count_distinct" )
                {
                    const auto col = categorical_aggregation(
                        type, as, json_col, unique, index );

                    result.add_float_column( col, "unused" );
                }
            else
                {
                    const auto col = numerical_aggregation(
                        type, as, json_col, unique, index );

                    result.add_float_column( col, "unused" );
                }
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::numerical_aggregation(
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index )
{
    const auto col =
        NumOpParser( categories_, join_keys_encoding_, {df()}, df().nrows() )
            .parse( _json_col );

    if ( _type == "assert_equal" )
        {
            const auto assert_equal = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::assert_equal(
                    vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, assert_equal );
        }
    else if ( _type == "avg" )
        {
            const auto avg = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::avg( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, avg );
        }
    else if ( _type == "count" )
        {
            const auto count = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::count( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, count );
        }
    else if ( _type == "max" )
        {
            const auto max = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::max( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, max );
        }
    else if ( _type == "median" )
        {
            const auto median = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::median( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, median );
        }
    else if ( _type == "min" )
        {
            const auto min = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::min( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, min );
        }
    else if ( _type == "stddev" )
        {
            const auto stddev = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::stddev( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, stddev );
        }
    else if ( _type == "sum" )
        {
            const auto sum = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::sum( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, sum );
        }
    else if ( _type == "var" )
        {
            const auto var = []( const std::vector<Float>& vec ) {
                return utils::ColumnOperators::var( vec.begin(), vec.end() );
            };

            return aggregate( _unique, _index, col, _as, var );
        }
    else
        {
            throw std::invalid_argument(
                "Aggregation '" + _type +
                "' not recognized for a numerical column." );

            return containers::Column<Float>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
