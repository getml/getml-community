#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

Float AggOpParser::avg( const containers::Column<Float>& _col )
{
    const auto sum = []( const Float init, const Float val ) {
        return init + val;
    };

    const auto numerator = num_agg( _col, sum );

    const auto divisor = count( _col );

    return numerator / divisor;
}

// ----------------------------------------------------------------------------

Float AggOpParser::count_distinct( const std::vector<std::string>& _vec )
{
    auto set = std::unordered_set<std::string>();

    for ( const auto& str : _vec )
        {
            set.insert( str );
        }

    return static_cast<Float>( set.size() );
}

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

    if ( _type == "count_distinct" )
        {
            return count_distinct( vec );
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

    if ( type == "count" )
        {
            return static_cast<Float>( _df.nrows() );
        }
    else if ( type == "count_distinct" )
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

Float AggOpParser::median( const containers::Column<Float>& _col )
{
    auto values = std::vector<Float>( _col.begin(), _col.end() );

    std::sort( values.begin(), values.end() );

    if ( values.size() % 2 == 0 )
        {
            return ( values[( values.size() / 2 ) - 1] +
                     values[values.size() / 2] ) /
                   2.0;
        }
    else
        {
            return values[values.size() / 2];
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
            const auto assert_equal = []( const Float init, const Float val ) {
                if ( init != val )
                    {
                        throw std::runtime_error(
                            "Values not equal: " + std::to_string( init ) +
                            " vs. " + std::to_string( val ) + "." );
                    }

                return init;
            };

            return num_agg( col, assert_equal );
        }
    else if ( _type == "avg" )
        {
            return avg( col );
        }
    else if ( _type == "max" )
        {
            const auto max = []( const Float init, const Float val ) {
                return ( ( init > val ) ? init : val );
            };

            return num_agg( col, max );
        }
    else if ( _type == "median" )
        {
            return median( col );
        }
    else if ( _type == "min" )
        {
            const auto min = []( const Float init, const Float val ) {
                return ( ( init < val ) ? init : val );
            };

            return num_agg( col, min );
        }
    else if ( _type == "stddev" )
        {
            return stddev( col );
        }
    else if ( _type == "sum" )
        {
            const auto sum = []( const Float init, const Float val ) {
                return init + val;
            };

            return num_agg( col, sum );
        }
    else if ( _type == "var" )
        {
            return var( col );
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

/// Efficient implementation of var aggregation.
Float AggOpParser::var( const containers::Column<Float>& _col )
{
    const auto mean = avg( _col );

    const auto n = count( _col );

    const auto var_op = [mean, n]( const Float init, const Float val ) {
        const auto diff = val - mean;
        return init + diff * diff / n;
    };

    return std::accumulate( _col.begin(), _col.end(), 0.0, var_op );
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
