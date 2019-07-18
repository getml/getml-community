#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::avg(
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index,
    const containers::Column<Float>& _col,
    const std::string& _as )
{
    const auto sum = [_col]( const Float init, const size_t ix ) {
        return init + _col[ix];
    };

    const auto numerator = num_agg( _unique, _index, _col, _as, sum );

    const auto divisor = count( _unique, _index, _as );

    assert( numerator.nrows() == divisor.nrows() );

    auto result = containers::Column<Float>( numerator.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < result.nrows(); ++i )
        {
            assert( divisor[i] > 0.0 );

            result[i] = numerator[i] / divisor[i];
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::count(
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index,
    const std::string& _as )
{
    assert( _index.map() );

    auto result = containers::Column<Float>( _unique.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < _unique.size(); ++i )
        {
            const auto it = _index.map()->find( _unique[i] );

            assert( it != _index.map()->end() );

            result[i] = static_cast<Float>( it->second.size() );
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::count_distinct(
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index,
    const std::vector<std::string>& _vec,
    const std::string& _as )
{
    assert( _index.map() );

    auto result = containers::Column<Float>( _unique.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < _unique.size(); ++i )
        {
            const auto it = _index.map()->find( _unique[i] );

            assert( it != _index.map()->end() );

            auto set = std::unordered_set<std::string>();

            for ( const auto ix : it->second )
                {
                    assert( ix < _vec.size() );
                    set.insert( _vec[ix] );
                }

            result[i] = static_cast<Float>( set.size() );
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::categorical_aggregation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index )
{
    const auto vec = CatOpParser::parse(
        _categories, _join_keys_encoding, {_df}, _json_col );

    if ( _type == "count_distinct" )
        {
            return count_distinct( _unique, _index, vec, _as );
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
GroupByParser::find_index(
    const containers::DataFrame& _df, const std::string& _join_key_name )
{
    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            if ( _df.join_key( i ).name() == _join_key_name )
                {
                    const auto index = _df.index( i );

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
        "DataFrame '" + _df.name() + "' has no join key named '" +
        _join_key_name + "'." );
}

// ----------------------------------------------------------------------------

containers::DataFrame GroupByParser::group_by(
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const containers::DataFrame& _df,
    const std::string& _name,
    const std::string& _join_key_name,
    const Poco::JSON::Array& _aggregations )
{
    auto result =
        containers::DataFrame( _name, _categories, _join_keys_encoding );

    const auto [index, unique] = find_index( _df, _join_key_name );

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

            if ( type == "count" )
                {
                    const auto col = count( unique, index, as );

                    result.add_float_column( col, "numerical" );
                }
            else if ( type == "count_distinct" )
                {
                    const auto col = categorical_aggregation(
                        *_categories,
                        *_join_keys_encoding,
                        _df,
                        type,
                        as,
                        json_col,
                        unique,
                        index );

                    result.add_float_column( col, "numerical" );
                }
            else
                {
                    const auto col = numerical_aggregation(
                        *_categories,
                        *_join_keys_encoding,
                        _df,
                        type,
                        as,
                        json_col,
                        unique,
                        index );

                    result.add_float_column( col, "numerical" );
                }
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::median(
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index,
    const containers::Column<Float>& _col,
    const std::string& _as )
{
    assert( _index.map() );

    auto result = containers::Column<Float>( _unique.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < _unique.size(); ++i )
        {
            const auto it = _index.map()->find( _unique[i] );

            assert( it != _index.map()->end() );

            assert( it->second.size() > 0 );

            auto values = std::vector<Float>( it->second.size() );

            for ( size_t j = 0; j < it->second.size(); ++j )
                {
                    values[j] = _col[it->second[j]];
                }

            std::sort( values.begin(), values.end() );

            if ( values.size() % 2 == 0 )
                {
                    result[i] = ( values[( values.size() / 2 ) - 1] +
                                  values[values.size() / 2] ) /
                                2.0;
                }
            else
                {
                    result[i] = values[values.size() / 2];
                }
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::numerical_aggregation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index )
{
    const auto col = NumOpParser::parse(
        _categories, _join_keys_encoding, {_df}, _json_col );

    if ( _type == "assert_equal" )
        {
            const auto assert_equal = [_as, col](
                                          const Float init, const size_t ix ) {
                if ( init != col[ix] )
                    {
                        throw std::runtime_error(
                            "Values for column '" + _as +
                            "' not equal: " + std::to_string( init ) + " vs. " +
                            std::to_string( col[ix] ) + "." );
                    }

                return init;
            };

            return num_agg( _unique, _index, col, _as, assert_equal );
        }
    else if ( _type == "avg" )
        {
            return avg( _unique, _index, col, _as );
        }
    else if ( _type == "max" )
        {
            const auto max = [col]( const Float init, const size_t ix ) {
                return ( ( init > col[ix] ) ? init : col[ix] );
            };

            return num_agg( _unique, _index, col, _as, max );
        }
    else if ( _type == "median" )
        {
            return median( _unique, _index, col, _as );
        }
    else if ( _type == "min" )
        {
            const auto min = [col]( const Float init, const size_t ix ) {
                return ( ( init < col[ix] ) ? init : col[ix] );
            };

            return num_agg( _unique, _index, col, _as, min );
        }
    else if ( _type == "stddev" )
        {
            return stddev( _unique, _index, col, _as );
        }
    else if ( _type == "sum" )
        {
            const auto sum = [col]( const Float init, const size_t ix ) {
                return init + col[ix];
            };

            return num_agg( _unique, _index, col, _as, sum );
        }
    else if ( _type == "var" )
        {
            return var( _unique, _index, col, _as );
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

/// Efficient implementation of var aggregation.
containers::Column<Float> GroupByParser::var(
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index,
    const containers::Column<Float>& _col,
    const std::string& _as )
{
    const auto sum = [_col]( const Float init, const size_t ix ) {
        return init + _col[ix];
    };

    const auto sums = num_agg( _unique, _index, _col, _as, sum );

    const auto counts = count( _unique, _index, _as );

    assert( sums.nrows() == counts.nrows() );

    assert( sums.nrows() == _unique.nrows() );

    auto result = containers::Column<Float>( sums.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < result.nrows(); ++i )
        {
            assert( counts[i] > 0.0 );

            const auto mean = sums[i] / counts[i];

            const auto it = _index.map()->find( _unique[i] );

            assert( it != _index.map()->end() );

            assert( it->second.size() > 0 );

            for ( const auto ix : it->second )
                {
                    const auto diff = _col[ix] - mean;

                    result[i] += diff * diff / counts[i];
                }
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
