#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
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
                        "Error while parsing JSON: On of the aggregations is "
                        "not in proper format." );
                }

            const auto col = numerical_aggregation(
                *_categories, *_join_keys_encoding, _df, *agg, unique, index );

            result.add_float_column( col, "numerical" );
        }

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> GroupByParser::numerical_aggregation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _agg,
    const containers::Column<Int>& _unique,
    const containers::DataFrameIndex& _index )
{
    const auto type = JSON::get_value<std::string>( _agg, "type_" );

    const auto as = JSON::get_value<std::string>( _agg, "as_" );

    const auto json_col = *JSON::get_object( _agg, "col_" );

    const auto col =
        NumOpParser::parse( _categories, _join_keys_encoding, {_df}, json_col );

    if ( type == "assert_equal" )
        {
            const auto assert_equal = [as, col](
                                          const Float init, const size_t ix ) {
                if ( init != col[ix] )
                    {
                        throw std::runtime_error(
                            "Values for column '" + as +
                            "' not equal: " + std::to_string( init ) + " vs. " +
                            std::to_string( col[ix] ) + "." );
                    }

                return init;
            };

            return num_agg( _unique, _index, col, as, assert_equal );
        }
    else if ( type == "avg" )
        {
            return avg( _unique, _index, col, as );
        }
    else if ( type == "count" )
        {
            return count( _unique, _index, as );
        }
    else if ( type == "max" )
        {
            const auto max = [col]( const Float init, const size_t ix ) {
                return ( ( init > col[ix] ) ? init : col[ix] );
            };

            return num_agg( _unique, _index, col, as, max );
        }
    else if ( type == "median" )
        {
            return median( _unique, _index, col, as );
        }
    else if ( type == "min" )
        {
            const auto min = [col]( const Float init, const size_t ix ) {
                return ( ( init < col[ix] ) ? init : col[ix] );
            };

            return num_agg( _unique, _index, col, as, min );
        }
    else if ( type == "sum" )
        {
            const auto sum = [col]( const Float init, const size_t ix ) {
                return init + col[ix];
            };

            return num_agg( _unique, _index, col, as, sum );
        }
    else
        {
            throw std::invalid_argument(
                "Aggregation '" + type + "' not recognized." );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
