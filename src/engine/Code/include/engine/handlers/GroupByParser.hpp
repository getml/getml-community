#ifndef ENGINE_HANDLERS_GROUPBYPARSER_HPP_
#define ENGINE_HANDLERS_GROUPBYPARSER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class GroupByParser
{
    // ------------------------------------------------------------------------

   public:
    GroupByParser(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<const std::vector<containers::DataFrame>>& _df )
        : categories_( _categories ),
          df_( _df ),
          join_keys_encoding_( _join_keys_encoding )
    {
        assert_true( categories_ );
        assert_true( df_ );
        assert_true( join_keys_encoding_ );
    }

    GroupByParser(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::vector<containers::DataFrame>& _df )
        : categories_( _categories ),
          df_( std::make_shared<const std::vector<containers::DataFrame>>(
              _df ) ),
          join_keys_encoding_( _join_keys_encoding )
    {
    }

    ~GroupByParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Executes a group_by operation.
    containers::DataFrame group_by(
        const std::string& _name,
        const std::string& _join_key_name,
        const Poco::JSON::Array& _aggregations );

    // ------------------------------------------------------------------------

   private:
    /// Undertakes a numerical aggregation based on template class
    /// Aggregation.
    template <class Aggregation, class ColumnType, class UniqueType, class Hash>
    containers::Column<Float> aggregate(
        const containers::Column<UniqueType>& _unique,
        const containers::Index<UniqueType, Hash>& _index,
        const ColumnType& _col,
        const std::string& _as,
        const Aggregation& _agg );

    /// Aggregates over a categorical column.
    template <class UniqueType, class Hash>
    containers::Column<Float> categorical_aggregation(
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<UniqueType>& _unique,
        const containers::Index<UniqueType, Hash>& _index );

    /// Implements a count distinct aggregation.
    template <class UniqueType, class Hash>
    containers::Column<Float> count_distinct(
        const containers::Column<UniqueType>& _unique,
        const containers::Index<UniqueType, Hash>& _index,
        const std::vector<std::string>& _vec,
        const std::string& _as );

    /// Finds the correct index.
    std::pair<const containers::DataFrameIndex, const containers::Column<Int>>
    find_index( const std::string& _join_key_name );

    /// Executes a group_by operation.
    template <class UniqueType, class Hash>
    void group_by_unique(
        const Poco::JSON::Array& _aggregations,
        const containers::Column<UniqueType>& _unique,
        const containers::Index<UniqueType, Hash>& _index,
        containers::DataFrame* _result );

    /// We use this when we want to GROUP BY a column that is not a join key
    template <class UniqueType, class Hash = std::hash<UniqueType>>
    std::pair<
        const containers::Index<UniqueType, Hash>,
        const containers::Column<UniqueType>>
    make_index( const containers::Column<UniqueType>& _col );

    /// Parses a particular numerical aggregation.
    template <class UniqueType, class Hash>
    containers::Column<Float> numerical_aggregation(
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<UniqueType>& _unique,
        const containers::Index<UniqueType, Hash>& _index );

    // ------------------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    const containers::DataFrame& df() const
    {
        assert_true( df_ );
        assert_true( df_->size() == 1 );
        return ( *df_ )[0];
    }

    // ------------------------------------------------------------------------

   private:
    /// Encodes the categories used.
    const std::shared_ptr<containers::Encoding> categories_;

    /// The DataFrames this is based on.
    const std::shared_ptr<const std::vector<containers::DataFrame>> df_;

    /// Encodes the join keys used.
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class Aggregation, class ColumnType, class UniqueType, class Hash>
containers::Column<Float> GroupByParser::aggregate(
    const containers::Column<UniqueType>& _unique,
    const containers::Index<UniqueType, Hash>& _index,
    const ColumnType& _col,
    const std::string& _as,
    const Aggregation& _agg )
{
    assert_true( _index.map() );

    auto result = containers::Column<Float>( _unique.nrows() );

    result.set_name( _as );

    for ( size_t i = 0; i < _unique.size(); ++i )
        {
            const auto it = _index.map()->find( _unique[i] );

            assert_true( it != _index.map()->end() );

            assert_true( it->second.size() > 0 );

            auto values = std::vector<typename ColumnType::value_type>(
                it->second.size() );

            for ( size_t j = 0; j < values.size(); ++j )
                {
                    const auto ix = it->second[j];
                    assert_true( ix < _col.size() );
                    values[j] = _col[ix];
                }

            result[i] = _agg( values );
        }

    return result;
}

// ----------------------------------------------------------------------------

template <class UniqueType, class Hash>
containers::Column<Float> GroupByParser::categorical_aggregation(
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<UniqueType>& _unique,
    const containers::Index<UniqueType, Hash>& _index )
{
    const auto data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>();

    ( *data_frames )[df().name()] = df();

    const auto vec =
        CatOpParser(
            categories_, join_keys_encoding_, data_frames, df().nrows(), false )
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

template <class UniqueType, class Hash>
void GroupByParser::group_by_unique(
    const Poco::JSON::Array& _aggregations,
    const containers::Column<UniqueType>& _unique,
    const containers::Index<UniqueType, Hash>& _index,
    containers::DataFrame* _result )
{
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
                        type, as, json_col, _unique, _index );

                    _result->add_float_column( col, "unused_float" );
                }
            else
                {
                    const auto col = numerical_aggregation(
                        type, as, json_col, _unique, _index );

                    _result->add_float_column( col, "unused_float" );
                }
        }
}

// ----------------------------------------------------------------------------

template <class UniqueType, class Hash>
std::pair<
    const containers::Index<UniqueType, Hash>,
    const containers::Column<UniqueType>>
GroupByParser::make_index( const containers::Column<UniqueType>& _col )
{
    auto index = containers::Index<UniqueType, Hash>();

    index.calculate( _col );

    auto unique = containers::Column<UniqueType>( index.map()->size() );

    unique.set_name( _col.name() );

    size_t j = 0;

    for ( const auto& [k, v] : *index.map() )
        {
            unique[j++] = k;
        }

    return std::make_pair( index, unique );
}

// ----------------------------------------------------------------------------

template <class UniqueType, class Hash>
containers::Column<Float> GroupByParser::numerical_aggregation(
    const std::string& _type,
    const std::string& _as,
    const Poco::JSON::Object& _json_col,
    const containers::Column<UniqueType>& _unique,
    const containers::Index<UniqueType, Hash>& _index )
{
    const auto data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>();

    ( *data_frames )[df().name()] = df();

    const auto col =
        NumOpParser(
            categories_, join_keys_encoding_, data_frames, df().nrows(), false )
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

#endif  // ENGINE_HANDLERS_GROUPBYPARSER_HPP_
