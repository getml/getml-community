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
    /// .
    static containers::DataFrame group_by(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _name,
        const std::string& _join_key_name,
        const Poco::JSON::Array& _aggregations );

    // ------------------------------------------------------------------------

   private:
    /// Finds the correct index.
    static std::
        pair<const containers::DataFrameIndex, const containers::Column<Int>>
        find_index(
            const containers::DataFrame& _df,
            const std::string& _join_key_name );

    /// Parses a particular numerical aggregation.
    static containers::Column<Float> numerical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _agg,
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index );

    // ------------------------------------------------------------------------

   private:
    /// Efficient implementation of avg aggregation.
    static containers::Column<Float> avg(
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

    /// Efficient implementation of count aggregation.
    static containers::Column<Float> count(
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

    /// Efficient implementation of median aggregation.
    static containers::Column<Float> median(
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

    /// Undertakes a numerical aggregation based on template class
    /// Aggreagation.
    template <class Aggregation>
    static containers::Column<Float> num_agg(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const containers::Column<Float>& _col,
        const std::string& _as,
        const Aggregation& _agg )
    {
        assert( _index.map() );

        auto result = containers::Column<Float>( _unique.nrows() );

        result.set_name( _as );

        for ( size_t i = 0; i < _unique.size(); ++i )
            {
                const auto it = _index.map()->find( _unique[i] );

                assert( it != _index.map()->end() );

                assert( it->second.size() > 0 );

                result[i] = std::accumulate(
                    it->second.begin() + 1,
                    it->second.end(),
                    _col[*it->second.begin()],
                    _agg );
            }

        return result;
    }

    /// Efficient implementation of stddev aggregation.
    static containers::Column<Float> stddev(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const containers::Column<Float>& _col,
        const std::string& _as )
    {
        auto result = var( _unique, _index, _col, _as );

        for ( auto& val : result )
            {
                val = std::sqrt( val );
            }

        return result;
    }

    /// Efficient implementation of var aggregation.
    static containers::Column<Float> var(
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

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_GROUPBYPARSER_HPP_
