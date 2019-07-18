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
    /// Executes a group_by operation.
    static containers::DataFrame group_by(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _name,
        const std::string& _join_key_name,
        const Poco::JSON::Array& _aggregations );

    // ------------------------------------------------------------------------

   private:
    /// Efficient implementation of avg aggregation.
    static containers::Column<Float> avg(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const containers::Column<Float>& _col,
        const std::string& _as );

    /// Efficient implementation of count aggregation.
    static containers::Column<Float> count(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const std::string& _as );

    /// Efficient implementation of count_distinct aggregation.
    static containers::Column<Float> count_distinct(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const std::vector<std::string>& _vec,
        const std::string& _as );

    /// Aggregates over a categorical column.
    static containers::Column<Float> categorical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index );

    /// Finds the correct index.
    static std::
        pair<const containers::DataFrameIndex, const containers::Column<Int>>
        find_index(
            const containers::DataFrame& _df,
            const std::string& _join_key_name );

    /// Efficient implementation of median aggregation.
    static containers::Column<Float> median(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const containers::Column<Float>& _col,
        const std::string& _as );

    /// Parses a particular numerical aggregation.
    static containers::Column<Float> numerical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index );

    /// Efficient implementation of var aggregation.
    static containers::Column<Float> var(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const containers::Column<Float>& _col,
        const std::string& _as );

    // ------------------------------------------------------------------------

   private:
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

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_GROUPBYPARSER_HPP_
