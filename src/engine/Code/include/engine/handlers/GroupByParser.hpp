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

    static containers::Column<Float> count_distinct(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const std::vector<std::string>& _vec,
        const std::string& _as );

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
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index );

    // ------------------------------------------------------------------------

   private:
    /// Undertakes a numerical aggregation based on template class
    /// Aggreagation.
    template <class Aggregation, class ColumnType>
    static containers::Column<Float> aggregate(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
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

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_GROUPBYPARSER_HPP_
