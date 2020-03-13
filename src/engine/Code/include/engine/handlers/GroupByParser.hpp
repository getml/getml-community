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
    /// Aggregates over a categorical column.
    containers::Column<Float> categorical_aggregation(
        const std::string& _type,
        const std::string& _as,
        const Poco::JSON::Object& _json_col,
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index );

    containers::Column<Float> count_distinct(
        const containers::Column<Int>& _unique,
        const containers::DataFrameIndex& _index,
        const std::vector<std::string>& _vec,
        const std::string& _as );

    /// Finds the correct index.
    std::pair<const containers::DataFrameIndex, const containers::Column<Int>>
    find_index( const std::string& _join_key_name );

    /// Parses a particular numerical aggregation.
    containers::Column<Float> numerical_aggregation(
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
    containers::Column<Float> aggregate(
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
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_GROUPBYPARSER_HPP_
