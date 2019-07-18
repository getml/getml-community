#ifndef ENGINE_HANDLERS_AGGOPPARSER_HPP_
#define ENGINE_HANDLERS_AGGOPPARSER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class AggOpParser
{
    // ------------------------------------------------------------------------

   public:
    /// Executes an aggregation.
    static Float aggregate(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _aggregation );

    // ------------------------------------------------------------------------

   private:
    /// Efficient implementation of avg aggregation.
    static Float avg( const containers::Column<Float>& _col );

    /// Efficient implementation of count_distinct aggregation.
    static Float count_distinct( const std::vector<std::string>& _vec );

    /// Aggregates over a categorical column.
    static Float categorical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const Poco::JSON::Object& _json_col );

    /// Efficient implementation of median aggregation.
    static Float median( const containers::Column<Float>& _col );

    /// Parses a particular numerical aggregation.
    static Float numerical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const Poco::JSON::Object& _json_col );

    /// Efficient implementation of var aggregation.
    static Float var( const containers::Column<Float>& _col );

    // ------------------------------------------------------------------------

   private:
    /// Efficient implementation of count aggregation.
    static Float count( const containers::Column<Float>& _col )
    {
        return static_cast<Float>( _col.nrows() );
    }

    /// Undertakes a numerical aggregation based on template class
    /// Aggreagation.
    template <class Aggregation>
    static Float num_agg(
        const containers::Column<Float>& _col, const Aggregation& _agg )
    {
        if ( _col.nrows() == 0 )
            {
                throw std::runtime_error( "Column cannot be of length 0." );
            }

        return std::accumulate(
            _col.begin() + 1, _col.end(), *_col.begin(), _agg );
    }

    /// Efficient implementation of stddev aggregation.
    static Float stddev( const containers::Column<Float>& _col )
    {
        return std::sqrt( var( _col ) );
    }

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
