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
    AggOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<const std::vector<containers::DataFrame>>& _df )
        : categories_( _categories ),
          df_( _df ),
          join_keys_encoding_( _join_keys_encoding )
    {
    }

    AggOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::vector<containers::DataFrame>& _df )
        : categories_( _categories ),
          df_( std::make_shared<const std::vector<containers::DataFrame>>(
              _df ) ),
          join_keys_encoding_( _join_keys_encoding )
    {
    }

    ~AggOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Executes an aggregation.
    Float aggregate( const Poco::JSON::Object& _aggregation );

    // ------------------------------------------------------------------------

   private:
    /// Aggregates over a categorical column.
    Float categorical_aggregation(
        const std::string& _type, const Poco::JSON::Object& _json_col );

    /// Parses a particular numerical aggregation.
    Float numerical_aggregation(
        const std::string& _type, const Poco::JSON::Object& _json_col );

    // ------------------------------------------------------------------------

   private:
    /// Encodes the categories used.
    const std::shared_ptr<const containers::Encoding> categories_;

    /// The DataFrames this is based on.
    const std::shared_ptr<const std::vector<containers::DataFrame>> df_;

    /// Encodes the join keys used.
    const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
