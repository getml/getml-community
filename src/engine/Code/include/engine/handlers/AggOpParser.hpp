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
    /// Aggregates over a categorical column.
    static Float categorical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const Poco::JSON::Object& _json_col );

    /// Parses a particular numerical aggregation.
    static Float numerical_aggregation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const std::string& _type,
        const Poco::JSON::Object& _json_col );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
