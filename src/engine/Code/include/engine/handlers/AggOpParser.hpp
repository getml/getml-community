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
        const std::shared_ptr<
            const std::map<std::string, containers::DataFrame>>& _data_frames )
        : categories_( _categories ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding )
    {
        assert_true( categories_ );
        assert_true( data_frames_ );
        assert_true( join_keys_encoding_ );
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
    const std::shared_ptr<const std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Encodes the join keys used.
    const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_AGGOPPARSER_HPP_
