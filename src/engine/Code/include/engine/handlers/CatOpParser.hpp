#ifndef ENGINE_HANDLERS_CATOPPARSER_HPP_
#define ENGINE_HANDLERS_CATOPPARSER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class CatOpParser
{
    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    static std::vector<std::string> parse(
        const containers::Encoding& categories_,
        const containers::Encoding& join_keys_encoding_,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    static std::vector<std::string> binary_operation(
        const containers::Encoding& categories_,
        const containers::Encoding& join_keys_encoding_,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    /// Transforms a float column to a string.
    static std::vector<std::string> to_string(
        const containers::DataFrame& _df, const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    static std::vector<std::string> unary_operation(
        const containers::Encoding& categories_,
        const containers::Encoding& join_keys_encoding_,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static std::vector<std::string> bin_op(
        const containers::Encoding& categories_,
        const containers::Encoding& join_keys_encoding_,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = parse(
            categories_,
            join_keys_encoding_,
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse(
            categories_,
            join_keys_encoding_,
            _df,
            *JSON::get_object( _col, "operand2_" ) );

        assert( operand1.size() == operand2.size() );

        auto result = std::vector<std::string>( operand1.size() );

        std::transform(
            operand1.begin(),
            operand1.end(),
            operand2.begin(),
            result.begin(),
            _op );

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    static std::vector<std::string> un_op(
        const containers::Encoding& categories_,
        const containers::Encoding& join_keys_encoding_,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = parse(
            categories_,
            join_keys_encoding_,
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        auto result = std::vector<std::string>( operand1.size() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }

    /// Transforms a column to vector of equal length.
    static std::vector<std::string> to_vec(
        const containers::Encoding& _encoding,
        const containers::Column<Int>& _col )
    {
        auto result = std::vector<std::string>( _col.nrows() );

        std::transform(
            _col.begin(),
            _col.end(),
            result.begin(),
            [_encoding]( const Int val ) { return _encoding[val]; } );

        return result;
    }

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_CATOPPARSER_HPP_
