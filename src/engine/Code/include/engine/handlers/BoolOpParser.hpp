#ifndef ENGINE_HANDLERS_BOOLOPPARSER_HPP_
#define ENGINE_HANDLERS_BOOLOPPARSER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class BoolOpParser
{
    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    static std::vector<bool> parse(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    static std::vector<bool> binary_operation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    static std::vector<bool> unary_operation(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static std::vector<bool> bin_op(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = parse(
            _categories,
            _join_keys_encoding,
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse(
            _categories,
            _join_keys_encoding,
            _df,
            *JSON::get_object( _col, "operand2_" ) );

        assert( operand1.size() == operand2.size() );

        auto result = std::vector<bool>( operand1.size() );

        std::transform(
            operand1.begin(),
            operand1.end(),
            operand2.begin(),
            result.begin(),
            _op );

        return result;
    }

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static std::vector<bool> cat_bin_op(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = CatOpParser::parse(
            _categories,
            _join_keys_encoding,
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = CatOpParser::parse(
            _categories,
            _join_keys_encoding,
            _df,
            *JSON::get_object( _col, "operand2_" ) );

        assert( operand1.size() == operand2.size() );

        auto result = std::vector<bool>( operand1.size() );

        std::transform(
            operand1.begin(),
            operand1.end(),
            operand2.begin(),
            result.begin(),
            _op );

        return result;
    }

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static std::vector<bool> num_bin_op(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = NumOpParser::parse(
            /*_categories,
            _join_keys_encoding,*/
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = NumOpParser::parse(
            /*_categories,
            _join_keys_encoding,*/
            _df,
            *JSON::get_object( _col, "operand2_" ) );

        assert( operand1.size() == operand2.size() );

        auto result = std::vector<bool>( operand1.size() );

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
    static std::vector<bool> un_op(
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encoding,
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 = parse(
            _categories,
            _join_keys_encoding,
            _df,
            *JSON::get_object( _col, "operand1_" ) );

        auto result = std::vector<bool>( operand1.size() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_BOOLOPPARSER_HPP_
