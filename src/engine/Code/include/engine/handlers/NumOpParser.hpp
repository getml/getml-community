#ifndef ENGINE_HANDLERS_NUMOPPARSER_HPP_
#define ENGINE_HANDLERS_NUMOPPARSER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class NumOpParser
{
    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    static containers::Column<Float> parse(
        const containers::DataFrame& _df, const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    static containers::Column<Float> binary_operation(
        const std::string& _operator,
        const containers::Column<Float>& _operand1,
        const containers::Column<Float>& _operand2 );

    /// Parses the operator and undertakes a unary operation.
    static containers::Column<Float> unary_operation(
        const std::string& _operator,
        const containers::Column<Float>& _operand1 );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static containers::Column<Float> bin_op(
        const containers::Column<Float>& _operand1,
        const containers::Column<Float>& _operand2,
        const Operator& _op )
    {
        assert( _operand1.nrows() == _operand2.nrows() );
        auto result = containers::Column<Float>( _operand1.nrows() );

        std::transform(
            _operand1.begin(),
            _operand1.end(),
            _operand2.begin(),
            result.begin(),
            _op );

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    static containers::Column<Float> un_op(
        const containers::Column<Float>& _operand1, const Operator& _op )
    {
        auto result = containers::Column<Float>( _operand1.nrows() );

        std::transform(
            _operand1.begin(), _operand1.end(), result.begin(), _op );

        return result;
    }
    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_NUMOPPARSER_HPP_
