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
        const containers::DataFrame& _df, const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    static containers::Column<Float> unary_operation(
        const containers::DataFrame& _df, const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    static containers::Column<Float> bin_op(
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 =
            parse( _df, *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 =
            parse( _df, *JSON::get_object( _col, "operand2_" ) );

        assert( operand1.nrows() == operand2.nrows() );

        auto result = containers::Column<Float>( operand1.nrows() );

        std::transform(
            operand1.begin(),
            operand1.end(),
            operand2.begin(),
            result.begin(),
            _op );

        return result;
    }

    /// Returns a columns containing random values.
    static containers::Column<Float> random(
        const containers::DataFrame& _df, const Poco::JSON::Object& _col )
    {
        const auto seed = JSON::get_value<unsigned int>( _col, "seed_" );

        auto rng = std::mt19937( seed );

        std::uniform_real_distribution<Float> dis( 0.0, 1.0 );

        auto result = containers::Column<Float>( _df.nrows() );

        for ( auto& val : result )
            {
                val = dis( rng );
            }

        return result;
    }

    /// Returns a columns containing the rowids.
    static containers::Column<Float> rowid( const containers::DataFrame& _df )
    {
        auto result = containers::Column<Float>( _df.nrows() );

        for ( size_t i = 0; i < _df.nrows(); ++i )
            {
                result[i] = static_cast<Float>( i );
            }

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    static containers::Column<Float> un_op(
        const containers::DataFrame& _df,
        const Poco::JSON::Object& _col,
        const Operator& _op )
    {
        const auto operand1 =
            parse( _df, *JSON::get_object( _col, "operand1_" ) );

        auto result = containers::Column<Float>( operand1.nrows() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }
    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_NUMOPPARSER_HPP_
