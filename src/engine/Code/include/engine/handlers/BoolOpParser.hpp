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
    BoolOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<const std::vector<containers::DataFrame>>& _df,
        const size_t _num_elem )
        : categories_( _categories ),
          df_( _df ),
          join_keys_encoding_( _join_keys_encoding ),
          num_elem_( _num_elem )
    {
        assert_true( categories_ );
        assert_true( df_ );
        assert_true( join_keys_encoding_ );
    }

    BoolOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::vector<containers::DataFrame>& _df,
        const size_t _num_elem )
        : categories_( _categories ),
          df_( std::make_shared<const std::vector<containers::DataFrame>>(
              _df ) ),
          join_keys_encoding_( _join_keys_encoding ),
          num_elem_( _num_elem )
    {
        assert_true( categories_ );
        assert_true( df_ );
        assert_true( join_keys_encoding_ );
    }

    ~BoolOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    std::vector<bool> parse( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    std::vector<bool> binary_operation( const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    std::vector<bool> unary_operation( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    std::vector<bool> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        assert_true( operand1.size() == operand2.size() );

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
    /// Operator for categorical columns.
    template <class Operator>
    std::vector<bool> cat_bin_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 =
            CatOpParser( categories_, join_keys_encoding_, df_, num_elem_ )
                .parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 =
            CatOpParser( categories_, join_keys_encoding_, df_, num_elem_ )
                .parse( *JSON::get_object( _col, "operand2_" ) );

        assert_true( operand1.size() == operand2.size() );

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
    /// Operator for numerical columns.
    template <class Operator>
    std::vector<bool> num_bin_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 =
            NumOpParser( categories_, join_keys_encoding_, df_, num_elem_ )
                .parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 =
            NumOpParser( categories_, join_keys_encoding_, df_, num_elem_ )
                .parse( *JSON::get_object( _col, "operand2_" ) );

        assert_true( operand1.size() == operand2.size() );

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
    /// Operator for numerical columns.
    template <class Operator>
    std::vector<bool> num_un_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 =
            NumOpParser( categories_, join_keys_encoding_, df_, num_elem_ )
                .parse( *JSON::get_object( _col, "operand1_" ) );

        auto result = std::vector<bool>( operand1.size() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    std::vector<bool> un_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        auto result = std::vector<bool>( operand1.size() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }

    // ------------------------------------------------------------------------

   private:
    /// Encodes the categories used.
    const std::shared_ptr<const containers::Encoding> categories_;

    /// The DataFrames this is based on.
    const std::shared_ptr<const std::vector<containers::DataFrame>> df_;

    /// Encodes the join keys used.
    const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

    /// The number of elements required (must not be greater than the number of
    /// rows in df)
    const size_t num_elem_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_BOOLOPPARSER_HPP_
