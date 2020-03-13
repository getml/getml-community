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
    CatOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<
            const std::map<std::string, containers::DataFrame>>& _data_frames,
        const size_t _num_elem,
        const bool _subselection )
        : categories_( _categories ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          num_elem_( _num_elem ),
          subselection_( _subselection )
    {
        assert_true( categories_ );
        assert_true( data_frames_ );
        assert_true( join_keys_encoding_ );
    }

    ~CatOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    std::vector<std::string> parse( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    std::vector<std::string> binary_operation( const Poco::JSON::Object& _col );

    /// Transforms a boolean column to a string.
    std::vector<std::string> boolean_as_string(
        const Poco::JSON::Object& _col );

    /// Transforms a float column to a string.
    std::vector<std::string> numerical_as_string(
        const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    std::vector<std::string> unary_operation( const Poco::JSON::Object& _col );

    /// Returns an updated version of the column.
    std::vector<std::string> update( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    std::vector<std::string> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        if ( operand1.size() != operand2.size() )
            {
                throw std::invalid_argument(
                    "Columns must have the same length for binary operations "
                    "to be possible!" );
            }

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
    std::vector<std::string> un_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        auto result = std::vector<std::string>( operand1.size() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
    }

    /// Transforms a column to vector of equal length.
    std::vector<std::string> to_vec(
        const containers::Column<Int>& _col,
        const containers::Encoding& _encoding )
    {
        const bool wrong_length =
            ( !subselection_ && _col.size() != num_elem_ ) ||
            _col.size() < num_elem_;

        if ( wrong_length )
            {
                throw std::invalid_argument(
                    "Columns must have the same length for binary operations "
                    "to be possible!" );
            }

        auto result = std::vector<std::string>( num_elem_ );

        std::transform(
            _col.begin(),
            _col.begin() + num_elem_,
            result.begin(),
            [_encoding]( const Int val ) { return _encoding[val].str(); } );

        return result;
    }

    /// Transforms an unused string column to a vector of equal length.
    std::vector<std::string> to_vec(
        const containers::Column<strings::String>& _col )
    {
        const bool wrong_length =
            ( !subselection_ && _col.size() != num_elem_ ) ||
            _col.size() < num_elem_;

        if ( wrong_length )
            {
                throw std::invalid_argument(
                    "Columns must have the same length for binary operations "
                    "to be possible!" );
            }

        auto result = std::vector<std::string>( num_elem_ );

        std::transform(
            _col.begin(),
            _col.begin() + num_elem_,
            result.begin(),
            []( const strings::String& val ) { return val.str(); } );

        return result;
    }

    // ------------------------------------------------------------------------

   private:
    /// Encodes the categories used.
    const std::shared_ptr<const containers::Encoding> categories_;

    /// The DataFrames this is based on.
    const std::shared_ptr<const std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Encodes the join keys used.
    const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

    /// The number of elements required (must not be greater than the number of
    /// rows in df)
    const size_t num_elem_;

    /// Whether we want to get a subselection.
    const bool subselection_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_CATOPPARSER_HPP_
