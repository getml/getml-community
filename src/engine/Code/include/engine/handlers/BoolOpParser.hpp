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
    typedef containers::ColumnView<bool>::UnknownSize UnknownSize;
    typedef containers::ColumnView<bool>::NRowsType NRowsType;
    typedef containers::ColumnView<bool>::ValueFunc ValueFunc;

    static constexpr UnknownSize NOT_KNOWABLE =
        containers::ColumnView<bool>::NOT_KNOWABLE;
    static constexpr UnknownSize INFINITE =
        containers::ColumnView<bool>::INFINITE;

    static constexpr bool NROWS_MUST_MATCH =
        containers::ColumnView<bool>::NROWS_MUST_MATCH;

    // ------------------------------------------------------------------------

   public:
    BoolOpParser(
        const std::shared_ptr<const containers::Encoding>& _categories,
        const std::shared_ptr<const containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<
            const std::map<std::string, containers::DataFrame>>& _data_frames,
        const size_t _begin,
        const size_t _length,
        const bool _subselection )
        : begin_( _begin ),
          categories_( _categories ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          length_( _length ),
          subselection_( _subselection )
    {
        assert_true( categories_ );
        assert_true( data_frames_ );
        assert_true( join_keys_encoding_ );
    }

    ~BoolOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    containers::ColumnView<bool> parse( const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    containers::ColumnView<bool> binary_operation(
        const Poco::JSON::Object& _col ) const;

    /// Parses the operator and undertakes a unary operation.
    containers::ColumnView<bool> unary_operation(
        const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    containers::ColumnView<bool> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        return containers::ColumnView<bool>::from_bin_op(
            operand1, operand2, _op );
    }

    /// Undertakes a binary operation based on template class
    /// Operator for categorical columns.
    template <class Operator>
    containers::ColumnView<bool> cat_bin_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto vec1 = CatOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand1_" ) );

        const auto vec2 = CatOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand2_" ) );

        const auto operand1 =
            containers::ColumnView<std::string>::from_vector( vec1 );

        const auto operand2 =
            containers::ColumnView<std::string>::from_vector( vec2 );

        return containers::ColumnView<bool>::from_bin_op(
            operand1, operand2, _op );
    }

    /// Undertakes a binary operation based on template class
    /// Operator for numerical columns.
    template <class Operator>
    containers::ColumnView<bool> num_bin_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto col1 = NumOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand1_" ) );

        const auto col2 = NumOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand2_" ) );

        const auto operand1 =
            containers::ColumnView<Float>::from_column( col1 );

        const auto operand2 =
            containers::ColumnView<Float>::from_column( col2 );

        return containers::ColumnView<bool>::from_bin_op(
            operand1, operand2, _op );
    }

    /// Undertakes a unary operation based on template class
    /// Operator for numerical columns.
    template <class Operator>
    containers::ColumnView<bool> num_un_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto col1 = NumOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand1 =
            containers::ColumnView<Float>::from_column( col1 );

        return containers::ColumnView<bool>::from_un_op( operand1, _op );
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    containers::ColumnView<bool> un_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        return containers::ColumnView<bool>::from_un_op( operand1, _op );
    }

    // ------------------------------------------------------------------------

   private:
    /// The index of the first element to be drawn
    const size_t begin_;

    /// Encodes the categories used.
    const std::shared_ptr<const containers::Encoding> categories_;

    /// The DataFrames this is based on.
    const std::shared_ptr<const std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Encodes the join keys used.
    const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

    /// The number of elements required (must not be greater than the number of
    /// rows in df)
    const size_t length_;

    /// Whether we want to get a subselection.
    const bool subselection_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_BOOLOPPARSER_HPP_
