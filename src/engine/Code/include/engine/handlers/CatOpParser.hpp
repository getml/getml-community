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
    typedef containers::ColumnView<bool>::UnknownSize UnknownSize;
    typedef containers::ColumnView<bool>::NRowsType NRowsType;
    typedef containers::ColumnView<bool>::ValueFunc ValueFunc;

    static constexpr UnknownSize NOT_KNOWABLE =
        containers::ColumnView<bool>::NOT_KNOWABLE;
    static constexpr UnknownSize INFINITE =
        containers::ColumnView<bool>::INFINITE;

    static constexpr bool NROWS_MUST_MATCH =
        containers::ColumnView<bool>::NROWS_MUST_MATCH;

    static constexpr const char* FLOAT_COLUMN =
        containers::Column<bool>::FLOAT_COLUMN;
    static constexpr const char* STRING_COLUMN =
        containers::Column<bool>::STRING_COLUMN;

    static constexpr const char* FLOAT_COLUMN_VIEW =
        containers::Column<bool>::FLOAT_COLUMN_VIEW;
    static constexpr const char* STRING_COLUMN_VIEW =
        containers::Column<bool>::STRING_COLUMN_VIEW;
    static constexpr const char* BOOLEAN_COLUMN_VIEW =
        containers::Column<bool>::BOOLEAN_COLUMN_VIEW;

    // ------------------------------------------------------------------------

   public:
    CatOpParser(
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

    ~CatOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Checks the string column for any obvious problems.
    void check(
        const std::vector<std::string>& _col,
        const std::string& _name,
        const std::shared_ptr<const communication::Logger>& _logger,
        Poco::Net::StreamSocket* _socket ) const;

    /// Parses a numerical column.
    containers::ColumnView<std::string> parse(
        const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    containers::ColumnView<std::string> binary_operation(
        const Poco::JSON::Object& _col ) const;

    /// Transforms a boolean column to a string.
    containers::ColumnView<std::string> boolean_as_string(
        const Poco::JSON::Object& _col ) const;

    /// Transforms a float column to a string.
    containers::ColumnView<std::string> numerical_as_string(
        const Poco::JSON::Object& _col ) const;

    /// Returns a subselection on the column.
    containers::ColumnView<std::string> subselection(
        const Poco::JSON::Object& _col ) const;

    /// Transforms an int column to a column view.
    containers::ColumnView<std::string> to_view(
        const containers::Column<Int>& _col,
        const std::shared_ptr<const containers::Encoding>& _encoding ) const;

    /// Transforms a string column to a column view.
    containers::ColumnView<std::string> to_view(
        const containers::Column<strings::String>& _col ) const;

    /// Parses the operator and undertakes a unary operation.
    containers::ColumnView<std::string> unary_operation(
        const Poco::JSON::Object& _col ) const;

    /// Returns an updated version of the column.
    containers::ColumnView<std::string> update(
        const Poco::JSON::Object& _col ) const;

    /// Returns a new column with a new unit.
    containers::ColumnView<std::string> with_unit(
        const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    containers::ColumnView<std::string> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        return containers::ColumnView<std::string>::from_bin_op(
            operand1, operand2, _op );
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    containers::ColumnView<std::string> un_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        return containers::ColumnView<std::string>::from_un_op( operand1, _op );
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

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_CATOPPARSER_HPP_
