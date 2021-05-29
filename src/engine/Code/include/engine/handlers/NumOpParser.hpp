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
    NumOpParser(
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

    ~NumOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Checks a column for any obvious issues (such as high share of NULL
    /// values).
    void check(
        const containers::Column<Float>& _col,
        const std::shared_ptr<const communication::Logger>& _logger,
        Poco::Net::StreamSocket* _socket ) const;

    /// Parses a numerical column.
    containers::Column<Float> parse( const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

   private:
    /// Transforms a string column to a float.
    containers::Column<Float> as_num( const Poco::JSON::Object& _col ) const;

    /// Transforms a string column to a time stamp.
    containers::Column<Float> as_ts( const Poco::JSON::Object& _col ) const;

    /// Parses the operator and undertakes a binary operation.
    containers::Column<Float> binary_operation(
        const Poco::JSON::Object& _col ) const;

    /// Transforms a boolean column to a float column.
    containers::Column<Float> boolean_as_num(
        const Poco::JSON::Object& _col ) const;

    /// Returns an actual column.
    containers::Column<Float> get_column(
        const Poco::JSON::Object& _col ) const;

    /// Parses the operator and undertakes a unary operation.
    containers::Column<Float> unary_operation(
        const Poco::JSON::Object& _col ) const;

    /// Returns an updated version of the column.
    containers::Column<Float> update( const Poco::JSON::Object& _col ) const;

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    containers::Column<Float> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        if ( operand1.size() != operand2.size() )
            {
                throw std::invalid_argument(
                    "Columns must have the same length for binary operations "
                    "to be possible!" );
            }

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
    containers::Column<Float> random( const Poco::JSON::Object& _col ) const
    {
        const auto seed = JSON::get_value<unsigned int>( _col, "seed_" );

        auto rng = std::mt19937( seed );

        std::uniform_real_distribution<Float> dis( 0.0, 1.0 );

        auto result = containers::Column<Float>( length_ );

        for ( auto& val : result )
            {
                val = dis( rng );
            }

        return result;
    }

    /// Returns a columns containing the rowids.
    containers::Column<Float> rowid() const
    {
        auto result = containers::Column<Float>( length_ );

        for ( size_t i = 0; i < result.size(); ++i )
            {
                result[i] = begin_ + static_cast<Float>( i );
            }

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    containers::Column<Float> un_op(
        const Poco::JSON::Object& _col, const Operator& _op ) const
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        auto result = containers::Column<Float>( operand1.nrows() );

        std::transform( operand1.begin(), operand1.end(), result.begin(), _op );

        return result;
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

#endif  // ENGINE_HANDLERS_NUMOPPARSER_HPP_
