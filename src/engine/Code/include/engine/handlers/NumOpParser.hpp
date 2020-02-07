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
    NumOpParser(
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

    NumOpParser(
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

    ~NumOpParser() = default;

    // ------------------------------------------------------------------------

   public:
    /// Parses a numerical column.
    containers::Column<Float> parse( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

   private:
    /// Parses the operator and undertakes a binary operation.
    containers::Column<Float> binary_operation(
        const Poco::JSON::Object& _col );

    /// Transforms a boolean column to a float column.
    containers::Column<Float> boolean_to_num( const Poco::JSON::Object& _col );

    /// Returns an actual column.
    containers::Column<Float> get_column( const Poco::JSON::Object& _col );

    /// Transforms a string column to a float.
    containers::Column<Float> to_num( const Poco::JSON::Object& _col );

    /// Transforms a string column to a time stamp.
    containers::Column<Float> to_ts( const Poco::JSON::Object& _col );

    /// Parses the operator and undertakes a unary operation.
    containers::Column<Float> unary_operation( const Poco::JSON::Object& _col );

    /// Returns an updated version of the column.
    containers::Column<Float> update( const Poco::JSON::Object& _col );

    // ------------------------------------------------------------------------

    /// Undertakes a binary operation based on template class
    /// Operator.
    template <class Operator>
    containers::Column<Float> bin_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

        assert_true( operand1.nrows() == operand2.nrows() );

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
    containers::Column<Float> random( const Poco::JSON::Object& _col )
    {
        const auto seed = JSON::get_value<unsigned int>( _col, "seed_" );

        auto rng = std::mt19937( seed );

        std::uniform_real_distribution<Float> dis( 0.0, 1.0 );

        assert_true( df_->size() > 0 );

        auto result = containers::Column<Float>( ( *df_ )[0].nrows() );

        for ( auto& val : result )
            {
                val = dis( rng );
            }

        return result;
    }

    /// Returns a columns containing the rowids.
    containers::Column<Float> rowid()
    {
        assert_true( df_->size() > 0 );

        auto result = containers::Column<Float>( ( *df_ )[0].nrows() );

        for ( size_t i = 0; i < result.size(); ++i )
            {
                result[i] = static_cast<Float>( i );
            }

        return result;
    }

    /// Undertakes a unary operation based on template class
    /// Operator.
    template <class Operator>
    containers::Column<Float> un_op(
        const Poco::JSON::Object& _col, const Operator& _op )
    {
        const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

        auto result = containers::Column<Float>( operand1.nrows() );

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

#endif  // ENGINE_HANDLERS_NUMOPPARSER_HPP_
