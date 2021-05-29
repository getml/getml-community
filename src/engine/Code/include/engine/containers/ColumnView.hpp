#ifndef ENGINE_CONTAINERS_COLUMNVIEW_HPP_
#define ENGINE_CONTAINERS_COLUMNVIEW_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class ColumnView
{
   public:
    typedef T value_type;
    typedef bool UnknownSize;
    typedef std::variant<size_t, UnknownSize> NRowsType;
    typedef std::function<NRowsType()> NRowsFunc;
    typedef std::function<std::optional<T>( size_t )> ValueFunc;

    static constexpr UnknownSize NOT_KNOWABLE = true;
    static constexpr UnknownSize INFINITE = false;

    static constexpr bool NROWS_MUST_MATCH = true;

   public:
    ColumnView(
        const ValueFunc& _value_func,
        const NRowsFunc& _nrows_func,
        const std::string& _unit = "" )
        : nrows_func_( _nrows_func ), unit_( _unit ), value_func_( _value_func )
    {
        static_assert( !std::is_same<T, void>(), "Type cannot be void!" );
    }

    ~ColumnView() {}

    // -------------------------------

    template <class T1, class T2, class Operator>
    static ColumnView<T> from_bin_op(
        const ColumnView<T1>& _operand1,
        const ColumnView<T2>& _operand2,
        const Operator _op )
    {
        const auto value_func =
            [_operand1, _operand2, _op]( const size_t _i ) -> std::optional<T> {
            if ( !_operand1[_i] )
                {
                    const bool nrows_do_not_match =
                        std::holds_alternative<UnknownSize>(
                            _operand2.nrows() ) &&
                        std::get<UnknownSize>( _operand2.nrows() ) !=
                            INFINITE &&
                        _operand2[_i];

                    if ( nrows_do_not_match )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for binary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            if ( !_operand2[_i] )
                {
                    const bool nrows_do_not_match =
                        std::holds_alternative<UnknownSize>(
                            _operand1.nrows() ) &&
                        std::get<UnknownSize>( _operand1.nrows() ) !=
                            INFINITE &&
                        _operand1[_i];

                    if ( nrows_do_not_match )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for binary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            return _op( *_operand1[_i], *_operand2[_i] );
        };

        const auto nrows_func = [_operand1, _operand2]() -> NRowsType {
            if ( std::holds_alternative<size_t>( _operand1.nrows() ) )
                {
                    const bool nrows_do_not_match =
                        std::holds_alternative<size_t>( _operand2.nrows() ) &&
                        std::get<size_t>( _operand1.nrows() ) !=
                            std::get<size_t>( _operand2.nrows() );

                    if ( nrows_do_not_match )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for binary "
                                "operations to be possible: " +
                                std::to_string(
                                    std::get<size_t>( _operand1.nrows() ) ) +
                                " vs. " +
                                std::to_string(
                                    std::get<size_t>( _operand2.nrows() ) ) +
                                "." );
                        }

                    return _operand1.nrows();
                }

            if ( std::holds_alternative<size_t>( _operand2.nrows() ) )
                {
                    return _operand2.nrows();
                }

            return std::get<UnknownSize>( _operand1.nrows() ) ||
                   std::get<UnknownSize>( _operand2.nrows() );
        };

        return ColumnView<T>( value_func, nrows_func );
    }

    static ColumnView<T> from_column( const Column<T>& _col )
    {
        const auto value_func = [_col]( const size_t _i ) -> std::optional<T> {
            if ( _i >= _col.nrows() )
                {
                    return std::nullopt;
                };

            return _col[_i];
        };

        const auto nrows_func = [_col]() -> NRowsType { return _col.nrows(); };

        return ColumnView<T>( value_func, nrows_func, _col.unit() );
    }

    template <class T1, class Operator>
    static ColumnView<T> from_un_op(
        const ColumnView<T1>& _operand, const Operator& _op )
    {
        const auto value_func = [_operand,
                                 _op]( const size_t _i ) -> std::optional<T> {
            if ( !_operand[_i] )
                {
                    return std::nullopt;
                }

            return _op( *_operand[_i] );
        };

        const auto nrows_func = [_operand]() -> NRowsType {
            return _operand.nrows();
        };

        return ColumnView<T>( value_func, nrows_func );
    }

    // TODO: Remove this -  it is a temporary fix and dangerous for your memory!
    static ColumnView<T> from_vector( const std::vector<T>& _vec )
    {
        const auto value_func = [_vec]( const size_t _i ) -> std::optional<T> {
            if ( _i >= _vec.size() )
                {
                    return std::nullopt;
                };

            return _vec[_i];
        };

        const auto nrows_func = [_vec]() -> NRowsType { return _vec.size(); };

        return ColumnView<T>( value_func, nrows_func );
    }

    // -------------------------------

   public:
    /// Transforms the ColumnView to a physical column.
    Column<T> to_column(
        const std::optional<size_t> _expected_nrows,
        const bool _nrows_must_match ) const;

    /// Transforms the ColumnView to a physical vector.
    std::shared_ptr<std::vector<T>> to_vector(
        const std::optional<size_t> _expected_nrows,
        const bool _nrows_must_match ) const;

    // -------------------------------

   public:
    /// Accessor to data
    std::optional<T> operator[]( const size_t _i ) const
    {
        return value_func_( _i );
    }

    /// Trivial getter
    NRowsType nrows() const { return nrows_func_(); }

    /// Trivial getter
    const std::string& unit() const { return unit_; }

    // -------------------------------

   private:
    /// Functiona returning the Number of rows (if that is knowable).
    const NRowsFunc nrows_func_;

    /// Unit of the column.
    const std::string unit_;

    /// The function returning the actual data point.
    const ValueFunc value_func_;

    // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
Column<T> ColumnView<T>::to_column(
    const std::optional<size_t> _expected_nrows,
    const bool _nrows_must_match ) const
{
    const auto data_ptr = to_vector( _expected_nrows, _nrows_must_match );

    auto col = Column<T>( data_ptr );

    col.set_unit( unit() );

    return col;
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<std::vector<T>> ColumnView<T>::to_vector(
    const std::optional<size_t> _expected_nrows,
    const bool _nrows_must_match ) const
{
    assert_true( _expected_nrows || !_nrows_must_match );

    const auto expected_nrows =
        _expected_nrows ? *_expected_nrows : std::numeric_limits<size_t>::max();

    const bool nrows_do_not_match =
        _nrows_must_match && std::holds_alternative<size_t>( nrows() ) &&
        std::get<size_t>( nrows() ) != _expected_nrows;

    if ( nrows_do_not_match )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_nrows ) +
                " nrows, but got " +
                std::to_string( std::get<size_t>( nrows() ) ) + "." );
        }

    const auto data_ptr = std::make_shared<std::vector<T>>();

    for ( size_t i = 0; i < expected_nrows; ++i )
        {
            const auto val = value_func_( i );

            if ( !val )
                {
                    break;
                }

            data_ptr->push_back( *val );
        }

    const bool exceeds_expected_by_unknown_number =
        _nrows_must_match && std::holds_alternative<UnknownSize>( nrows() ) &&
        std::get<UnknownSize>( nrows() ) == NOT_KNOWABLE &&
        value_func_( expected_nrows );

    if ( exceeds_expected_by_unknown_number )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_nrows ) +
                " nrows, but there were more." );
        }

    if ( _nrows_must_match && data_ptr->size() != expected_nrows )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_nrows ) +
                " nrows, but got " + std::to_string( data_ptr->size() ) + "." );
        }

    return data_ptr;
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_COLUMN_HPP_
