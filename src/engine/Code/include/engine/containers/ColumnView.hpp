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
    typedef std::function<std::optional<T>( size_t )> ValueFunc;

    static constexpr UnknownSize NOT_KNOWABLE = true;
    static constexpr UnknownSize INFINITE = false;

    static constexpr bool NROWS_MUST_MATCH = true;

   public:
    ColumnView(
        const ValueFunc& _value_func,
        const NRowsType& _nrows,
        const std::string& _unit = "" )
        : nrows_( _nrows ), unit_( _unit ), value_func_( _value_func )
    {
        static_assert( !std::is_same<T, void>(), "Type cannot be void!" );
    }

    ~ColumnView() {}

    // -------------------------------

    /// Constructs a column view from a binary operation.
    template <class T1, class T2, class Operator>
    static ColumnView<T> from_bin_op(
        const ColumnView<T1>& _operand1,
        const ColumnView<T2>& _operand2,
        const Operator _op )
    {
        const auto value_func =
            [_operand1, _operand2, _op]( const size_t _i ) -> std::optional<T> {
            const auto op1 = _operand1[_i];
            const auto op2 = _operand2[_i];

            if ( !op1 )
                {
                    const bool nrows_do_not_match =
                        std::holds_alternative<UnknownSize>(
                            _operand2.nrows() ) &&
                        std::get<UnknownSize>( _operand2.nrows() ) !=
                            INFINITE &&
                        op2;

                    if ( nrows_do_not_match )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for binary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            if ( !op2 )
                {
                    const bool nrows_do_not_match =
                        std::holds_alternative<UnknownSize>(
                            _operand1.nrows() ) &&
                        std::get<UnknownSize>( _operand1.nrows() ) !=
                            INFINITE &&
                        op1;

                    if ( nrows_do_not_match )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for binary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            return _op( *op1, *op2 );
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

        return ColumnView<T>( value_func, nrows_func() );
    }

    /// Constructs a column view from a column.
    static ColumnView<T> from_column( const Column<T>& _col )
    {
        const auto value_func = [_col]( const size_t _i ) -> std::optional<T> {
            if ( _i >= _col.nrows() )
                {
                    return std::nullopt;
                };

            return _col[_i];
        };

        return ColumnView<T>( value_func, _col.nrows(), _col.unit() );
    }

    template <class T1, class Operator>
    static ColumnView<T> from_un_op(
        const ColumnView<T1>& _operand, const Operator& _op )
    {
        const auto value_func = [_operand,
                                 _op]( const size_t _i ) -> std::optional<T> {
            const auto op1 = _operand[_i];

            if ( !op1 )
                {
                    return std::nullopt;
                }

            return _op( *op1 );
        };

        return ColumnView<T>( value_func, _operand.nrows() );
    }

    // -------------------------------

    /// Constructs a column view from a ternary operation.
    template <class T1, class T2, class T3, class Operator>
    static ColumnView<T> from_tern_op(
        const ColumnView<T1>& _operand1,
        const ColumnView<T2>& _operand2,
        const ColumnView<T3>& _operand3,
        const Operator _op )
    {
        const auto value_func = [_operand1, _operand2, _operand3, _op](
                                    const size_t _i ) -> std::optional<T> {
            const auto op1 = _operand1[_i];
            const auto op2 = _operand2[_i];
            const auto op3 = _operand3[_i];

            if ( !op1 )
                {
                    const bool nrows_do_not_match2 =
                        std::holds_alternative<UnknownSize>(
                            _operand2.nrows() ) &&
                        std::get<UnknownSize>( _operand2.nrows() ) !=
                            INFINITE &&
                        op2;

                    const bool nrows_do_not_match3 =
                        std::holds_alternative<UnknownSize>(
                            _operand3.nrows() ) &&
                        std::get<UnknownSize>( _operand3.nrows() ) !=
                            INFINITE &&
                        op3;

                    if ( nrows_do_not_match2 || nrows_do_not_match3 )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for ternary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            if ( !op2 )
                {
                    const bool nrows_do_not_match1 =
                        std::holds_alternative<UnknownSize>(
                            _operand1.nrows() ) &&
                        std::get<UnknownSize>( _operand1.nrows() ) !=
                            INFINITE &&
                        op1;

                    const bool nrows_do_not_match3 =
                        std::holds_alternative<UnknownSize>(
                            _operand3.nrows() ) &&
                        std::get<UnknownSize>( _operand3.nrows() ) !=
                            INFINITE &&
                        op3;

                    if ( nrows_do_not_match1 || nrows_do_not_match3 )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for ternary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            if ( !op3 )
                {
                    const bool nrows_do_not_match1 =
                        std::holds_alternative<UnknownSize>(
                            _operand1.nrows() ) &&
                        std::get<UnknownSize>( _operand1.nrows() ) !=
                            INFINITE &&
                        op1;

                    const bool nrows_do_not_match2 =
                        std::holds_alternative<UnknownSize>(
                            _operand2.nrows() ) &&
                        std::get<UnknownSize>( _operand2.nrows() ) !=
                            INFINITE &&
                        op2;

                    if ( nrows_do_not_match1 || nrows_do_not_match2 )
                        {
                            throw std::invalid_argument(
                                "Number of rows between two columns do not "
                                "match, which is necessary for ternary "
                                "operations to be possible." );
                        }

                    return std::nullopt;
                }

            return _op( *op1, *op2, *op3 );
        };

        const auto nrows_func =
            [_operand1, _operand2, _operand3]() -> NRowsType {
            if ( std::holds_alternative<size_t>( _operand1.nrows() ) )
                {
                    const bool nrows_do_not_match2 =
                        std::holds_alternative<size_t>( _operand2.nrows() ) &&
                        std::get<size_t>( _operand1.nrows() ) !=
                            std::get<size_t>( _operand2.nrows() );

                    const bool nrows_do_not_match3 =
                        std::holds_alternative<size_t>( _operand3.nrows() ) &&
                        std::get<size_t>( _operand1.nrows() ) !=
                            std::get<size_t>( _operand3.nrows() );

                    if ( nrows_do_not_match2 || nrows_do_not_match3 )
                        {
                            throw std::invalid_argument(
                                "Number of rows between three columns do not "
                                "match, which is necessary for ternary "
                                "operations to be possible." );
                        }

                    return _operand1.nrows();
                }

            if ( std::holds_alternative<size_t>( _operand2.nrows() ) )
                {
                    const bool nrows_do_not_match3 =
                        std::holds_alternative<size_t>( _operand3.nrows() ) &&
                        std::get<size_t>( _operand2.nrows() ) !=
                            std::get<size_t>( _operand3.nrows() );

                    if ( nrows_do_not_match3 )
                        {
                            throw std::invalid_argument(
                                "Number of rows between three columns do not "
                                "match, which is necessary for ternary "
                                "operations to be possible." );
                        }

                    return _operand2.nrows();
                }

            if ( std::holds_alternative<size_t>( _operand3.nrows() ) )
                {
                    return _operand3.nrows();
                }

            return std::get<UnknownSize>( _operand1.nrows() ) ||
                   std::get<UnknownSize>( _operand2.nrows() ) ||
                   std::get<UnknownSize>( _operand3.nrows() );
        };

        return ColumnView<T>( value_func, nrows_func() );
    }

    /// Constructs a column view from a value.
    static ColumnView<T> from_value( const T _value )
    {
        const auto value_func =
            [_value]( const size_t _i ) -> std::optional<T> { return _value; };

        return ColumnView<T>( value_func, INFINITE );
    }

    // -------------------------------

   public:
    /// Transforms the ColumnView to a physical column.
    Column<T> to_column(
        const size_t _begin,
        const std::optional<size_t> _expected_length,
        const bool _nrows_must_match ) const;

    /// Transforms the ColumnView to a physical vector.
    std::shared_ptr<std::vector<T>> to_vector(
        const size_t _begin,
        const std::optional<size_t> _expected_length,
        const bool _nrows_must_match ) const;

    // -------------------------------

   public:
    /// Whether the column view is infinite.
    bool is_infinite() const
    {
        return std::holds_alternative<UnknownSize>( nrows() ) &&
               std::get<UnknownSize>( nrows() ) == INFINITE;
    }
    /// Accessor to data
    std::optional<T> operator[]( const size_t _i ) const
    {
        return value_func_( _i );
    }

    /// Trivial getter
    NRowsType nrows() const { return nrows_; }

    /// Trivial getter
    const std::string& unit() const { return unit_; }

    // -------------------------------

   private:
    /// Functiona returning the Number of rows (if that is knowable).
    const NRowsType nrows_;

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
    const size_t _begin,
    const std::optional<size_t> _expected_length,
    const bool _nrows_must_match ) const
{
    const auto data_ptr =
        to_vector( _begin, _expected_length, _nrows_must_match );

    auto col = Column<T>( data_ptr );

    col.set_unit( unit() );

    return col;
}

// -------------------------------------------------------------------------

template <class T>
std::shared_ptr<std::vector<T>> ColumnView<T>::to_vector(
    const size_t _begin,
    const std::optional<size_t> _expected_length,
    const bool _nrows_must_match ) const
{
    assert_true( _expected_length || !_nrows_must_match );

    const auto expected_length = _expected_length
                                     ? *_expected_length
                                     : std::numeric_limits<size_t>::max();

    const bool nrows_do_not_match =
        _nrows_must_match && std::holds_alternative<size_t>( nrows() ) &&
        std::get<size_t>( nrows() ) != expected_length;

    if ( nrows_do_not_match )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_length ) +
                " nrows, but got " +
                std::to_string( std::get<size_t>( nrows() ) ) + "." );
        }

    const auto data_ptr = std::make_shared<std::vector<T>>();

    for ( size_t i = 0; i < expected_length; ++i )
        {
            const auto val = value_func_( _begin + i );

            if ( !val )
                {
                    break;
                }

            data_ptr->push_back( *val );
        }

    const bool exceeds_expected_by_unknown_number =
        _nrows_must_match && std::holds_alternative<UnknownSize>( nrows() ) &&
        std::get<UnknownSize>( nrows() ) == NOT_KNOWABLE &&
        value_func_( _begin + expected_length );

    if ( exceeds_expected_by_unknown_number )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_length ) +
                " nrows, but there were more." );
        }

    if ( _nrows_must_match && data_ptr->size() != expected_length )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( expected_length ) +
                " nrows, but got " + std::to_string( data_ptr->size() ) + "." );
        }

    return data_ptr;
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_COLUMN_HPP_
