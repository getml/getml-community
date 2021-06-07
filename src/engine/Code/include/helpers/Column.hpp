#ifndef HELPERS_COLUMN_HPP_
#define HELPERS_COLUMN_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

template <typename T>
struct Column
{
    // ---------------------------------------------------------------------

    typedef const T* iterator;

    // ---------------------------------------------------------------------

    Column(
        const std::shared_ptr<const std::vector<T>> _ptr,
        const std::string& _name,
        const std::vector<Subrole>& _subroles,
        const std::string& _unit )
        : data_( _ptr ? _ptr->data() : nullptr ),
          name_( _name ),
          nrows_( _ptr ? _ptr->size() : static_cast<size_t>( 0 ) ),
          ptr_( _ptr ),
          subroles_( _subroles ),
          unit_( _unit )
    {
        assert_true( ptr_ );
    }

    ~Column() = default;

    // ---------------------------------------------------------------------

    /// Iterator begin
    iterator begin() const { return data_; }

    /// Iterator end
    iterator end() const { return data_ + nrows_; }

    /// Access operator
    const T& operator[]( size_t _i ) const
    {
        assert_msg(
            _i < nrows_,
            "_i: " + std::to_string( _i ) +
                ", nrows_: " + std::to_string( nrows_ ) );

        return *( data_ + _i );
    }

    // ---------------------------------------------------------------------

    /// Pointer to the underlying data.
    const T* const data_;

    /// Name of the column
    const std::string name_;

    /// Number of rows
    const size_t nrows_;

    /// The pointer to take ownership of the underlying data.
    const std::shared_ptr<const std::vector<T>> ptr_;

    /// Subroles of the column
    const std::vector<Subrole> subroles_;

    /// Unit of the column
    const std::string unit_;

    // ---------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_COLUMN_HPP_
