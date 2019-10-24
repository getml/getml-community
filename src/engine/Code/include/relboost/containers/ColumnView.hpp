#ifndef RELBOOST_CONTAINER_COLUMNVIEW_HPP_
#define RELBOOST_CONTAINER_COLUMNVIEW_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T, class ContainerType>
class ColumnView
{
    // -------------------------------

   public:
    ColumnView() {}

    ColumnView( const Column<T>& _col )
        : col_( std::make_optional<Column<T>>( _col ) ),
          rows_( std::shared_ptr<const ContainerType>() )
    {
    }

    ColumnView(
        const Column<T>& _col,
        const std::shared_ptr<const ContainerType>& _rows )
        : col_( std::make_optional<Column<T>>( _col ) ), rows_( _rows )
    {
    }

    ~ColumnView() = default;

    // -------------------------------

    /// Deletes all data in the view
    void clear() { *this = ColumnView(); }

    /// Returns the underlying column.
    const Column<T>& col() const
    {
        assert_true( col_ );
        return *col_;
    }

    /// Whether or not the column view is empty.
    operator bool() const { return ( col_ && true ); }

    /// Accessor to data (when rows are std::vector<Int>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<size_t>>::value,
            int>::type = 0>
    inline T operator[]( const size_t _i ) const
    {
        assert_true( _i < rows_->size() );
        return ( *col_ )[( *rows_ )[_i]];
    }

    /// Accessor to data (when rows are std::map<Int, Int>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<Int, Int>>::value,
            int>::type = 0>
    inline T operator[]( const Int _i ) const
    {
        assert_true( _i >= 0 );
        auto it = rows_->find( _i );
        assert_true( it != rows_->end() );
        return ( *col_ )[it->second];
    }

    // -------------------------------

   private:
    /// Shallow copy of the column in which we are interested.
    std::optional<Column<T>> col_;

    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const ContainerType> rows_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINER_COLUMNVIEW_HPP_
