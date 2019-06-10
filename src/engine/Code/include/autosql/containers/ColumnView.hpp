#ifndef AUTOSQL_CONTAINER_COLUMNVIEW_HPP_
#define AUTOSQL_CONTAINER_COLUMNVIEW_HPP_

namespace autosql
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
        : rows_( std::shared_ptr<const ContainerType>() )
    {
        col_.reset( new Column<T>( _col ) );
    }

    ColumnView(
        const Column<T>& _col,
        const std::shared_ptr<const ContainerType>& _rows )
        : rows_( _rows )
    {
        col_.reset( new Column<T>( _col ) );
    }

    ~ColumnView() = default;

    // -------------------------------

    /// Deletes all data in the view
    void clear() { *this = ColumnView(); }

    /// Returns the underlying column.
    const Column<T>& col() const
    {
        assert( col_ );
        assert( !rows_ );
        return *col_;
    }

    /// Whether or not the column view is empty.
    operator bool() const { return ( col_ && true ); }

    /// Accessor to data (when rows are std::vector<AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<size_t>>::value,
            int>::type = 0>
    inline T operator[]( const size_t _i ) const
    {
        assert( _i < rows_->size() );
        return ( *col_ )[( *rows_ )[_i]];
    }

    /// Accessor to data (when rows are std::map<AUTOSQL_INT, AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<AUTOSQL_INT, AUTOSQL_INT>>::value,
            int>::type = 0>
    inline T operator[]( const AUTOSQL_INT _i ) const
    {
        assert( _i >= 0 );
        auto it = rows_->find( _i );
        assert( it != rows_->end() );
        return ( *col_ )[it->second];
    }

    // -------------------------------

   private:
    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const ContainerType> rows_;

    /// Shallow copy of the column in which we are interested.
    Optional<Column<T>> col_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_COLUMNVIEW_HPP_
