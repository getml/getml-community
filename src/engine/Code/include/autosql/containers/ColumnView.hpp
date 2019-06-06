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

    ColumnView( const Column<T>& _col ) { col_.reset( new Column<T>( _col ) ); }

    ColumnView(
        const Column<T>& _col,
        const std::shared_ptr<const ContainerType>& _indices )
        : indices_( _indices )
    {
        col_.reset( new Column<T>( _col ) );
    }

    ~ColumnView() = default;

    // -------------------------------

    /// Deletes all data in the view
    void clear() { *this = ColumnView(); }

    /// Whether or not the column view is empty.
    operator bool() const { return ( col_ && true ); }

    /// Accessor to data (when indices are std::vector<AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<AUTOSQL_INT>>::value,
            int>::type = 0>
    inline T& operator()( const AUTOSQL_INT _i )
    {
        assert( _i >= 0 );
        assert( _i < static_cast<AUTOSQL_INT>( indices_->size() ) );
        return ( *col_ )[( *indices_ )[_i]];
    }

    /// Accessor to data (when indices are std::vector<AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<AUTOSQL_INT>>::value,
            int>::type = 0>
    inline T operator()( const AUTOSQL_INT _i ) const
    {
        assert( _i >= 0 );
        assert( _i < static_cast<AUTOSQL_INT>( indices_->size() ) );
        return ( *col_ )[( *indices_ )[_i]];
    }

    /// Accessor to data (when indices are std::map<AUTOSQL_INT, AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<AUTOSQL_INT, AUTOSQL_INT>>::value,
            int>::type = 0>
    inline T& operator()( const AUTOSQL_INT _i )
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return ( *col_ )[it->second];
    }

    /// Accessor to data (when indices are std::map<AUTOSQL_INT, AUTOSQL_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<AUTOSQL_INT, AUTOSQL_INT>>::value,
            int>::type = 0>
    inline T operator()( const AUTOSQL_INT _i ) const
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return ( *col_ )[it->second];
    }

    /// Accessor to data
    inline T& operator[]( const AUTOSQL_INT _i )
    {
        assert( !indices_ );
        return ( *col_ )[_i];
    }

    /// Accessor to data
    inline T operator[]( const AUTOSQL_INT _i ) const
    {
        assert( !indices_ );
        return ( *col_ )[_i];
    }

    // -------------------------------

   private:
    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const ContainerType> indices_;

    /// Shallow copy of the matrix in which we are interested
    Optional<Column<T>> col_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_COLUMNVIEW_HPP_
