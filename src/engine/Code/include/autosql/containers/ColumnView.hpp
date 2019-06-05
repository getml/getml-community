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
   public:
    ColumnView() : column_used_( -1 ) {}

    ColumnView( const Matrix<T>& _mat, const SQLNET_INT _column_used )
        : column_used_( _column_used ), mat_( _mat )
    {
        assert( column_used_ >= 0 );
        assert( column_used_ < mat_.ncols() );
    }

    ColumnView(
        const Matrix<T>& _mat,
        const std::shared_ptr<const ContainerType>& _indices,
        const SQLNET_INT _column_used )
        : column_used_( _column_used ), indices_( _indices ), mat_( _mat )
    {
        assert( column_used_ >= 0 );
        assert( column_used_ < mat_.ncols() );
    }

    ~ColumnView() = default;

    // -------------------------------

    /// Deletes all data in the view
    void clear() { *this = ColumnView(); }

    /// Whether or not the column view is empty.
    operator bool() const { return mat_.nrows() > 0; }

    /// Accessor to data (when indices are std::vector<SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<SQLNET_INT>>::value,
            int>::type = 0>
    inline T& operator()( const SQLNET_INT _i )
    {
        assert( _i >= 0 );
        assert( _i < static_cast<SQLNET_INT>( indices_->size() ) );
        return mat_( ( *indices_ )[_i], column_used_ );
    }

    /// Accessor to data (when indices are std::vector<SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<SQLNET_INT>>::value,
            int>::type = 0>
    inline T operator()( const SQLNET_INT _i ) const
    {
        assert( _i >= 0 );
        assert( _i < static_cast<SQLNET_INT>( indices_->size() ) );
        return mat_( ( *indices_ )[_i], column_used_ );
    }

    /// Accessor to data (when indices are std::map<SQLNET_INT, SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<SQLNET_INT, SQLNET_INT>>::value,
            int>::type = 0>
    inline T& operator()( const SQLNET_INT _i )
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return mat_( it->second, column_used_ );
    }

    /// Accessor to data (when indices are std::map<SQLNET_INT, SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<SQLNET_INT, SQLNET_INT>>::value,
            int>::type = 0>
    inline T operator()( const SQLNET_INT _i ) const
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return mat_( it->second, column_used_ );
    }

    /// Accessor to data
    inline T& operator[]( const SQLNET_INT _i )
    {
        assert( column_used_ >= 0 );
        assert( !indices_ );
        return mat_( _i, column_used_ );
    }

    /// Accessor to data
    inline T operator[]( const SQLNET_INT _i ) const
    {
        assert( column_used_ >= 0 );
        assert( !indices_ );
        return mat_( _i, column_used_ );
    }

    // -------------------------------

   private:
    /// Indicates which column we want to use
    SQLNET_INT column_used_;

    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const ContainerType> indices_;

    /// Shallow copy of the matrix in which we are interested
    Matrix<T> mat_;
};

// -------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_CONTAINER_COLUMNVIEW_HPP_
