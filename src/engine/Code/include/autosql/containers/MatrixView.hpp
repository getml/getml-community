#ifndef AUTOSQL_CONTAINER_MATRIXVIEW_HPP_
#define AUTOSQL_CONTAINER_MATRIXVIEW_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T, class ContainerType>
class MatrixView
{
   public:
    MatrixView() {}

    MatrixView(
        const Matrix<T>& _mat,
        const std::shared_ptr<const ContainerType>& _indices )
        : indices_( _indices ), mat_( _mat )
    {
    }

    ~MatrixView() = default;

    // -------------------------------

    /// Deletes all data in the view
    void clear() { *this = MatrixView(); }

    /// Returns a column view on this MatrixView
    inline ColumnView<T, ContainerType> column_view(
        const SQLNET_INT _column_used )
    {
        assert( _column_used >= 0 );
        assert( _column_used < mat_.ncols() );
        return ColumnView<T, ContainerType>( mat_, indices_, _column_used );
    }

    /// Returns the underlying matrix object
    Matrix<T>& mat() { return mat_; }

    /// Returns the number of rows of the view (as opposed to the underlying
    /// matrix)
    inline SQLNET_INT ncols() const { return mat_.ncols(); }

    /// Returns the number of rows of the view (as opposed to the underlying
    /// matrix)
    inline SQLNET_INT nrows() const
    {
        return static_cast<SQLNET_INT>( indices_->size() );
    }

    /// Whether or not the matrix view is empty.
    operator bool() const { return mat_.nrows() > 0; }

    /// Accessor to data (when indices are std::vector<SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<SQLNET_INT>>::value,
            int>::type = 0>
    inline T& operator()( const SQLNET_INT _i, const SQLNET_INT _j )
    {
        assert( _i >= 0 );
        assert( _i < static_cast<SQLNET_INT>( indices_->size() ) );
        return mat_( ( *indices_ )[_i], _j );
    }

    /// Accessor to data (when indices are std::vector<SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::vector<SQLNET_INT>>::value,
            int>::type = 0>
    inline T operator()( const SQLNET_INT _i, const SQLNET_INT _j ) const
    {
        assert( _i >= 0 );
        assert( _i < static_cast<SQLNET_INT>( indices_->size() ) );
        return mat_( ( *indices_ )[_i], _j );
    }

    /// Accessor to data (when indices are std::map<SQLNET_INT, SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<SQLNET_INT, SQLNET_INT>>::value,
            int>::type = 0>
    inline T& operator()( const SQLNET_INT _i, const SQLNET_INT _j )
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return mat_( it->second, _j );
    }

    /// Accessor to data (when indices are std::map<SQLNET_INT, SQLNET_INT>)
    template <
        typename CType = ContainerType,
        typename std::enable_if<
            std::is_same<CType, std::map<SQLNET_INT, SQLNET_INT>>::value,
            int>::type = 0>
    inline T operator()( const SQLNET_INT _i, const SQLNET_INT _j ) const
    {
        assert( _i >= 0 );
        auto it = indices_->find( _i );
        assert( it != indices_->end() );
        return mat_( it->second, _j );
    }

    // -------------------------------

   private:
    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const ContainerType> indices_;

    /// Shallow copy of the matrix in which we are interested
    Matrix<T> mat_;
};

// -------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_CONTAINER_MATRIXVIEW_HPP_
