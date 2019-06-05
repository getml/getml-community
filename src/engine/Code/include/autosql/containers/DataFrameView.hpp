#ifndef AUTOSQL_CONTAINER_DATAFRAMEVIEW_HPP_
#define AUTOSQL_CONTAINER_DATAFRAMEVIEW_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrameView
{
   public:
    DataFrameView()
        : df_( DataFrame(
              std::make_shared<containers::Encoding>(),
              std::make_shared<containers::Encoding>() ) ),
          indices_( std::make_shared<const std::vector<AUTOSQL_INT>>( 0 ) )
    {
    }

    explicit DataFrameView( const DataFrame &_df )
        : df_( _df ),
          indices_( std::make_shared<const std::vector<AUTOSQL_INT>>( 0 ) )
    {
    }

    DataFrameView(
        const DataFrame &_df,
        const std::shared_ptr<const std::vector<AUTOSQL_INT>> &_indices )
        : df_( _df ), indices_( _indices )
    {
    }

    template <class ContainerType>
    DataFrameView( const DataFrame &_df, const ContainerType &_indices )
        : df_( _df ),
          indices_( std::make_shared<const std::vector<AUTOSQL_INT>>(
              _indices.begin(), _indices.end() ) )
    {
    }

    ~DataFrameView() = default;

    // -------------------------------

    /// Clean up all the data
    void clear()
    {
        df_.clear();
        indices_ = std::make_shared<const std::vector<AUTOSQL_INT>>( 0 );
    }

    /// Trivial accessor
    DataFrame &df() { return df_; }

    /// Trivial accessor
    const DataFrame &df() const { return df_; }

    /// Trivial getter
    inline AUTOSQL_INT categorical(
        const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.categorical()( indices()[_i], _j );
    }

    /// Creates a view on categorical
    inline ColumnView<AUTOSQL_INT, std::vector<AUTOSQL_INT>> categorical_column(
        const AUTOSQL_INT _column_used ) const
    {
        return ColumnView<AUTOSQL_INT, std::vector<AUTOSQL_INT>>(
            df_.categorical(), indices_, _column_used );
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT discrete(
        const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.discrete()( indices()[_i], _j );
    }

    /// Creates a view on discrete
    inline ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>> discrete_column(
        const AUTOSQL_INT _column_used ) const
    {
        return ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>(
            df_.discrete(), indices_, _column_used );
    }

    /// Trivial getter
    inline const std::shared_ptr<const std::vector<AUTOSQL_INT>> &get_indices()
        const
    {
        return indices_;
    }

    /// Trivial getter
    inline AUTOSQL_INT join_key( const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.join_key( _j )[indices()[_i]];
    }

    /// Trivial getter
    inline AUTOSQL_INT join_key( const AUTOSQL_INT _i ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.join_key()[indices()[_i]];
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT numerical(
        const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.numerical()( indices()[_i], _j );
    }

    /// Creates a view on numerical
    inline ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>> numerical_column(
        const AUTOSQL_INT _column_used ) const
    {
        return ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>(
            df_.numerical(), indices_, _column_used );
    }

    /// Returns the number of rows of the view (as opposed to the underlying
    /// matrix)
    inline AUTOSQL_INT nrows() const
    {
        return static_cast<AUTOSQL_INT>( indices().size() );
    }

    /// Trivial setter
    void set_indices(
        const std::shared_ptr<const std::vector<AUTOSQL_INT>> &_indices )
    {
        indices_ = _indices;
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT targets(
        const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.targets()( indices()[_i], _j );
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT time_stamp(
        const AUTOSQL_INT _i, const AUTOSQL_INT _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.time_stamps( _j )[indices()[_i]];
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT time_stamp( const AUTOSQL_INT _i ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return df_.time_stamps()[indices()[_i]];
    }

    /// Creates a view on time stamps
    template <typename T>
    inline ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>> time_stamps_column(
        const T _column_used ) const
    {
        return ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>(
            df_.time_stamps( _column_used ), indices_, 0 );
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT upper_time_stamp( const AUTOSQL_INT _i ) const
    {
        const auto ptr = df_.upper_time_stamps();

        if ( ptr == nullptr )
            {
                return NAN;
            }

        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < indices().size() );

        return ( *ptr )[indices()[_i]];
    }

    // -------------------------------

   private:
    /// Trivial accessor
    const std::vector<AUTOSQL_INT> &indices() const { return *indices_; }

    // -------------------------------

   private:
    /// Shallow copy of the data frame pointed to
    DataFrame df_;

    /// Indices indicating all of the rows that are part of this view
    std::shared_ptr<const std::vector<AUTOSQL_INT>> indices_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_DATAFRAME_HPP_
