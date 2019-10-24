#ifndef RELBOOST_CONTAINERS_DATAFRAMEVIEW_HPP_
#define RELBOOST_CONTAINERS_DATAFRAMEVIEW_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrameView
{
    // ---------------------------------------------------------------------

   public:
    typedef relboost::containers::Column<Float> FloatColumnType;

    typedef relboost::containers::Column<Int> IntColumnType;

    // ---------------------------------------------------------------------

   public:
    DataFrameView(
        const DataFrame& _df,
        const std::shared_ptr<const std::vector<size_t>>& _rows )
        : df_( _df ), rows_( _rows )
    {
    }

    ~DataFrameView() = default;

    // ---------------------------------------------------------------------

   public:
    /// Getter for a categorical value.
    Int categorical( size_t _i, size_t _j ) const
    {
        return df_.categorical( row( _i ), _j );
    }

    /// Getter for a categorical column.
    const Column<Int> categorical_col( size_t _j ) const
    {
        return df_.categorical_col( _j );
    }

    /// Getter for a categorical name.
    const std::string& categorical_name( size_t _j ) const
    {
        return df_.categorical_name( _j );
    }

    /// Getter for a categorical name.
    const std::string& categorical_unit( size_t _j ) const
    {
        return df_.categorical_unit( _j );
    }

    /// Creates a subview.
    DataFrameView create_subview(
        const std::string& _name,
        const std::string& _join_key,
        const std::string& _time_stamp,
        const std::string& _upper_time_stamp ) const
    {
        return DataFrameView(
            df_.create_subview(
                _name, _join_key, _time_stamp, _upper_time_stamp ),
            rows_ );
    }

    /// Getter for underlying data frame.
    const DataFrame df() const { return df_; }

    /// Getter for a discrete value.
    Float discrete( size_t _i, size_t _j ) const
    {
        return df_.discrete( row( _i ), _j );
    }

    /// Getter for a discrete column.
    const Column<Float> discrete_col( size_t _j ) const
    {
        return df_.discrete_col( _j );
    }

    /// Getter for a discrete name.
    const std::string& discrete_name( size_t _j ) const
    {
        return df_.discrete_name( _j );
    }

    /// Getter for a discrete name.
    const std::string& discrete_unit( size_t _j ) const
    {
        return df_.discrete_unit( _j );
    }

    /// Getter for the indices (TODO: remove this).
    const std::vector<std::shared_ptr<containers::Index>>& indices() const
    {
        return df_.indices();
    }

    /// Getter for a join key.
    Int join_key( size_t _i ) const { return df_.join_key( row( _i ) ); }

    /// Getter for a join keys.
    const std::vector<Column<Int>>& join_keys() const
    {
        return df_.join_keys();
    }

    /// Getter for the join key name.
    const std::string& join_keys_name() const { return df_.join_keys_name(); }

    /// Return the name of the data frame.
    const std::string& name() const { return df_.name(); }

    /// Trivial getter
    size_t nrows() const
    {
        assert_true( rows_ );
        return rows_->size();
    }

    /// Trivial getter
    size_t num_categoricals() const { return df_.num_categoricals(); }

    /// Trivial getter
    size_t num_discretes() const { return df_.num_discretes(); }

    /// Trivial getter
    size_t num_join_keys() const { return df_.num_join_keys(); }

    /// Trivial getter
    size_t num_numericals() const { return df_.num_numericals(); }

    /// Trivial getter
    size_t num_targets() const { return df_.num_targets(); }

    /// Trivial getter
    size_t num_time_stamps() const { return df_.num_time_stamps(); }

    /// Getter for a numerical value.
    Float numerical( size_t _i, size_t _j ) const
    {
        return df_.numerical( row( _i ), _j );
    }

    /// Getter for a numerical column.
    const Column<Float> numerical_col( size_t _j ) const
    {
        return df_.numerical_col( _j );
    }

    /// Getter for a numerical name.
    const std::string& numerical_name( size_t _j ) const
    {
        return df_.numerical_name( _j );
    }

    /// Getter for a numerical name.
    const std::string& numerical_unit( size_t _j ) const
    {
        return df_.numerical_unit( _j );
    }

    /// Returns the indices of the rows that this view points to.
    const std::vector<size_t>& rows() const { return *rows_; }

    /// Returns the indices of the rows that this view points to.
    const std::shared_ptr<const std::vector<size_t>>& rows_ptr() const
    {
        return rows_;
    }

    /// Getter for a target value.
    Float target( size_t _i, size_t _j ) const
    {
        return df_.target( row( _i ), _j );
    }

    /// Getter for a target column.
    const Column<Float> target_col( size_t _j ) const
    {
        return df_.target_col( _j );
    }

    /// Getter for a target name.
    const std::string& target_name( size_t _j ) const
    {
        return df_.target_name( _j );
    }

    /// Getter for a target name.
    const std::string& target_unit( size_t _j ) const
    {
        return df_.target_unit( _j );
    }

    /// Trivial getter
    Float time_stamp( size_t _i ) const { return df_.time_stamp( row( _i ) ); }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        return df_.time_stamps_name();
    }

    /// Trivial getter
    Float upper_time_stamp( size_t _i ) const
    {
        return df_.upper_time_stamp( row( _i ) );
    }

    /// Getter for the time stamps name.
    const std::string& upper_time_stamps_name() const
    {
        return df_.upper_time_stamps_name();
    }

    // ---------------------------------------------------------------------

   private:
    /// Transforms the index.
    const size_t row( size_t _i ) const
    {
        assert_true( rows_ );
        assert_true( _i < rows_->size() );
        return ( *rows_ )[_i];
    }

    // ---------------------------------------------------------------------

   private:
    /// The underlying data frame.
    const DataFrame df_;

    /// The rows that become part of this view.
    const std::shared_ptr<const std::vector<size_t>> rows_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_DATAFRAMEVIEW_HPP_
