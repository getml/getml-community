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
    typedef relboost::containers::Column<RELBOOST_FLOAT> FloatColumnType;

    typedef relboost::containers::Column<RELBOOST_INT> IntColumnType;

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
    RELBOOST_INT categorical( size_t _i, size_t _j ) const
    {
        return df_.categorical( row( _i ), _j );
    }

    /// Getter for a categorical column.
    const Column<RELBOOST_INT> categorical_col( size_t _j ) const
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

    /// Getter for a discrete value.
    RELBOOST_FLOAT discrete( size_t _i, size_t _j ) const
    {
        return df_.discrete( row( _i ), _j );
    }

    /// Getter for a discrete column.
    const Column<RELBOOST_FLOAT> discrete_col( size_t _j ) const
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

    /// Getter for the indices.
    const std::vector<std::shared_ptr<RELBOOST_INDEX>>& indices() const
    {
        return df_.indices();
    }

    /// Getter for a join key.
    RELBOOST_INT join_key( size_t _i ) const
    {
        return df_.join_key( row( _i ) );
    }

    /// Getter for a join keys.
    const std::vector<Column<RELBOOST_INT>>& join_keys() const
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
        assert( rows_ );
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
    RELBOOST_FLOAT numerical( size_t _i, size_t _j ) const
    {
        return df_.numerical( row( _i ), _j );
    }

    /// Getter for a numerical column.
    const Column<RELBOOST_FLOAT> numerical_col( size_t _j ) const
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

    /// Getter for a target value.
    RELBOOST_FLOAT target( size_t _i, size_t _j ) const
    {
        return df_.target( row( _i ), _j );
    }

    /// Getter for a target column.
    const Column<RELBOOST_FLOAT> target_col( size_t _j ) const
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
    RELBOOST_FLOAT time_stamp( size_t _i ) const
    {
        return df_.time_stamp( row( _i ) );
    }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        return df_.time_stamps_name();
    }

    /// Trivial getter
    RELBOOST_FLOAT upper_time_stamp( size_t _i ) const
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
        assert( rows_ );
        assert( _i < rows_->size() );
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
