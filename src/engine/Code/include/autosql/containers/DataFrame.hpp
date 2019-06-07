#ifndef AUTOSQL_CONTAINERS_DATAFRAME_HPP_
#define AUTOSQL_CONTAINERS_DATAFRAME_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrame
{
    // ---------------------------------------------------------------------

   public:
    typedef autosql::containers::Column<AUTOSQL_FLOAT> FloatColumnType;

    typedef autosql::containers::Column<AUTOSQL_INT> IntColumnType;

    // ---------------------------------------------------------------------

   public:
    DataFrame(
        const std::vector<Column<AUTOSQL_INT>>& _categoricals,
        const std::vector<Column<AUTOSQL_FLOAT>>& _discretes,
        const std::vector<std::shared_ptr<AUTOSQL_INDEX>>& _indices,
        const std::vector<Column<AUTOSQL_INT>>& _join_keys,
        const std::string& _name,
        const std::vector<Column<AUTOSQL_FLOAT>>& _numericals,
        const std::vector<Column<AUTOSQL_FLOAT>>& _targets,
        const std::vector<Column<AUTOSQL_FLOAT>>& _time_stamps );

    DataFrame(
        const std::vector<Column<AUTOSQL_INT>>& _categoricals,
        const std::vector<Column<AUTOSQL_FLOAT>>& _discretes,
        const std::vector<Column<AUTOSQL_INT>>& _join_keys,
        const std::string& _name,
        const std::vector<Column<AUTOSQL_FLOAT>>& _numericals,
        const std::vector<Column<AUTOSQL_FLOAT>>& _targets,
        const std::vector<Column<AUTOSQL_FLOAT>>& _time_stamps );

    ~DataFrame() = default;

    // ---------------------------------------------------------------------

   public:
    /// Creates a subview.
    DataFrame create_subview(
        const std::string& _name,
        const std::string& _join_key,
        const std::string& _time_stamp,
        const std::string& _upper_time_stamp ) const;

    // ---------------------------------------------------------------------

   public:
    /// Getter for a categorical value.
    AUTOSQL_INT categorical( size_t _i, size_t _j ) const
    {
        assert( _j < categoricals_.size() );
        return categoricals_[_j][_i];
    }

    /// Getter for a categorical column.
    const Column<AUTOSQL_INT> categorical_col( size_t _j ) const
    {
        assert( _j < categoricals_.size() );
        return categoricals_[_j];
    }

    /// Getter for a categorical name.
    const std::string& categorical_name( size_t _j ) const
    {
        assert( _j < categoricals_.size() );
        return categoricals_[_j].name_;
    }

    /// Getter for a categorical name.
    const std::string& categorical_unit( size_t _j ) const
    {
        assert( _j < categoricals_.size() );
        return categoricals_[_j].unit_;
    }

    /// Getter for a discrete value.
    AUTOSQL_FLOAT discrete( size_t _i, size_t _j ) const
    {
        assert( _j < discretes_.size() );
        return discretes_[_j][_i];
    }

    /// Getter for a discrete column.
    const Column<AUTOSQL_FLOAT> discrete_col( size_t _j ) const
    {
        assert( _j < discretes_.size() );
        return discretes_[_j];
    }

    /// Getter for a discrete name.
    const std::string& discrete_name( size_t _j ) const
    {
        assert( _j < discretes_.size() );
        return discretes_[_j].name_;
    }

    /// Getter for a discrete name.
    const std::string& discrete_unit( size_t _j ) const
    {
        assert( _j < discretes_.size() );
        return discretes_[_j].unit_;
    }

    /// Find the indices associated with this join key.
    AUTOSQL_INDEX::const_iterator find( const AUTOSQL_INT _join_key ) const
    {
        assert( indices().size() > 0 );
        return indices_[0]->find( _join_key );
    }

    /// Whether a certain join key is included in the indices.
    bool has( const AUTOSQL_INT _join_key ) const
    {
        assert( indices().size() > 0 );
        return indices_[0]->find( _join_key ) != indices_[0]->end();
    }

    /// Getter for the indices (TODO: make this private).
    const std::vector<std::shared_ptr<AUTOSQL_INDEX>>& indices() const
    {
        return indices_;
    }

    /// Getter for a join key.
    AUTOSQL_INT join_key( size_t _i ) const
    {
        assert( join_keys_.size() == 1 );

        return join_keys_[0][_i];
    }

    /// Getter for a join keys.
    const std::vector<Column<AUTOSQL_INT>>& join_keys() const
    {
        return join_keys_;
    }

    /// Getter for the join key name.
    const std::string& join_keys_name() const
    {
        assert( join_keys_.size() == 1 );

        return join_keys_[0].name_;
    }

    /// Return the name of the data frame.
    const std::string& name() const { return name_; }

    /// Trivial getter
    size_t nrows() const
    {
        assert( join_keys_.size() > 0 );
        return join_keys_[0].nrows_;
    }

    /// Trivial getter
    size_t num_categoricals() const { return categoricals_.size(); }

    /// Trivial getter
    size_t num_discretes() const { return discretes_.size(); }

    /// Trivial getter
    size_t num_join_keys() const { return join_keys_.size(); }

    /// Trivial getter
    size_t num_numericals() const { return numericals_.size(); }

    /// Trivial getter
    size_t num_targets() const { return targets_.size(); }

    /// Trivial getter
    size_t num_time_stamps() const { return time_stamps_.size(); }

    /// Getter for a numerical value.
    AUTOSQL_FLOAT numerical( size_t _i, size_t _j ) const
    {
        assert( _j < numericals_.size() );
        return numericals_[_j][_i];
    }

    /// Getter for a numerical column.
    const Column<AUTOSQL_FLOAT> numerical_col( size_t _j ) const
    {
        assert( _j < numericals_.size() );
        return numericals_[_j];
    }

    /// Getter for a numerical name.
    const std::string& numerical_name( size_t _j ) const
    {
        assert( _j < numericals_.size() );
        return numericals_[_j].name_;
    }

    /// Getter for a numerical name.
    const std::string& numerical_unit( size_t _j ) const
    {
        assert( _j < numericals_.size() );
        return numericals_[_j].unit_;
    }

    /// Getter for a target value.
    AUTOSQL_FLOAT target( size_t _i, size_t _j ) const
    {
        assert( _j < targets_.size() );
        return targets_[_j][_i];
    }

    /// Getter for a target column.
    const Column<AUTOSQL_FLOAT> target_col( size_t _j ) const
    {
        assert( _j < targets_.size() );
        return targets_[_j];
    }

    /// Getter for a target name.
    const std::string& target_name( size_t _j ) const
    {
        assert( _j < targets_.size() );
        return targets_[_j].name_;
    }

    /// Getter for a target name.
    const std::string& target_unit( size_t _j ) const
    {
        assert( _j < targets_.size() );
        return targets_[_j].unit_;
    }

    /// Trivial getter
    AUTOSQL_FLOAT time_stamp( size_t _i ) const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );
        assert( _i < time_stamps_[0].nrows_ );

        return time_stamps_[0][_i];
    }

    /// Getter for the time stamps column.
    const Column<AUTOSQL_FLOAT> time_stamp_col() const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );
        return time_stamps_[0];
    }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );

        return time_stamps_[0].name_;
    }

    /// Returns the schema.
    Schema to_schema() const
    {
        return Schema(
            get_colnames( categoricals_ ),
            get_colnames( discretes_ ),
            get_colnames( join_keys_ ),
            name_,
            get_colnames( numericals_ ),
            get_colnames( targets_ ),
            get_colnames( time_stamps_ ) );
    }

    /// Trivial getter
    AUTOSQL_FLOAT upper_time_stamp( size_t _i ) const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );

        if ( time_stamps_.size() == 1 )
            {
                return NAN;
            }

        assert( _i < time_stamps_[1].nrows_ );

        return time_stamps_[1][_i];
    }

    /// Getter for the time stamps name.
    const std::string& upper_time_stamps_name() const
    {
        assert( time_stamps_.size() == 2 );

        return time_stamps_[1].name_;
    }

    // ---------------------------------------------------------------------

   private:
    /// Creates the indices for this data frame
    static std::vector<std::shared_ptr<AUTOSQL_INDEX>> create_indices(
        const std::vector<Column<AUTOSQL_INT>>& _join_keys );

    /// Helper class that extracts the column names.
    template <typename T>
    std::vector<std::string> get_colnames(
        const std::vector<Column<T>>& _columns ) const;

    // ---------------------------------------------------------------------

   private:
    /// Pointer to categorical columns.
    const std::vector<Column<AUTOSQL_INT>> categoricals_;

    /// Pointer to discrete columns.
    const std::vector<Column<AUTOSQL_FLOAT>> discretes_;

    /// Indices assiciated with join keys.
    const std::vector<std::shared_ptr<AUTOSQL_INDEX>> indices_;

    /// Join keys of this data frame.
    const std::vector<Column<AUTOSQL_INT>> join_keys_;

    /// Name of the data frame.
    const std::string name_;

    /// Pointer to numerical columns.
    const std::vector<Column<AUTOSQL_FLOAT>> numericals_;

    /// Pointer to target column.
    const std::vector<Column<AUTOSQL_FLOAT>> targets_;

    /// Time stamps of this data frame.
    const std::vector<Column<AUTOSQL_FLOAT>> time_stamps_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

template <typename T>
std::vector<std::string> DataFrame::get_colnames(
    const std::vector<Column<T>>& _columns ) const
{
    std::vector<std::string> colnames;

    for ( auto& col : _columns )
        {
            colnames.push_back( col.name_ );
        }

    return colnames;
}

// ----------------------------------------------------------------------------

}  // namespace containers
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_CONTAINERS_DATAFRAME_HPP_
