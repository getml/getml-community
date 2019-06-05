#ifndef AUTOSQL_DECISIONTREES_DECISIONTREEIMPL_HPP_
#define AUTOSQL_DECISIONTREES_DECISIONTREEIMPL_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

struct DecisionTreeImpl
{
    // ----------------------------------------

    DecisionTreeImpl() : comm_( nullptr ), random_number_generator_( nullptr )
    {
    }

    ~DecisionTreeImpl() = default;

    // ----------------------------------------

    /// Trivial accessor
    inline const containers::Encoding& categories() const
    {
        assert( categories_ );
        return *categories_.get();
    }

    /// Clears up the memory
    inline void clear()
    {
        aggregation_->clear();
        aggregation_->clear_extras();
        peripheral_.clear();
        population_.clear();
        subfeatures_.clear();
    }

    /// Trivial getter
    inline AUTOSQL_INT ix_perip_used() const
    {
        return column_to_be_aggregated_.ix_perip_used;
    }

    /// Returns a custom random number generator
    inline RandomNumberGenerator rng() const
    {
        assert( random_number_generator_ != nullptr );
        return RandomNumberGenerator( random_number_generator_, comm_ );
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_categorical() const
    {
        assert( same_units_.same_units_categorical );
        return *same_units_.same_units_categorical;
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_discrete() const
    {
        assert( same_units_.same_units_discrete );
        return *same_units_.same_units_discrete;
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_numerical() const
    {
        assert( same_units_.same_units_numerical );
        return *same_units_.same_units_numerical;
    }

    /// Trivial setter
    inline void set_same_units( const descriptors::SameUnits& _same_units )
    {
        same_units_ = _same_units;
    }

    /// Trivial accessor
    inline containers::
        MatrixView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>&
        subfeatures()
    {
        return subfeatures_;
    }

    /// Trivial accessor
    inline const containers::
        MatrixView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>&
        subfeatures() const
    {
        return subfeatures_;
    }

    /// Trivial accessor
    inline const std::string& x_perip_categorical_colname( AUTOSQL_INT _i ) const
    {
        assert( x_perip_categorical_colnames_ );
        return x_perip_categorical_colnames_.get()[0][_i];
    }

    /// Trivial accessor
    inline const std::string& x_perip_numerical_colname( AUTOSQL_INT _i ) const
    {
        assert( x_perip_numerical_colnames_ );
        return x_perip_numerical_colnames_.get()[0][_i];
    }

    /// Trivial accessor
    inline const std::string& x_perip_discrete_colname( AUTOSQL_INT _i ) const
    {
        assert( x_perip_discrete_colnames_ );
        return x_perip_discrete_colnames_.get()[0][_i];
    }

    /// Trivial accessor
    inline const std::string& x_popul_categorical_colname( AUTOSQL_INT _i ) const
    {
        assert( x_popul_categorical_colnames_ );
        return x_popul_categorical_colnames_.get()[0][_i];
    }

    /// Trivial accessor
    inline const std::string& x_popul_numerical_colname( AUTOSQL_INT _i ) const
    {
        assert( x_popul_numerical_colnames_ );
        return x_popul_numerical_colnames_.get()[0][_i];
    }

    /// Trivial accessor
    inline const std::string& x_popul_discrete_colname( AUTOSQL_INT _i ) const
    {
        assert( x_popul_discrete_colnames_ );
        return x_popul_discrete_colnames_.get()[0][_i];
    }

    // ------------------------------------------------------------

    /// Returns the colname corresponding to _data used and
    /// _ix_column_used
    std::string get_colname(
        const std::string& _feature_num,
        const DataUsed _data_used,
        const AUTOSQL_INT _ix_column_used,
        const bool _equals = true ) const;

    /// Updates the map for the importance ranking
    void source_importances(
        const DataUsed _data_used,
        const AUTOSQL_INT _ix_column_used,
        const AUTOSQL_FLOAT _factor,
        std::map<descriptors::SourceImportancesColumn, AUTOSQL_FLOAT>& _map )
        const;

    // ------------------------------------------------------------

    /// The aggregation is what connects the peripheral table
    /// to the population table and thus the targets.
    std::shared_ptr<aggregations::AbstractAggregation> aggregation_;

    /// Whether we want to summarize our categorical features in sets.
    bool allow_sets_;

    /// Type of the aggregation used (needed for copy constructor)
    std::string aggregation_type_;

    /// Pointer to the vector that maps the integers to categories
    std::shared_ptr<containers::Encoding> categories_;

    /// Pointer to the structure that contains information
    /// about the column aggregated by this tree
    ColumnToBeAggregated column_to_be_aggregated_;

    /// Communicator (either MPI or self-defined communicator)
    AUTOSQL_COMMUNICATOR* comm_;

    /// Name of the join key in the peripheral table
    std::string join_keys_perip_name_;

    /// Name of the join key in the population table
    std::string join_keys_popul_name_;

    /// Factor that is proportional to the number of splits
    /// can be set by the user
    AUTOSQL_FLOAT grid_factor_;

    /// Maximum depth allowed
    AUTOSQL_INT max_length_;

    /// Minimum number of samples required in each part of
    /// a split
    AUTOSQL_INT min_num_samples_;

    /// The optimization criterion is what we want to maximize -
    /// using it we can determine the optimal splits
    optimizationcriteria::OptimizationCriterion* optimization_criterion_;

    /// The peripheral table used for fitting/transformation - note
    /// that a DecisionTree can only have one peripheral table!
    containers::DataFrame peripheral_;

    /// Name of the peripheral table
    std::string peripheral_name_;

    /// The population table used for fitting/transformation
    containers::DataFrameView population_;

    /// Name of the population table
    std::string population_name_;

    /// Random number generator
    std::mt19937* random_number_generator_;

    /// Regularization - the higher the factor the less
    /// complex the trees. This is implemented by requiring
    /// that the new optimization_criterion.value() must be at least
    /// old optimization_criterion.value() + regularization_.
    AUTOSQL_FLOAT regularization_;

    /// Contains information on which of the columns contain the same units
    descriptors::SameUnits same_units_;

    /// Contains the subfeatures, which may or may not exist.
    containers::MatrixView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>
        subfeatures_;

    /// The share of conditions randomly selected
    AUTOSQL_FLOAT share_conditions_;

    /// Name of the time stamps in the peripheral table
    std::string time_stamps_perip_name_;

    /// Name of the time stamps in the population table
    std::string time_stamps_popul_name_;

    /// Name of the time stamps in the peripheral table
    /// used for defining the upper limit.
    std::string upper_time_stamps_name_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_perip_categorical_colnames_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_perip_discrete_colnames_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_perip_numerical_colnames_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_popul_categorical_colnames_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_popul_numerical_colnames_;

    /// Associated column names
    std::shared_ptr<std::vector<std::string>> x_popul_discrete_colnames_;
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
#endif  // AUTOSQL_DECISIONTREES_DECISIONTREEIMPL_HPP_
