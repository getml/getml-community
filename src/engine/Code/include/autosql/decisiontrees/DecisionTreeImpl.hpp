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

    DecisionTreeImpl(
        const std::shared_ptr<const std::vector<std::string>>& _categories,
        const std::shared_ptr<const descriptors::TreeHyperparameters>&
            _tree_hyperparameters )
        : categories_( _categories ),
          comm_( nullptr ),
          random_number_generator_( nullptr ),
          tree_hyperparameters_( _tree_hyperparameters )
    {
    }

    ~DecisionTreeImpl() = default;

    // ----------------------------------------

    /// Trivial getter
    inline bool allow_sets() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->allow_sets_;
    }

    /// Trivial accessor
    inline const std::vector<std::string>& categories() const
    {
        assert( categories_ );
        return *categories_;
    }

    /// Clears up the memory
    inline void clear()
    {
        aggregation_->clear();
        aggregation_->clear_extras();
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT grid_factor() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->grid_factor_;
    }

    /// Trivial getter
    inline const containers::Schema& input() const
    {
        assert( input_ );
        return *input_;
    }

    /// Trivial getter
    inline AUTOSQL_INT ix_perip_used() const
    {
        return column_to_be_aggregated_.ix_perip_used;
    }

    /// Trivial getter
    inline AUTOSQL_INT max_length() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->max_length_;
    }

    /// Trivial getter
    inline AUTOSQL_INT min_num_samples() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->min_num_samples_;
    }

    /// Trivial getter
    inline const containers::Schema& output() const
    {
        assert( output_ );
        return *output_;
    }

    /// Returns a custom random number generator
    inline utils::RandomNumberGenerator rng() const
    {
        assert( random_number_generator_ != nullptr );
        return utils::RandomNumberGenerator( random_number_generator_, comm_ );
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_categorical() const
    {
        assert( same_units_.same_units_categorical_ );
        return *same_units_.same_units_categorical_;
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_discrete() const
    {
        assert( same_units_.same_units_discrete_ );
        return *same_units_.same_units_discrete_;
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER& same_units_numerical() const
    {
        assert( same_units_.same_units_numerical_ );
        return *same_units_.same_units_numerical_;
    }

    /// Trivial setter
    inline void set_same_units( const descriptors::SameUnits& _same_units )
    {
        same_units_ = _same_units;
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT regularization() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->regularization_;
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT share_conditions() const
    {
        assert( tree_hyperparameters_ );
        return tree_hyperparameters_->share_conditions_;
    }

    // ------------------------------------------------------------

    /// Returns the colname corresponding to _data used and
    /// _ix_column_used
    std::string get_colname(
        const std::string& _feature_num,
        const enums::DataUsed _data_used,
        const AUTOSQL_INT _ix_column_used,
        const bool _equals = true ) const;

    // ------------------------------------------------------------

    /// The aggregation is what connects the peripheral table
    /// to the population table and thus the targets.
    std::shared_ptr<aggregations::AbstractAggregation> aggregation_;

    /// Type of the aggregation used (needed for copy constructor)
    std::string aggregation_type_;

    /// Pointer to the vector that maps the integers to categories
    std::shared_ptr<const std::vector<std::string>> categories_;

    /// Pointer to the structure that contains information
    /// about the column aggregated by this tree
    descriptors::ColumnToBeAggregated column_to_be_aggregated_;

    /// Communicator
    multithreading::Communicator* comm_;

    /// The input table used (we keep it, because we need the colnames)
    containers::Optional<containers::Schema> input_;

    /// The optimization criterion is what we want to maximize -
    /// using it we can determine the optimal splits
    optimizationcriteria::OptimizationCriterion* optimization_criterion_;

    /// The output table used (we keep it, because we need the colnames)
    containers::Optional<containers::Schema> output_;

    /// Random number generator
    std::mt19937* random_number_generator_;

    /// Contains information on which of the columns contain the same units
    descriptors::SameUnits same_units_;

    /// Hyperparameters needed to fit this tree.
    std::shared_ptr<const descriptors::TreeHyperparameters>
        tree_hyperparameters_;
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql

#endif  // AUTOSQL_DECISIONTREES_DECISIONTREEIMPL_HPP_
