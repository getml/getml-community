#ifndef AUTOSQL_DECISIONTREES_TREEFITTER_HPP_
#define AUTOSQL_DECISIONTREES_TREEFITTER_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class TreeFitter
{
    // -------------------------------------------------------------------------

   public:
    TreeFitter(
        std::shared_ptr<containers::Encoding> &_categories,
        multithreading::Communicator *_comm,
        descriptors::Hyperparameters &_hyperparameters,
        std::mt19937 &_random_number_generator )
        : categories_( _categories ),
          comm_( _comm ),
          hyperparameters_( _hyperparameters ),
          random_number_generator_( _random_number_generator )
    {
    }

    ~TreeFitter() = default;

    // -------------------------------------------------------------------------

   public:
    /// Fits the trees in a recursive manner, starting with any subtrees.
    void fit(
        TableHolder &_table_holder,
        std::vector<AUTOSQL_SAMPLES> &_samples,
        std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        std::list<DecisionTree> &_candidate_trees,
        std::vector<DecisionTree> &_trees );

    // -------------------------------------------------------------------------

   private:
    /// Finds the best of the candidate trees - and refits, if required.
    void find_best_trees(
        const size_t _num_trees,
        const std::vector<AUTOSQL_FLOAT> &_values,
        std::vector<AUTOSQL_SAMPLES> &_samples,
        std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        TableHolder &_table_holder,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        std::list<DecisionTree> &_candidate_trees,
        std::vector<DecisionTree> &_trees );

    /// Fits the subtrees.
    void fit_subtrees(
        TableHolder &_table_holder,
        const std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        optimizationcriteria::OptimizationCriterion *_opt,
        std::list<DecisionTree> &_candidate_trees );

    /// Fits the subtrees for each of the candidates.
    void fit_subtrees_for_candidates(
        const AUTOSQL_INT _ix_subtable,
        TableHolder &_subtable,
        std::vector<AUTOSQL_SAMPLES> &_samples,
        std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        const std::vector<descriptors::SameUnits> &_same_units,
        std::shared_ptr<aggregations::IntermediateAggregationImpl> &_opt_impl,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        std::list<DecisionTree> &_candidate_trees );

    /// Fits an individual tree.
    void fit_tree(
        const AUTOSQL_INT _max_length,
        std::vector<AUTOSQL_SAMPLES> &_samples,
        std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        TableHolder &_table_holder,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        DecisionTree &_tree );

    /// Fits all candidate trees at max_depth = _max_length_probe.
    void probe(
        std::vector<AUTOSQL_SAMPLES> &_samples,
        std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
        TableHolder &_table_holder,
        optimizationcriteria::OptimizationCriterion *_opt,
        std::vector<AUTOSQL_FLOAT> &_values,
        std::list<DecisionTree> &_candidate_trees );

    // -------------------------------------------------------------------------

   private:
    /// Trivial accessor
    std::shared_ptr<containers::Encoding> &categories()
    {
        return categories_;
    }

    /// Trivial accessor
    multithreading::Communicator *comm() { return comm_; }

    /// Trivial accessor
    descriptors::Hyperparameters &hyperparameters() { return hyperparameters_; }

    /// Trivial accessor
    std::mt19937 &random_number_generator() { return random_number_generator_; }

    /// Trivial accessor
    const descriptors::TreeHyperparameters &tree_hyperparameters()
    {
        return hyperparameters_.tree_hyperparameters;
    }

    /// Trivial accessor
    bool use_timestamps() const { return hyperparameters_.use_timestamps; }

    // -------------------------------------------------------------------------

   private:
    /// List of categories mapping category names to integers
    std::shared_ptr<containers::Encoding> &categories_;

    /// Communicator
    multithreading::Communicator *comm_;

    /// The hyperparameters for training
    descriptors::Hyperparameters &hyperparameters_;

    /// Random number generator used for the candidate trees.
    std::mt19937 &random_number_generator_;

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_TREEFITTER_HPP_
