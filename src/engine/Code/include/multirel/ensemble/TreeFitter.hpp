#ifndef MULTIREL_ENSEMBLE_TREEFITTER_HPP_
#define MULTIREL_ENSEMBLE_TREEFITTER_HPP_

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class TreeFitter
{
    // -------------------------------------------------------------------------

   public:
    TreeFitter(
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        std::mt19937 *_random_number_generator,
        multithreading::Communicator *_comm )
        : comm_( _comm ),
          hyperparameters_( _hyperparameters ),
          random_number_generator_( _random_number_generator )
    {
    }

    ~TreeFitter() = default;

    // -------------------------------------------------------------------------

   public:
    /// Fits the trees in a recursive manner, starting with any subtrees.
    void fit(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_opt,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_match_containers,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<decisiontrees::DecisionTree> *_trees );

    // -------------------------------------------------------------------------

   private:
    /// Finds the best of the candidate trees - and refits, if required.
    void find_best_trees(
        const size_t _num_trees,
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_opt,
        const std::vector<Float> &_values,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_match_containers,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<decisiontrees::DecisionTree> *_trees );

    /// Fits an individual tree.
    void fit_tree(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_opt,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_match_containers,
        decisiontrees::DecisionTree *_tree );

    /// Fits all candidate trees at max_depth = _max_length_probe.
    void probe(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_opt,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_match_containers,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<Float> *_values );

    // -------------------------------------------------------------------------

   private:
    /// Trivial accessor
    multithreading::Communicator *comm() { return comm_; }

    /// Trivial accessor
    const descriptors::Hyperparameters &hyperparameters() const
    {
        assert_true( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Trivial accessor
    std::mt19937 &random_number_generator()
    {
        return *random_number_generator_;
    }

    /// Trivial accessor
    const descriptors::TreeHyperparameters &tree_hyperparameters() const
    {
        assert_true( hyperparameters_ );
        assert_true( hyperparameters_->tree_hyperparameters_ );
        return *hyperparameters_->tree_hyperparameters_;
    }

    /// Trivial accessor
    bool use_timestamps() const
    {
        assert_true( hyperparameters_ );
        return hyperparameters_->use_timestamps_;
    }

    // -------------------------------------------------------------------------

   private:
    /// Communicator
    multithreading::Communicator *comm_;

    /// The hyperparameters for training
    std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

    /// Random number generator used for the candidate trees.
    std::mt19937 *random_number_generator_;

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel
#endif  // MULTIREL_ENSEMBLE_TREEFITTER_HPP_
