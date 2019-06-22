#ifndef AUTOSQL_ENSEMBLE_TREEFITTER_HPP_
#define AUTOSQL_ENSEMBLE_TREEFITTER_HPP_

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class TreeFitter
{
    // -------------------------------------------------------------------------

   public:
    TreeFitter(
        const std::shared_ptr<const std::vector<std::string>> &_categories,
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        std::mt19937 *_random_number_generator,
        multithreading::Communicator *_comm )
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
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_sample_containers,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<decisiontrees::DecisionTree> *_trees );

    // -------------------------------------------------------------------------

   private:
    /// Finds the best of the candidate trees - and refits, if required.
    void find_best_trees(
        const size_t _num_trees,
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const std::vector<Float> &_values,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_sample_containers,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<decisiontrees::DecisionTree> *_trees );

    /// Fits an individual tree.
    void fit_tree(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_sample_containers,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        decisiontrees::DecisionTree *_tree );

    /// Fits all candidate trees at max_depth = _max_length_probe.
    void probe(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        std::vector<containers::Matches> *_samples,
        std::vector<containers::MatchPtrs> *_sample_containers,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        std::list<decisiontrees::DecisionTree> *_candidate_trees,
        std::vector<Float> *_values );

    // -------------------------------------------------------------------------

   private:
    /// Trivial accessor
    std::shared_ptr<const std::vector<std::string>> &categories()
    {
        assert( categories_ );
        return categories_;
    }

    /// Trivial accessor
    multithreading::Communicator *comm() { return comm_; }

    /// Trivial accessor
    const descriptors::Hyperparameters &hyperparameters() const
    {
        assert( hyperparameters_ );
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
        assert( hyperparameters_ );
        assert( hyperparameters_->tree_hyperparameters_ );
        return *hyperparameters_->tree_hyperparameters_;
    }

    /// Trivial accessor
    bool use_timestamps() const
    {
        assert( hyperparameters_ );
        return hyperparameters_->use_timestamps_;
    }

    // -------------------------------------------------------------------------

   private:
    /// List of categories mapping category names to integers
    std::shared_ptr<const std::vector<std::string>> categories_;

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
}  // namespace autosql
#endif  // AUTOSQL_ENSEMBLE_TREEFITTER_HPP_
