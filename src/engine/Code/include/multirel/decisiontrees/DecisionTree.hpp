#ifndef MULTIREL_DECISIONTREES_DECISIONTREE_HPP_
#define MULTIREL_DECISIONTREES_DECISIONTREE_HPP_

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class DecisionTree
{
   public:
    DecisionTree(
        const std::shared_ptr<const descriptors::TreeHyperparameters>
            &_tree_hyperparameters,
        const Poco::JSON::Object &_json_obj );

    DecisionTree(
        const std::string &_agg,
        const std::shared_ptr<const descriptors::TreeHyperparameters>
            &_tree_hyperparameters,
        const size_t _ix_perip_used,
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnits &_same_units,
        std::mt19937 *_random_number_generator,
        multithreading::Communicator *_comm );

    DecisionTree( const DecisionTree &_other );

    DecisionTree( DecisionTree &&_other ) noexcept;

    ~DecisionTree() = default;

    // --------------------------------------

    /// Fits the decision tree
    void fit(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const std::shared_ptr<aggregations::AbstractFitAggregation>
            &_aggregation,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion );

    /// Rebuilds the tree from a Poco::JSON::Object
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Copy constructor
    DecisionTree &operator=( const DecisionTree &_other );

    /// Copy assignment constructor
    DecisionTree &operator=( DecisionTree &&_other ) noexcept;

    /// Generates the appropriate intermediate aggregation.
    std::shared_ptr<optimizationcriteria::OptimizationCriterion>
    make_intermediate(
        std::shared_ptr<aggregations::IntermediateAggregationImpl> _impl )
        const;

    /// Extracts the tree as a JSON object
    Poco::JSON::Object to_json_obj() const;

    /// Extracts the SQL statement underlying the tree
    /// as a string
    std::string to_sql(
        const std::vector<strings::String> &_categories,
        const std::string &_feature_prefix,
        const std::string &_feature_num,
        const bool _use_timestamps ) const;

    /// Transforms a set of raw data into extracted features
    std::vector<Float> transform(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const bool _use_timestamps ) const;

    // --------------------------------------

   private:
    /// Returns a set containing the unique subfeatures used.
    std::set<size_t> make_subfeatures_used() const;

    // --------------------------------------

   public:
    /// Accessor for the aggregation.
    inline std::shared_ptr<aggregations::AbstractFitAggregation> aggregation()
    {
        assert_true( impl_.aggregation_ );
        return impl_.aggregation_;
    }

    /// Type of the aggregation used by this tree.
    inline const std::string aggregation_type() const
    {
        return impl()->aggregation_type_;
    }

    /// Calculates the column importances for this tree.
    inline void column_importances(
        utils::ImportanceMaker *_importance_maker ) const
    {
        assert_true( root_ );
        root_->column_importances( _importance_maker );
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline descriptors::ColumnToBeAggregated &column_to_be_aggregated()
    {
        return impl()->column_to_be_aggregated_;
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline const descriptors::ColumnToBeAggregated &column_to_be_aggregated()
        const
    {
        return impl()->column_to_be_aggregated_;
    }

    /// Whether the decision tree has subtrees.
    inline const bool has_subtrees() const { return subtrees_.size() > 0; }

    /// Trivial getter
    inline size_t ix_perip_used() const { return impl()->ix_perip_used(); }

    /// Generates the fit aggregation.
    inline std::shared_ptr<aggregations::AbstractFitAggregation>
    make_aggregation(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_optimization_criterion,
        containers::Matches *_matches ) const
    {
        return aggregations::FitAggregationParser::parse_aggregation(
            aggregation_type(),
            same_units_discrete(),
            same_units_numerical(),
            column_to_be_aggregated(),
            _population,
            _peripheral,
            _subfeatures,
            _aggregation_impl,
            _optimization_criterion,
            _matches );
    }

    /// Reverses to the status since the last time we have
    /// called commit.
    inline void revert_to_commit()
    {
        aggregation()->revert_to_commit();
        optimization_criterion()->revert_to_commit();
    }

    /// Parallel version only: Set the pointer to the communicator
    inline void set_comm( multithreading::Communicator *_comm )
    {
        impl_.comm_ = _comm;
    }

    /// Trivial setter
    inline void set_same_units( const descriptors::SameUnits &_same_units )
    {
        impl_.set_same_units( _same_units );
    }

    /// Trivial setter
    inline void set_subtrees( const std::vector<DecisionTree> &_subtrees )
    {
        subtrees_ = _subtrees;
    }

    /// Stores the current stage (storing means that it is a candidate for
    /// a commit)
    inline void store_current_stage(
        const Float _num_samples_smaller, const Float _num_samples_greater )
    {
        optimization_criterion()->store_current_stage(
            _num_samples_smaller, _num_samples_greater );
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    inline bool allow_sets() const { return impl_.allow_sets(); }

    /// Trivial accessor
    inline multithreading::Communicator *comm()
    {
        assert_true( impl_.comm_ != nullptr );
        return impl_.comm_;
    }

    /// Trivial accessor
    inline DecisionTreeImpl *impl() { return &impl_; }

    /// Trivial accessor
    inline const DecisionTreeImpl *impl() const { return &impl_; }

    /// Trivial accessor
    inline const containers::Placeholder &input() const
    {
        assert_true( impl()->input_ );
        return *impl()->input_;
    }

    /// Trivial accessor
    inline Float grid_factor() const { return impl_.grid_factor(); }

    /// Trivial accessor
    inline size_t max_length() const { return impl_.max_length(); }

    /// Trivial accessor
    inline size_t min_num_samples() const { return impl_.min_num_samples(); }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        assert_true( impl_.optimization_criterion_ );
        return impl_.optimization_criterion_;
    }

    /// Trivial accessor
    inline const containers::Placeholder &output() const
    {
        assert_true( impl()->output_ );
        return *impl()->output_;
    }

    /// Trivial accessor
    inline Float regularization() const { return impl_.regularization(); }

    /// Trivial accessor
    inline containers::Optional<DecisionTreeNode> &root() { return root_; }

    /// Trivial accessor
    inline const containers::Optional<DecisionTreeNode> &root() const
    {
        return root_;
    }

    /// Trivial accessor
    inline const descriptors::SameUnitsContainer &same_units_categorical() const
    {
        return impl()->same_units_categorical();
    }

    /// Trivial accessor
    inline const descriptors::SameUnitsContainer &same_units_discrete() const
    {
        return impl()->same_units_discrete();
    }

    /// Trivial accessor
    inline const descriptors::SameUnitsContainer &same_units_numerical() const
    {
        return impl()->same_units_numerical();
    }

    /// Trivial accessor
    inline Float share_conditions() const { return impl_.share_conditions(); }

    /// Subtrees
    inline std::vector<DecisionTree> &subtrees() { return subtrees_; }

    /// Subtrees
    inline const std::vector<DecisionTree> &subtrees() const
    {
        return subtrees_;
    }

    // --------------------------------------

   private:
    /// Contains all member variables, other than root_
    DecisionTreeImpl impl_;

    /// Root node - first node of the decision tree
    containers::Optional<DecisionTreeNode> root_;

    /// Needed for the snowflake data model
    std::vector<DecisionTree> subtrees_;

    // --------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel

#endif  // MULTIREL_DECISIONTREES_DECISIONTREE_HPP_
