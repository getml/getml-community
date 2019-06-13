#ifndef AUTOSQL_DECISIONTREES_DECISIONTREE_HPP_
#define AUTOSQL_DECISIONTREES_DECISIONTREE_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class DecisionTree
{
   public:
    DecisionTree(
        const std::shared_ptr<const std::vector<std::string>> &_categories,
        const std::shared_ptr<const descriptors::TreeHyperparameters>
            &_tree_hyperparameters,
        const Poco::JSON::Object &_json_obj );

    DecisionTree(
        const std::string &_agg,
        const std::shared_ptr<const std::vector<std::string>> &_categories,
        const std::shared_ptr<const descriptors::TreeHyperparameters>
            &_tree_hyperparameters,
        const size_t _ix_perip_used,
        const enums::DataUsed _data_used,
        const size_t _ix_column_used,
        const descriptors::SameUnits &_same_units,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm );

    DecisionTree( const DecisionTree &_other );

    DecisionTree( DecisionTree &&_other ) noexcept;

    ~DecisionTree() = default;

    // --------------------------------------

    /// A sample containers contains a value to be aggregated. This is set by
    /// this function.
    void create_value_to_be_aggregated(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<
            AUTOSQL_FLOAT,
            std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
        const AUTOSQL_SAMPLE_CONTAINER &_sample_container,
        aggregations::AbstractAggregation *_aggregation ) const;

    /// Fits the decision tree
    void fit(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<
            AUTOSQL_FLOAT,
            std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion );

    /// Rebuilds the tree from a Poco::JSON::Object
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Copy constructor
    DecisionTree &operator=( const DecisionTree &_other );

    /// Copy assignment constructor
    DecisionTree &operator=( DecisionTree &&_other ) noexcept;

    /// Generates the select statement
    std::string select_statement( const std::string &_feature_num ) const;

    /// Extracts the tree as a JSON object
    Poco::JSON::Object to_json_obj() const;

    /// Extracts the tree in a format the monitor can understand
    Poco::JSON::Object to_monitor(
        const std::string &_feature_num, const bool _use_timestamps ) const;

    /// Extracts the SQL statement underlying the tree
    /// as a string
    std::string to_sql(
        const std::string _feature_num, const bool _use_timestamps ) const;

    /// Transforms a set of raw data into extracted features
    std::vector<AUTOSQL_FLOAT> transform(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Optional<TableHolder> &_subtables,
        const bool _use_timestamps,
        aggregations::AbstractAggregation *_aggregation ) const;

    // --------------------------------------

   public:
    /// Some aggregations, such as MIN and MAX require the samples
    /// (not the pointers to the samples) to be sorted.
    inline bool const aggregation_needs_sorting()
    {
        return aggregation()->needs_sorting();
    }

    /// Type of the aggregation used by this tree.
    inline const std::string aggregation_type() const
    {
        return aggregation()->type();
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline descriptors::ColumnToBeAggregated &column_to_be_aggregated()
    {
        return impl()->column_to_be_aggregated_;
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline const descriptors::ColumnToBeAggregated &column_to_be_aggregated() const
    {
        return impl()->column_to_be_aggregated_;
    }

    /// Calls create_value_to_be_aggregated on the tree's own aggregation.
    void create_value_to_be_aggregated(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<
            AUTOSQL_FLOAT,
            std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
        const AUTOSQL_SAMPLE_CONTAINER &_sample_container )
    {
        create_value_to_be_aggregated(
            _population,
            _peripheral,
            _subfeatures,
            _sample_container,
            aggregation() );
    }

    /// Whether the decision tree has subtrees.
    inline const bool has_subtrees() const { return subtrees_.size() > 0; }

    /// Returns the type of an intermediate aggregation representing the
    /// aggregation of this DecisionTree.
    inline std::string intermediate_type() const
    {
        assert( impl()->aggregation_ );
        return impl()->aggregation_->intermediate_type();
    }

    /// Trivial getter
    inline AUTOSQL_INT ix_perip_used() const { return impl()->ix_perip_used(); }

    /// Creates the aggregation used by this tree or a clone thereof.
    inline std::shared_ptr<aggregations::AbstractAggregation> const
    make_aggregation() const
    {
        return aggregations::AggregationParser::parse_aggregation(
            impl()->aggregation_type_,
            column_to_be_aggregated().data_used,
            column_to_be_aggregated().ix_column_used,
            same_units_numerical(),
            same_units_discrete() );
    }

    /// Returns an intermediate aggregation representing the aggregation
    /// of this DecisionTree.
    inline std::shared_ptr<optimizationcriteria::OptimizationCriterion>
    make_intermediate(
        std::shared_ptr<aggregations::IntermediateAggregationImpl> _impl ) const
    {
        assert( impl()->aggregation_ );
        return impl()->aggregation_->make_intermediate( _impl );
    }

    /// Reverses to the status since the last time we have
    /// called commit.
    inline void revert_to_commit()
    {
        aggregation()->revert_to_commit();
        optimization_criterion()->revert_to_commit();
    }

    /// Separates all null values in the samples
    inline AUTOSQL_SAMPLES::iterator separate_null_values(
        AUTOSQL_SAMPLES &_samples )
    {
        return aggregation()->separate_null_values( _samples );
    }

    /// Separates all null values in the sample containers (recall
    /// that a sample containers contains pointers to samples)
    inline AUTOSQL_SAMPLE_CONTAINER::iterator separate_null_values(
        AUTOSQL_SAMPLE_CONTAINER &_samples )
    {
        return aggregation()->separate_null_values( _samples );
    }

    /// Passes the impl of the aggregation to the aggregation
    inline void set_aggregation_impl(
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl )
    {
        impl_.aggregation_->set_aggregation_impl( _aggregation_impl );
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

    /// Passes beginning and end of samples to the aggregation (
    /// this is required by those aggregations that also need the
    /// samples to be sorted)
    inline void set_samples_begin_end(
        Sample *_samples_begin, Sample *_samples_end )
    {
        aggregation()->set_samples_begin_end( _samples_begin, _samples_end );
    }

    /// Some aggregations, such as MIN and MAX require the samples
    /// (not the pointers to the samples) to be sorted.
    inline void sort_samples(
        AUTOSQL_SAMPLES::iterator _samples_begin,
        AUTOSQL_SAMPLES::iterator _samples_end )
    {
        aggregation()->sort_samples( _samples_begin, _samples_end );
    }

    /// Stores the current stage (storing means that it is a candidate for
    /// a commit)
    inline void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater )
    {
        optimization_criterion()->store_current_stage(
            _num_samples_smaller, _num_samples_greater );
    }

    /// Calls transform(...) using the tree's own aggregation.
    std::vector<AUTOSQL_FLOAT> transform(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Optional<TableHolder> &_subtable,
        const bool _use_timestamps )
    {
        return transform(
            _population,
            _peripheral,
            _subtable,
            _use_timestamps,
            aggregation() );
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    inline aggregations::AbstractAggregation *aggregation()
    {
        assert( impl_.aggregation_ && "aggregation()" );
        return impl_.aggregation_.get();
    }

    /// Trivial accessor
    inline const aggregations::AbstractAggregation *aggregation() const
    {
        assert( impl_.aggregation_ && "aggregation()" );
        return impl_.aggregation_.get();
    }

    /// Trivial accessor
    inline std::shared_ptr<aggregations::AbstractAggregation> &aggregation_ptr()
    {
        return impl_.aggregation_;
    }

    /// Trivial accessor
    inline bool allow_sets() const { return impl_.allow_sets(); }

    /// Trivial accessor
    inline multithreading::Communicator *comm()
    {
        assert( impl_.comm_ != nullptr );
        return impl_.comm_;
    }

    /// Trivial accessor
    inline DecisionTreeImpl *impl() { return &impl_; }

    /// Trivial accessor
    inline const DecisionTreeImpl *impl() const { return &impl_; }

    /// Trivial accessor
    inline const containers::Schema &input() const
    {
        assert( impl()->input_ );
        return *impl()->input_;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT grid_factor() const { return impl_.grid_factor(); }

    /// Trivial accessor
    inline AUTOSQL_INT max_length() const { return impl_.max_length(); }

    /// Trivial accessor
    inline AUTOSQL_INT min_num_samples() const
    {
        return impl_.min_num_samples();
    }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *&
    optimization_criterion()
    {
        return impl_.optimization_criterion_;
    }

    /// Trivial accessor
    inline const containers::Schema &output() const
    {
        assert( impl()->output_ );
        return *impl()->output_;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT regularization() const
    {
        return impl_.regularization();
    }

    /// Trivial accessor
    inline containers::Optional<DecisionTreeNode> &root() { return root_; }

    /// Trivial accessor
    inline const containers::Optional<DecisionTreeNode> &root() const
    {
        return root_;
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_categorical() const
    {
        return impl()->same_units_categorical();
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_discrete() const
    {
        return impl()->same_units_discrete();
    }

    /// Trivial accessor
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_numerical() const
    {
        return impl()->same_units_numerical();
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT share_conditions() const
    {
        return impl_.share_conditions();
    }

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
}  // namespace autosql

#endif  // AUTOSQL_DECISIONTREES_DECISIONTREE_HPP_
