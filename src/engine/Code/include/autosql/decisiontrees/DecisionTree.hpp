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
    DecisionTree( const Poco::JSON::Object &_json_obj );

    DecisionTree(
        const std::string &_agg,
        const AUTOSQL_INT _ix_column_used,
        const DataUsed _data_used,
        const AUTOSQL_INT _ix_perip_used,
        const descriptors::SameUnits &_same_units,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl>
            &_aggregation_impl );

    DecisionTree( const DecisionTree &_other );

    DecisionTree( DecisionTree &&_other ) noexcept;

    ~DecisionTree() = default;

    // --------------------------------------

    /// A sample containers contains a value to be aggregated. This is set by
    /// this function.
    void create_value_to_be_aggregated(
        TableHolder &_table_holder,
        AUTOSQL_SAMPLE_CONTAINER &_sample_container );

    /// Fits the decision tree
    void fit(
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end,
        TableHolder &_table_holder,
        optimizationcriteria::OptimizationCriterion *_optimization_criterion,
        bool _allow_sets,
        AUTOSQL_INT _max_length,
        AUTOSQL_INT _min_num_samples,
        AUTOSQL_FLOAT _grid_factor,
        AUTOSQL_FLOAT _regularization,
        AUTOSQL_FLOAT _share_conditions,
        bool _use_timestamps );

    /// Rebuilds the tree from a Poco::JSON::Object
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Copy constructor
    DecisionTree &operator=( const DecisionTree &_other );

    /// Copy assignment constructor
    DecisionTree &operator=( DecisionTree &&_other ) noexcept;

    /// Transforms a string into a proper aggregation
    std::shared_ptr<aggregations::AggregationBase> parse_aggregation(
        const std::string &_aggregation );

    /// Generates the select statement
    std::string select_statement( const std::string &_feature_num ) const;

    /// Calculates the feature importances
    void source_importances( descriptors::SourceImportances &_importances );

    /// Extracts the tree as a JSON object
    Poco::JSON::Object to_json_obj();

    /// Extracts the tree in a format the monitor can understand
    Poco::JSON::Object to_monitor(
        const std::string &_feature_num, const bool _use_timestamps ) const;

    /// Extracts the SQL statement underlying the tree
    /// as a string
    std::string to_sql(
        const std::string _feature_num, const bool _use_timestamps ) const;

    /// Transforms a set of raw data into extracted features
    containers::Matrix<AUTOSQL_FLOAT> transform(
        TableHolder &_table_holder, bool _use_timestamps );

    // --------------------------------------

   public:
    /// Some aggregations, such as MIN and MAX require the samples
    /// (not the pointers to the samples) to be sorted.
    inline bool const aggregation_needs_sorting()
    {
        return aggregation()->needs_sorting();
    }

    /// Type of the aggregation used by this tree.
    inline std::string const aggregation_type() const
    {
        return aggregation()->type();
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline ColumnToBeAggregated &column_to_be_aggregated()
    {
        return impl()->column_to_be_aggregated_;
    }

    /// Returns the information required for identifying the
    /// columns to be aggregated by this tree.
    inline const ColumnToBeAggregated &column_to_be_aggregated() const
    {
        return impl()->column_to_be_aggregated_;
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
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl )
    {
        impl_.aggregation_->set_aggregation_impl( _aggregation_impl );
    }

    /// Trivial setter
    inline void set_categories(
        const std::shared_ptr<containers::Encoding> &_categories )
    {
        impl_.categories_ = _categories;
    }

#ifdef AUTOSQL_PARALLEL

    /// Parallel version only: Set the pointer to the communicator
    inline void set_comm( AUTOSQL_COMMUNICATOR *_comm ) { impl_.comm_ = _comm; }

#endif  // AUTOSQL_PARALLEL

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

    // --------------------------------------

   private:
    /// Returns the right aggregation based on _data_used
    template <typename AggType>
    std::shared_ptr<aggregations::AggregationBase> make_aggregation();

    /// Generates the features from the subtrees
    void transform_subtrees( TableHolder &_table_holder, bool _use_timestamps );

    // --------------------------------------

   private:
    /// Trivial accessor
    inline aggregations::AggregationBase *aggregation()
    {
        assert( impl_.aggregation_ && "aggregation()" );
        return impl_.aggregation_.get();
    }

    /// Trivial accessor
    inline const aggregations::AggregationBase *aggregation() const
    {
        assert( impl_.aggregation_ && "aggregation()" );
        return impl_.aggregation_.get();
    }

    /// Trivial accessor
    inline std::shared_ptr<aggregations::AggregationBase> &aggregation_ptr()
    {
        return impl_.aggregation_;
    }

    /// Trivial accessor
    inline bool &allow_sets() { return impl_.allow_sets_; }

#ifdef AUTOSQL_PARALLEL

    /// Trivial accessor
    inline AUTOSQL_COMMUNICATOR *comm()
    {
        assert( impl_.comm_ != nullptr );
        return impl_.comm_;
    }

#endif  // AUTOSQL_PARALLEL

    /// Trivial accessor
    inline DecisionTreeImpl *impl() { return &impl_; }

    /// Trivial accessor
    inline const DecisionTreeImpl *impl() const { return &impl_; }

    /// Trivial getter
    inline AUTOSQL_INT ix_perip_used() const { return impl()->ix_perip_used(); }

    /// Trivial accessor
    inline std::string &join_keys_perip_name()
    {
        return impl()->join_keys_perip_name_;
    }

    /// Trivial accessor
    inline const std::string &join_keys_perip_name() const
    {
        return impl()->join_keys_perip_name_;
    }

    /// Trivial accessor
    inline std::string &join_keys_popul_name()
    {
        return impl()->join_keys_popul_name_;
    }

    /// Trivial accessor
    inline const std::string &join_keys_popul_name() const
    {
        return impl()->join_keys_popul_name_;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT &grid_factor() { return impl_.grid_factor_; }

    /// Trivial accessor
    inline AUTOSQL_INT &max_length() { return impl_.max_length_; }

    /// Trivial accessor
    inline AUTOSQL_INT &min_num_samples() { return impl_.min_num_samples_; }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *&
    optimization_criterion()
    {
        return impl_.optimization_criterion_;
    }

    /// Trivial accessor
    inline containers::DataFrame &peripheral() { return impl_.peripheral_; }

    /// Trivial accessor
    inline std::string &peripheral_name() { return impl_.peripheral_name_; }

    /// Trivial accessor
    inline const std::string &peripheral_name() const
    {
        return impl_.peripheral_name_;
    }

    /// Trivial accessor
    inline containers::DataFrameView &population() { return impl_.population_; }

    /// Trivial accessor
    inline std::string &population_name() { return impl_.population_name_; }

    /// Trivial accessor
    inline const std::string &population_name() const
    {
        return impl_.population_name_;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT &regularization() { return impl_.regularization_; }

    /// Trivial accessor
    inline containers::Optional<DecisionTreeNode> &root() { return root_; }

    /// Trivial accessor
    inline const containers::Optional<DecisionTreeNode> &root() const
    {
        return root_;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT &share_conditions() { return impl_.share_conditions_; }

    /// Subtrees
    inline std::vector<DecisionTree> &subtrees() { return subtrees_; }

    /// Subtrees
    inline const std::vector<DecisionTree> &subtrees() const
    {
        return subtrees_;
    }

    /// Trivial accessor
    inline std::string &time_stamps_perip_name()
    {
        return impl()->time_stamps_perip_name_;
    }

    /// Trivial accessor
    inline const std::string &time_stamps_perip_name() const
    {
        return impl()->time_stamps_perip_name_;
    }

    /// Trivial accessor
    inline std::string &time_stamps_popul_name()
    {
        return impl()->time_stamps_popul_name_;
    }

    /// Trivial accessor
    inline const std::string &time_stamps_popul_name() const
    {
        return impl()->time_stamps_popul_name_;
    }

    /// Trivial accessor
    inline std::string &upper_time_stamps_name()
    {
        return impl()->upper_time_stamps_name_;
    }

    /// Trivial accessor
    inline const std::string &upper_time_stamps_name() const
    {
        return impl()->upper_time_stamps_name_;
    }

    /// Trivial accessor
    inline std::string &x_perip_categorical_colname( AUTOSQL_INT _i )
    {
        return x_perip_categorical_colnames()[_i];
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_perip_categorical_colnames()
    {
        assert(
            impl_.x_perip_categorical_colnames_ &&
            "x_perip_categorical_colnames()" );
        return *( impl_.x_perip_categorical_colnames_.get() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_perip_numerical_colnames()
    {
        assert(
            impl_.x_perip_numerical_colnames_ &&
            "x_perip_numerical_colnames()" );
        return *( impl_.x_perip_numerical_colnames_.get() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_perip_discrete_colnames()
    {
        assert(
            impl_.x_perip_discrete_colnames_ && "x_perip_discrete_colnames()" );
        return *( impl_.x_perip_discrete_colnames_.get() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_popul_categorical_colnames()
    {
        assert(
            impl_.x_popul_categorical_colnames_ &&
            "x_popul_categorical_colnames()" );
        return *( impl_.x_popul_categorical_colnames_.get() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_popul_numerical_colnames()
    {
        assert(
            impl_.x_popul_numerical_colnames_ &&
            "x_popul_numerical_colnames()" );
        return *( impl_.x_popul_numerical_colnames_.get() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &x_popul_discrete_colnames()
    {
        assert(
            impl_.x_popul_discrete_colnames_ && "x_popul_discrete_colnames()" );
        return *( impl_.x_popul_discrete_colnames_.get() );
    }

    // --------------------------------------

   private:
    /// Contains all member variables, other than root_
    DecisionTreeImpl impl_;

    /// Root node - first node of the decision tree
    containers::Optional<DecisionTreeNode> root_;

    /// Needed for the snowflake data model
    std::vector<DecisionTree> subtrees_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename AggType>
std::shared_ptr<aggregations::AggregationBase> DecisionTree::make_aggregation()
{
    // ------------------------------------------------------------------------

    const auto data_used = column_to_be_aggregated().data_used;

    const AUTOSQL_INT ix_column_used = column_to_be_aggregated().ix_column_used;

    // ------------------------------------------------------------------------

    switch ( data_used )
        {
            case DataUsed::x_perip_numerical:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    DataUsed::x_perip_numerical,
                    false>>();

                break;

            case DataUsed::x_perip_discrete:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    DataUsed::x_perip_discrete,
                    false>>();

                break;

            case DataUsed::time_stamps_diff:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    DataUsed::time_stamps_diff,
                    true>>();

                break;

            case DataUsed::same_unit_numerical:

                {
                    const DataUsed data_used2 =
                        std::get<1>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .data_used;

                    if ( data_used2 == DataUsed::x_popul_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                DataUsed::same_unit_numerical,
                                true>>();
                        }
                    else if ( data_used2 == DataUsed::x_perip_numerical )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                DataUsed::same_unit_numerical,
                                false>>();
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AggregationBase>();
                        }
                }

                break;

            case DataUsed::same_unit_discrete:

                {
                    const DataUsed data_used2 =
                        std::get<1>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .data_used;

                    if ( data_used2 == DataUsed::x_popul_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                DataUsed::same_unit_discrete,
                                true>>();
                        }
                    else if ( data_used2 == DataUsed::x_perip_discrete )
                        {
                            return std::make_shared<aggregations::Aggregation<
                                AggType,
                                DataUsed::same_unit_discrete,
                                false>>();
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in make_aggregation(...)!" );

                            return std::shared_ptr<
                                aggregations::AggregationBase>();
                        }
                }

                break;

            case DataUsed::x_perip_categorical:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    DataUsed::x_perip_categorical,
                    false>>();

                break;

            case DataUsed::x_subfeature:

                return std::make_shared<
                    aggregations::
                        Aggregation<AggType, DataUsed::x_subfeature, false>>();

                break;

            case DataUsed::not_applicable:

                return std::make_shared<aggregations::Aggregation<
                    AggType,
                    DataUsed::not_applicable,
                    false>>();

                break;

            default:

                assert( !"Unknown DataUsed in make_aggregation(...)!" );

                return std::shared_ptr<aggregations::AggregationBase>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
#endif  // AUTOSQL_DECISIONTREES_DECISIONTREE_HPP_
