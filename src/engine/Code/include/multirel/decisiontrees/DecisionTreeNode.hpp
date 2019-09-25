#ifndef MULTIREL_DECISIONTREES_DECISIONTREENODE_HPP_
#define MULTIREL_DECISIONTREES_DECISIONTREENODE_HPP_

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class DecisionTreeNode
{
   public:
    DecisionTreeNode(
        bool _is_activated, Int _depth, const DecisionTreeImpl *_tree );

    ~DecisionTreeNode() = default;

    // --------------------------------------

    /// Fits the decision tree node
    void fit(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Calling this member functions means that this node is a root
    /// and must undertake the necessary steps: It removes all samples
    /// for which the sample weight is 0, activates all remaining samples
    /// and commits this is in the aggregation and the optimization criterion
    void fit_as_root(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<Float, std::map<Int, Int>>>
            &_subfeatures,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Builds the node from a Poco::JSON::Object.
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Expresses the conditions in a form that can be understood
    /// by the monitor application (professional only)
    void to_monitor(
        const std::string &_feature_num,
        Poco::JSON::Array _node,
        Poco::JSON::Array &_conditions ) const;

    /// Extracts the node (and its children) as a Poco::JSON::Object
    Poco::JSON::Object to_json_obj() const;

    /// Returns the SQL condition associated with this node
    void to_sql(
        const std::string &_feature_num,
        std::vector<std::string> &_conditions,
        std::string _sql ) const;

    /// Transforms the inserted samples
    void transform(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<Float, std::map<Int, Int>>>
            &_subfeatures,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        aggregations::AbstractAggregation *_aggregation ) const;

    // --------------------------------------

   public:
    /// Trivial setter
    void set_tree( DecisionTreeImpl *_tree ) noexcept
    {
        tree_ = _tree;

        if ( child_node_greater_ )
            {
                child_node_greater_.get()->set_tree( _tree );
                child_node_smaller_.get()->set_tree( _tree );
            }
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    inline aggregations::AbstractAggregation *aggregation()
    {
        return tree_->aggregation_.get();
    }

    /// Trivial accessor
    inline bool apply_from_above() const
    {
        assert_true( split_ );
        return split_->apply_from_above;
    }

    /// Calculates the appropriate number of critical values
    inline Int calculate_num_critical_values( size_t _num_samples_on_node )
    {
        return std::max(
            static_cast<Int>(
                tree_->grid_factor() *
                std::sqrt( static_cast<Float>( _num_samples_on_node ) ) ),
            1 );
    }

    /// Whether the data used is categorical
    inline bool categorical_data_used() const
    {
        assert_true( split_ );
        return (
            split_->data_used == enums::DataUsed::same_unit_categorical ||
            split_->data_used == enums::DataUsed::x_perip_categorical ||
            split_->data_used == enums::DataUsed::x_popul_categorical );
    }

    /// Trivial accessor
    inline const std::vector<Int> &categories_used() const
    {
        assert_true( split_ );
        assert_true(
            split_->categories_used->cbegin() ==
            split_->categories_used_begin );
        assert_true(
            split_->categories_used->cend() == split_->categories_used_end );

        return *split_->categories_used;
    }

    /// Trivial accessor
    inline std::vector<Int>::const_iterator categories_used_begin() const
    {
        assert_true( split_ );
        return split_->categories_used_begin;
    }

    /// Trivial accessor
    inline std::vector<Int>::const_iterator categories_used_end() const
    {
        assert_true( split_ );
        return split_->categories_used_end;
    }

    /// Trivial accessor
    inline multithreading::Communicator *comm()
    {
        assert_true( tree_->comm_ != nullptr );
        return tree_->comm_;
    }

    /// Trivial accessor
    inline size_t column_used() const
    {
        assert_true( split_ );
        return split_->column_used;
    }

    /// Trivial accessor
    inline Float critical_value() const
    {
        assert_true( split_ );
        return split_->critical_value;
    }

    /// Trivial accessor
    inline enums::DataUsed data_used() const
    {
        assert_true( split_ );
        return split_->data_used;
    }

    /// Whether the data used is discrete
    inline bool discrete_data_used() const
    {
        assert_true( split_ );
        return (
            split_->data_used == enums::DataUsed::same_unit_discrete ||
            split_->data_used == enums::DataUsed::x_perip_discrete ||
            split_->data_used == enums::DataUsed::x_popul_discrete );
    }

    /// Non-trivial getter
    const Int get_same_unit_categorical(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        const auto col1 =
            std::get<0>( tree_->same_units_categorical()[_col] ).ix_column_used;

        const auto col2 =
            std::get<1>( tree_->same_units_categorical()[_col] ).ix_column_used;

        const auto val1 =
            ( std::get<0>( tree_->same_units_categorical()[_col] ).data_used ==
              enums::DataUsed::x_perip_categorical )
                ? ( get_x_perip_categorical( _peripheral, _sample, col1 ) )
                : ( get_x_popul_categorical( _population, _sample, col1 ) );

        const auto val2 =
            ( std::get<1>( tree_->same_units_categorical()[_col] ).data_used ==
              enums::DataUsed::x_perip_categorical )
                ? ( get_x_perip_categorical( _peripheral, _sample, col2 ) )
                : ( get_x_popul_categorical( _population, _sample, col2 ) );

        // Feature -1 will be ignored during training - because it is equivalent
        // to != 0
        return ( val1 == val2 ) ? ( 0 ) : ( -1 );
    }

    /// Non-trivial getter
    const Float get_same_unit_discrete(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        const auto col1 =
            std::get<0>( tree_->same_units_discrete()[_col] ).ix_column_used;

        const auto col2 =
            std::get<1>( tree_->same_units_discrete()[_col] ).ix_column_used;

        Float val1 = 0.0;

        switch ( std::get<0>( tree_->same_units_discrete()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_discrete:
                    val1 = get_x_perip_discrete( _peripheral, _sample, col1 );
                    break;

                case enums::DataUsed::x_popul_discrete:
                    val1 = get_x_popul_discrete( _population, _sample, col1 );
                    break;

                default:
                    assert_true(
                        !"get_same_unit_discrete: enums::DataUsed not known!" );
                    break;
            }

        Float val2 = 0.0;

        switch ( std::get<1>( tree_->same_units_discrete()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_discrete:
                    val2 = get_x_perip_discrete( _peripheral, _sample, col2 );
                    break;

                case enums::DataUsed::x_popul_discrete:
                    val2 = get_x_popul_discrete( _population, _sample, col2 );
                    break;

                default:
                    assert_true(
                        !"get_same_unit_discrete: enums::DataUsed not known!" );
                    break;
            }

        return val2 - val1;
    }

    /// Non-trivial getter
    const Float get_same_unit_numerical(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        const auto col1 =
            std::get<0>( tree_->same_units_numerical()[_col] ).ix_column_used;

        const auto col2 =
            std::get<1>( tree_->same_units_numerical()[_col] ).ix_column_used;

        Float val1 = 0.0;

        switch ( std::get<0>( tree_->same_units_numerical()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_numerical:
                    val1 = get_x_perip_numerical( _peripheral, _sample, col1 );
                    break;

                case enums::DataUsed::x_popul_numerical:
                    val1 = get_x_popul_numerical( _population, _sample, col1 );
                    break;

                default:
                    assert_true( !"get_same_unit_numerical: enums::DataUsed not known!" );
                    break;
            }

        Float val2 = 0.0;

        switch ( std::get<1>( tree_->same_units_numerical()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_numerical:
                    val2 = get_x_perip_numerical( _peripheral, _sample, col2 );
                    break;

                case enums::DataUsed::x_popul_numerical:
                    val2 = get_x_popul_numerical( _population, _sample, col2 );
                    break;

                default:
                    assert_true( !"get_same_unit_numerical: enums::DataUsed not known!" );
                    break;
            }

        return val2 - val1;
    }

    /// Trivial getter
    inline const Float get_time_stamps_diff(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample ) const
    {
        return _population.time_stamp( _sample->ix_x_popul ) -
               _peripheral.time_stamp( _sample->ix_x_perip );
    }

    /// Trivial getter
    inline const Int get_x_perip_categorical(
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _peripheral.categorical( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const Float get_x_perip_numerical(
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _peripheral.numerical( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const Float get_x_perip_discrete(
        const containers::DataFrame &_peripheral,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _peripheral.discrete( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const Int get_x_popul_categorical(
        const containers::DataFrameView &_population,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _population.categorical( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const Float get_x_popul_numerical(
        const containers::DataFrameView &_population,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _population.numerical( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const Float get_x_popul_discrete(
        const containers::DataFrameView &_population,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        return _population.discrete( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const Float get_x_subfeature(
        const std::vector<containers::ColumnView<Float, std::map<Int, Int>>>
            &_subfeatures,
        const containers::Match *_sample,
        const size_t _col ) const
    {
        assert_true( _col < _subfeatures.size() );
        return _subfeatures[_col][static_cast<Int>( _sample->ix_x_perip )];
    }

    /// Trivial getter
    const size_t ix_perip_used() const { return tree_->ix_perip_used(); }

    /// Whether the data used is based on a lag variable
    inline bool lag_used() const
    {
        assert_true( split_ );
        return ( split_->data_used == enums::DataUsed::time_stamps_window );
    }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        return tree_->optimization_criterion_;
    }

    /// Trivial getter
    inline const descriptors::SameUnitsContainer &same_units_categorical() const
    {
        assert_true( tree_->same_units_.same_units_categorical_ );
        return *tree_->same_units_.same_units_categorical_;
    }

    /// Trivial getter
    inline const descriptors::SameUnitsContainer &same_units_discrete() const
    {
        assert_true( tree_->same_units_.same_units_discrete_ );
        return *tree_->same_units_.same_units_discrete_;
    }

    /// Trivial getter
    inline const descriptors::SameUnitsContainer &same_units_numerical() const
    {
        assert_true( tree_->same_units_.same_units_numerical_ );
        return *tree_->same_units_.same_units_numerical_;
    }

    /// Determines whether we should skip a condition
    inline const bool skip_condition() const
    {
        if ( tree_->share_conditions() >= 1.0 )
            {
                return false;
            }
        else
            {
                return tree_->rng().random_float( 0.0, 1.0 ) >
                       tree_->share_conditions();
            }
    }

    // --------------------------------------

   private:
    /// Apply changes based on the category used - used for prediction
    void apply_by_categories_used(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        aggregations::AbstractAggregation *_aggregation ) const;

    /// Apply changes based on the category used - used for training
    void apply_by_categories_used_and_commit(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Apply changes based on the critical value
    template <typename T>
    void apply_by_critical_value(
        const T &_critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        aggregations::AbstractAggregation *_aggregation ) const;

    /// Apply changes based on the lag operator
    template <typename T>
    void apply_by_lag(
        const T &_critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        aggregations::AbstractAggregation *_aggregation ) const;

    /// Calculates the beginning and end of the categorical
    /// values considered
    std::shared_ptr<const std::vector<Int>> calculate_categories(
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Given the sorted sample containers, this returns the critical_values
    /// for discrete values
    std::vector<Float> calculate_critical_values_discrete(
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Given the sorted sample containers, this returns the critical_values
    /// for numerical values
    std::vector<Float> calculate_critical_values_numerical(
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Given the sorted sample containers, this returns the critical_values
    /// for the moving time windows (lag variables).
    std::vector<Float> calculate_critical_values_window(
        const Float _lag,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Commits the split and spawns child nodes
    void commit(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<Float, std::map<Int, Int>>>
            &_subfeatures,
        const descriptors::Split &_split,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Returns the > condition (for numerical variables)
    /// or the != condition (for categorical variables)
    std::string greater_or_not_equal_to( const std::string &_colname ) const;

    /// Copies the split give ix_max.
    containers::MatchPtrs::iterator identify_parameters(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const descriptors::Split &_column,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Partitions the iterators according by the categories.
    containers::MatchPtrs::iterator partition_by_categories_used(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) const;

    /// Partitions the iterators according by the categories.
    containers::MatchPtrs::iterator partition_by_critical_value(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) const;

    /// Returns the sum of the sample sizes of all processes
    size_t reduce_sample_size( const size_t _sample_size );

    /// Separates the sample containers for which the numerical_value is NULL
    /// - note the important difference to separate_null_values in
    /// DecisionTree/Aggregation: This separates by the numerical_value,
    /// whereas DecisionTree/Aggregation separates by the value to be
    /// aggregated!
    containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        bool _null_values_to_beginning = true ) const;

    /// Assigns a the right values to the samples for faster lookup.
    void set_samples(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) const;

    /// Appends the <= condition (for numerical variables)
    /// or the == condition (for categorical variables)
    std::string smaller_or_equal_to( const std::string &_colname ) const;

    /// Sorts the sample by the previously set categorical_value
    void sort_by_categorical_value(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Sorts the sample by the previously set numerical_value
    void sort_by_numerical_value(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Spawns two new child nodes in the fit(...) function
    void spawn_child_nodes(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _null_values_separator,
        containers::MatchPtrs::iterator _sample_container_end );

    /// Tries to impose the peripheral categorical columns as a condition
    void try_categorical_peripheral(
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the population categorical columns as a condition
    void try_categorical_population(
        const containers::DataFrameView &_population,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries whether the categorical values might constitute a good split
    void try_categorical_values(
        const size_t _column_used,
        const enums::DataUsed _data_used,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose different conditions
    void try_conditions(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const std::vector<containers::ColumnView<Float, std::map<Int, Int>>>
            &_subfeatures,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the peripheral discrete columns as a condition
    void try_discrete_peripheral(
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the population discrete columns as a condition
    void try_discrete_population(
        const containers::DataFrameView &_population,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the peripheral numerical columns as a condition
    void try_numerical_peripheral(
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the population numerical columns as a condition
    void try_numerical_population(
        const containers::DataFrameView &_population,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries whether the discrete values might constitute a good split
    void try_discrete_values(
        const size_t _column_used,
        const enums::DataUsed _data_used,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Called by try_discrete_values(...) and try_numerical_values(...)
    void try_non_categorical_values(
        const size_t _column_used,
        const enums::DataUsed _data_used,
        const size_t _sample_size,
        const std::vector<Float> _critical_values,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _null_values_separator,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries whether the numerical values might constitute a good split
    void try_numerical_values(
        const size_t _column_used,
        const enums::DataUsed _data_used,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the same units categorical as a condition
    void try_same_units_categorical(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the same units discrete as a condition
    void try_same_units_discrete(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the same units numerical as a condition
    void try_same_units_numerical(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the subfeatures as a condition
    void try_subfeatures(
        const containers::Subfeatures &_subfeatures,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to impose the difference between the time stamps as a
    /// condition
    void try_time_stamps_diff(
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const size_t _sample_size,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    /// Tries to apply a moving time window, simulating a lag variable.
    void try_window(
        const size_t _column_used,
        const enums::DataUsed _data_used,
        const size_t _sample_size,
        const Float _lag,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end,
        std::vector<descriptors::Split> *_candidate_splits );

    // --------------------------------------

   private:
    /// Child node containing samples greater than the critical value
    containers::Optional<DecisionTreeNode> child_node_greater_;

    /// Child node containing all samples smaller than or equal to the
    /// critical value
    containers::Optional<DecisionTreeNode> child_node_smaller_;

    /// Depth at this node
    Int depth_;

    /// Denotes whether this is an activated
    /// or a deactivated node. If this is an activated node,
    /// that means that all samples passed on by the parent
    /// are activated and this node can deactivate some of
    /// them and vice versa.
    bool is_activated_;

    /// If a node has a child_node_greater_ and child_node_smaller_, it must
    /// have a split, but not necessarily the other way around.
    containers::Optional<descriptors::Split> split_;

    /// Pointer to tree impl of the tree that contains this node
    DecisionTreeImpl const *tree_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

// This is templated, because it makes a difference whether _critical_value
// is of type Float or or type std::vector<Float>

template <typename T>
void DecisionTreeNode::apply_by_critical_value(
    const T &_critical_value,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    aggregations::AbstractAggregation *_aggregation ) const
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            debug_log( "Distance is zero..." );
            return;
        }

    if ( lag_used() )
        {
            apply_by_lag(
                _critical_value,
                _sample_container_begin,
                _sample_container_end,
                _aggregation );

            return;
        }

    debug_log( "Apply by critical value..." );

    if ( apply_from_above() )
        {
            if ( is_activated_ )
                {
                    debug_log( "deactivate_samples_from_above..." );

                    _aggregation->deactivate_samples_from_above(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_from_above..." );

                    _aggregation->activate_samples_from_above(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
    else
        {
            if ( is_activated_ )
                {
                    debug_log( "deactivate_samples_from_below..." );

                    _aggregation->deactivate_samples_from_below(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_from_below..." );

                    _aggregation->activate_samples_from_below(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
}

// ----------------------------------------------------------------------------

// This is templated, because it makes a difference whether _critical_value
// is of type Float or or type std::vector<Float>

template <typename T>
void DecisionTreeNode::apply_by_lag(
    const T &_critical_value,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    aggregations::AbstractAggregation *_aggregation ) const
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            return;
        }

    debug_log( "Apply by lag..." );

    if ( apply_from_above() )
        {
            if ( is_activated_ )
                {
                    debug_log( "deactivate_samples_outside_window..." );

                    _aggregation->deactivate_samples_outside_window(
                        _critical_value,
                        tree_->delta_t(),
                        aggregations::Revert::not_at_all,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_outside_window..." );

                    _aggregation->activate_samples_outside_window(
                        _critical_value,
                        tree_->delta_t(),
                        aggregations::Revert::not_at_all,
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
    else
        {
            if ( is_activated_ )
                {
                    debug_log( "deactivate_samples_in_window..." );

                    _aggregation->deactivate_samples_in_window(
                        _critical_value,
                        tree_->delta_t(),
                        aggregations::Revert::not_at_all,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_in_window..." );

                    _aggregation->activate_samples_in_window(
                        _critical_value,
                        tree_->delta_t(),
                        aggregations::Revert::not_at_all,
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
}

// ----------------------------------------------------------------------------

}  // namespace decisiontrees
}  // namespace multirel

#endif  // MULTIREL_DECISIONTREES_DECISIONTREENODE_HPP_
