#ifndef AUTOSQL_DECISIONTREES_DECISIONTREENODE_HPP_
#define AUTOSQL_DECISIONTREES_DECISIONTREENODE_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class DecisionTreeNode
{
   public:
    DecisionTreeNode(
        bool _is_activated, AUTOSQL_INT _depth, const DecisionTreeImpl *_tree );

    ~DecisionTreeNode() = default;

    // --------------------------------------

    /// Fits the decision tree node
    void fit(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Calling this member functions means that this node is a root
    /// and must undertake the necessary steps: It removes all samples
    /// for which the sample weight is 0, activates all remaining samples
    /// and commits this is in the aggregation and the optimization criterion
    void fit_as_root(
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
        AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end );

    /// Builds the node from a Poco::JSON::Object.
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Expresses the conditions in a form that can be understood
    /// by the monitor application (professional only)
    void to_monitor(
        const std::string &_feature_num,
        Poco::JSON::Array _node,
        Poco::JSON::Array &_conditions ) const;

    /// Extracts the node (and its children) as a Poco::JSON::Object
    Poco::JSON::Object to_json_obj();

    /// Returns the SQL condition associated with this node
    void to_sql(
        const std::string &_feature_num,
        std::vector<std::string> &_conditions,
        std::string _sql ) const;

    /// Transforms the inserted samples
    void transform(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

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
        assert( split_ );
        return split_->apply_from_above;
    }

    /// Calculates the appropriate number of critical values
    inline AUTOSQL_INT calculate_num_critical_values(
        size_t _num_samples_on_node )
    {
        return std::max(
            static_cast<AUTOSQL_INT>(
                tree_->grid_factor_ * std::sqrt( static_cast<AUTOSQL_FLOAT>(
                                          _num_samples_on_node ) ) ),
            1 );
    }

    /// Whether the data used is categorical
    inline bool categorical_data_used() const
    {
        assert( split_ );
        return (
            split_->data_used == enums::DataUsed::same_unit_categorical ||
            split_->data_used == enums::DataUsed::x_perip_categorical ||
            split_->data_used == enums::DataUsed::x_popul_categorical );
    }

    /// Trivial accessor
    inline const std::vector<AUTOSQL_INT> &categories_used() const
    {
        assert( split_ );
        assert(
            split_->categories_used->cbegin() ==
            split_->categories_used_begin );
        assert(
            split_->categories_used->cend() == split_->categories_used_end );

        return *split_->categories_used;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_INT>::const_iterator categories_used_begin()
        const
    {
        assert( split_ );
        return split_->categories_used_begin;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_INT>::const_iterator categories_used_end() const
    {
        assert( split_ );
        return split_->categories_used_end;
    }

    /// Trivial accessor
    inline multithreading::Communicator *comm()
    {
        assert( tree_->comm_ != nullptr );
        return tree_->comm_;
    }

    /// Trivial accessor
    inline AUTOSQL_INT column_used() const
    {
        assert( split_ );
        return split_->column_used;
    }

    /// Trivial accessor
    inline AUTOSQL_FLOAT critical_value() const
    {
        assert( split_ );
        return split_->critical_value;
    }

    /// Trivial accessor
    inline enums::DataUsed data_used() const
    {
        assert( split_ );
        return split_->data_used;
    }

    /// Whether the data used is discrete
    inline bool discrete_data_used() const
    {
        assert( split_ );
        return (
            split_->data_used == enums::DataUsed::same_unit_discrete ||
            split_->data_used == enums::DataUsed::x_perip_discrete ||
            split_->data_used == enums::DataUsed::x_popul_discrete );
    }

    /// Non-trivial getter
    const AUTOSQL_INT get_same_unit_categorical(
        const Sample *_sample, const AUTOSQL_INT _col )
    {
        const AUTOSQL_INT col1 =
            std::get<0>( tree_->same_units_categorical()[_col] ).ix_column_used;

        const AUTOSQL_INT col2 =
            std::get<1>( tree_->same_units_categorical()[_col] ).ix_column_used;

        const AUTOSQL_INT val1 =
            ( std::get<0>( tree_->same_units_categorical()[_col] ).data_used ==
              enums::DataUsed::x_perip_categorical )
                ? ( get_x_perip_categorical( _sample, col1 ) )
                : ( get_x_popul_categorical( _sample, col1 ) );

        const AUTOSQL_INT val2 =
            ( std::get<1>( tree_->same_units_categorical()[_col] ).data_used ==
              enums::DataUsed::x_perip_categorical )
                ? ( get_x_perip_categorical( _sample, col2 ) )
                : ( get_x_popul_categorical( _sample, col2 ) );

        // Feature -1 will be ignored during training - because it is equivalent
        // to != 0
        return ( val1 == val2 ) ? ( 0 ) : ( -1 );
    }

    /// Non-trivial getter
    const AUTOSQL_FLOAT get_same_unit_discrete(
        const Sample *_sample, const AUTOSQL_INT _col )
    {
        const AUTOSQL_INT col1 =
            std::get<0>( tree_->same_units_discrete()[_col] ).ix_column_used;

        const AUTOSQL_INT col2 =
            std::get<1>( tree_->same_units_discrete()[_col] ).ix_column_used;

        AUTOSQL_FLOAT val1 = 0.0;

        switch ( std::get<0>( tree_->same_units_discrete()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_discrete:
                    val1 = get_x_perip_discrete( _sample, col1 );
                    break;

                case enums::DataUsed::x_popul_discrete:
                    val1 = get_x_popul_discrete( _sample, col1 );
                    break;

                default:
                    assert(
                        !"get_same_unit_discrete: enums::DataUsed not known!" );
                    break;
            }

        AUTOSQL_FLOAT val2 = 0.0;

        switch ( std::get<1>( tree_->same_units_discrete()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_discrete:
                    val2 = get_x_perip_discrete( _sample, col2 );
                    break;

                case enums::DataUsed::x_popul_discrete:
                    val2 = get_x_popul_discrete( _sample, col2 );
                    break;

                default:
                    assert(
                        !"get_same_unit_discrete: enums::DataUsed not known!" );
                    break;
            }

        return val2 - val1;
    }

    /// Non-trivial getter
    const AUTOSQL_FLOAT get_same_unit_numerical(
        const Sample *_sample, const AUTOSQL_INT _col )
    {
        const AUTOSQL_INT col1 =
            std::get<0>( tree_->same_units_numerical()[_col] ).ix_column_used;

        const AUTOSQL_INT col2 =
            std::get<1>( tree_->same_units_numerical()[_col] ).ix_column_used;

        AUTOSQL_FLOAT val1 = 0.0;

        switch ( std::get<0>( tree_->same_units_numerical()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_numerical:
                    val1 = get_x_perip_numerical( _sample, col1 );
                    break;

                case enums::DataUsed::x_popul_numerical:
                    val1 = get_x_popul_numerical( _sample, col1 );
                    break;

                default:
                    assert( !"get_same_unit_numerical: enums::DataUsed not known!" );
                    break;
            }

        AUTOSQL_FLOAT val2 = 0.0;

        switch ( std::get<1>( tree_->same_units_numerical()[_col] ).data_used )
            {
                case enums::DataUsed::x_perip_numerical:
                    val2 = get_x_perip_numerical( _sample, col2 );
                    break;

                case enums::DataUsed::x_popul_numerical:
                    val2 = get_x_popul_numerical( _sample, col2 );
                    break;

                default:
                    assert( !"get_same_unit_numerical: enums::DataUsed not known!" );
                    break;
            }

        return val2 - val1;
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_time_stamps_diff(
        const Sample *_sample ) const
    {
        return tree_->population_.time_stamp(
                   _sample->ix_x_popul, ix_perip_used() ) -
               tree_->peripheral_.time_stamps()[_sample->ix_x_perip];
    }

    /// Trivial getter
    inline const AUTOSQL_INT get_x_perip_categorical(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->peripheral_.categorical()( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_x_perip_numerical(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->peripheral_.numerical()( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_x_perip_discrete(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->peripheral_.discrete()( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_INT get_x_popul_categorical(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->population_.categorical( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_x_popul_numerical(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->population_.numerical( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_x_popul_discrete(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        return tree_->population_.discrete( _sample->ix_x_popul, _col );
    }

    /// Trivial getter
    inline const AUTOSQL_FLOAT get_x_subfeature(
        const Sample *_sample, const AUTOSQL_INT _col ) const
    {
        assert( tree_->subfeatures() );
        return tree_->subfeatures()( _sample->ix_x_perip, _col );
    }

    /// Trivial getter
    const AUTOSQL_INT ix_perip_used() const { return tree_->ix_perip_used(); }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        return tree_->optimization_criterion_;
    }

    /// Identifies the global minimum and the global maximum
    template <typename T>
    void reduce_min_max( T &_min, T &_max )
    {
        containers::Summarizer::reduce_min_max( *comm(), _min, _max );
    }

    /// Trivial getter
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_categorical() const
    {
        assert( tree_->same_units_.same_units_categorical_ );
        return *tree_->same_units_.same_units_categorical_;
    }

    /// Trivial getter
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_discrete() const
    {
        assert( tree_->same_units_.same_units_discrete_ );
        return *tree_->same_units_.same_units_discrete_;
    }

    /// Trivial getter
    inline const AUTOSQL_SAME_UNITS_CONTAINER &same_units_numerical() const
    {
        assert( tree_->same_units_.same_units_numerical_ );
        return *tree_->same_units_.same_units_numerical_;
    }

    /// Determines whether we should skip a condition
    inline const bool skip_condition() const
    {
        if ( tree_->share_conditions_ >= 1.0 )
            {
                return false;
            }
        else
            {
                return tree_->rng().random_float( 0.0, 1.0 ) >
                       tree_->share_conditions_;
            }
    }

    // --------------------------------------

   private:
    /// Apply changes based on the category used - used for prediction
    void apply_by_categories_used(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Apply changes based on the category used - used for training
    void apply_by_categories_used_and_commit(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Apply changes based on the critical value
    template <typename T>
    void apply_by_critical_value(
        const T &_critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Calculates the beginning and end of the categorical
    /// values considered
    std::shared_ptr<const std::vector<AUTOSQL_INT>> calculate_categories(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Given the sorted sample containers, this returns the critical_values
    /// for discrete values
    std::vector<AUTOSQL_FLOAT> calculate_critical_values_discrete(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size );

    /// Given the sorted sample containers, this returns the critical_values
    /// for numerical values
    std::vector<AUTOSQL_FLOAT> calculate_critical_values_numerical(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size );

    /// Commits the split and spawns child nodes
    void commit(
        const descriptors::Split &_split,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Returns the > condition (for numerical variables)
    /// or the != condition (for categorical variables)
    std::string greater_or_not_equal_to( const std::string &_colname ) const;

    /// Copies the split give ix_max.
    AUTOSQL_SAMPLE_ITERATOR identify_parameters(
        const descriptors::Split &_column,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Returns the sum of the sample sizes of all processes
    size_t reduce_sample_size( size_t _sample_size );

    /// Separates the sample containers for which the numerical_value is NULL
    /// - note the important difference to separate_null_values in
    /// DecisionTree/Aggregation: This separates by the numerical_value,
    /// whereas DecisionTree/Aggregation separates by the value to be
    /// aggregated!
    AUTOSQL_SAMPLE_ITERATOR separate_null_values(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        bool _null_values_to_beginning = true );

    /// Assigns a the right values to the samples for faster lookup.
    void set_samples(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Appends the <= condition (for numerical variables)
    /// or the == condition (for categorical variables)
    std::string smaller_or_equal_to( const std::string &_colname ) const;

    /// Sorts the sample by the previously set categorical_value
    void sort_by_categorical_value(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Sorts the sample by the previously set numerical_value
    void sort_by_numerical_value(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Spawns two new child nodes in the fit(...) function
    void spawn_child_nodes(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _null_values_separator,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

    /// Tries to impose the peripheral categorical columns as a condition
    void try_categorical_peripheral(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the population categorical columns as a condition
    void try_categorical_population(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries whether the categorical values might constitute a good split
    void try_categorical_values(
        const AUTOSQL_INT _column_used,
        const enums::DataUsed _data_used,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose different conditions
    void try_conditions(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the peripheral discrete columns as a condition
    void try_discrete_peripheral(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the population discrete columns as a condition
    void try_discrete_population(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the peripheral numerical columns as a condition
    void try_numerical_peripheral(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the population numerical columns as a condition
    void try_numerical_population(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries whether the discrete values might constitute a good split
    void try_discrete_values(
        const AUTOSQL_INT _column_used,
        const enums::DataUsed _data_used,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Called by try_discrete_values(...) and try_numerical_values(...)
    void try_non_categorical_values(
        const AUTOSQL_INT _column_used,
        const enums::DataUsed _data_used,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _null_values_separator,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size,
        std::vector<AUTOSQL_FLOAT> &_critical_values,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries whether the numerical values might constitute a good split
    void try_numerical_values(
        const AUTOSQL_INT _column_used,
        const enums::DataUsed _data_used,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        const size_t _sample_size,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the same units categorical as a condition
    void try_same_units_categorical(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the same units discrete as a condition
    void try_same_units_discrete(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the same units numerical as a condition
    void try_same_units_numerical(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the subfeatures as a condition
    void try_subfeatures(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    /// Tries to impose the difference between the time stamps as a
    /// condition
    void try_time_stamps_diff(
        const size_t _sample_size,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
        std::vector<descriptors::Split> &_candidate_splits );

    // --------------------------------------

   private:
    /// Child node containing samples greater than the critical value
    containers::Optional<DecisionTreeNode> child_node_greater_;

    /// Child node containing all samples smaller than or equal to the
    /// critical value
    containers::Optional<DecisionTreeNode> child_node_smaller_;

    /// Depth at this node
    AUTOSQL_INT depth_;

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
// is of type AUTOSQL_FLOAT or or type std::vector<AUTOSQL_FLOAT>

template <typename T>
void DecisionTreeNode::apply_by_critical_value(
    const T &_critical_value,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            return;
        }

    debug_log( "Apply by critical value..." );

    if ( apply_from_above() )
        {
            if ( is_activated_ )
                {
                    debug_log( "deactivate_samples_from_above..." );

                    aggregation()->deactivate_samples_from_above(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_from_above..." );

                    aggregation()->activate_samples_from_above(
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

                    aggregation()->deactivate_samples_from_below(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    debug_log( "activate_samples_from_below..." );

                    aggregation()->activate_samples_from_below(
                        _critical_value,
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql

#endif  // AUTOSQL_DECISIONTREES_DECISIONTREENODE_HPP_
