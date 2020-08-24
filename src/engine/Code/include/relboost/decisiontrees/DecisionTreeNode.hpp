#ifndef RELBOOST_DECISIONTREES_DECISIONTREENODE_HPP_
#define RELBOOST_DECISIONTREES_DECISIONTREENODE_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

class DecisionTreeNode
{
   public:
    DecisionTreeNode(
        const utils::ConditionMaker& _condition_maker,
        const Int _depth,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        const Float _weight,
        multithreading::Communicator* _comm );

    DecisionTreeNode(
        const utils::ConditionMaker& _condition_maker,
        const Int _depth,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        const Poco::JSON::Object& _obj );

    ~DecisionTreeNode() = default;

    // -----------------------------------------------------------------

   public:
    /// Updates the column importances based on the data of the this node.
    void column_importances( utils::ImportanceMaker* _importance_maker ) const;

    /// Fits the decision tree node.
    void fit(
        const containers::DataFrameView& _output,
        const std::optional<containers::DataFrame>& _input,
        const containers::Subfeatures& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        Float* _intercept );

    /// Expresses the DecisionTreeNode as a Poco::JSON::Object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Expresses the DecisionTreeNode as SQL code.
    void to_sql(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_num,
        const std::string& _sql,
        std::vector<std::string>* _conditions ) const;

    /// Transforms the data to form a prediction.
    Float transform(
        const containers::DataFrameView& _output,
        const std::optional<containers::DataFrame>& _input,
        const containers::Subfeatures& _subfeatures,
        const containers::Match& _match ) const;

    /// Multiplies all weights on the nodes with _update_rate. This is how
    /// update rates are implemented in relboost.
    void update_weights( const Float _update_rate );

    // -----------------------------------------------------------------

   public:
    /// Trivial setter.
    void set_comm( multithreading::Communicator* _comm )
    {
        comm_ = _comm;
        if ( child_greater_ )
            {
                assert_true( child_smaller_ );
                child_greater_->set_comm( _comm );
                child_smaller_->set_comm( _comm );
            }
    }

    // -----------------------------------------------------------------

   private:
    /// Add to the candidate splits.
    void add_candidates(
        const enums::Revert _revert,
        const enums::Update _update,
        const Float _old_intercept,
        const containers::Split& _split,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _last_it,
        const std::vector<containers::Match>::iterator _it,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::CandidateSplit>* _candidates );

    /// DEBUG ONLY:Makes sure that the candidates and the max element are
    /// aligned over all threads.
    void assert_aligned(
        const std::vector<containers::CandidateSplit>::iterator _begin,
        const std::vector<containers::CandidateSplit>::iterator _end,
        const std::vector<containers::CandidateSplit>::iterator _it );

    /// Expresses the split in SQL as passed on to the greater node.
    std::string condition_greater() const;

    /// Expresses the split in SQL as passed on to the smaller node.
    std::string condition_smaller() const;

    /// Partitions a set of matches according to the split.
    std::vector<containers::Match>::iterator partition(
        const containers::DataFrameView& _output,
        const std::optional<containers::DataFrame>& _input,
        const containers::Subfeatures& _subfeatures,
        const containers::Split& _split,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end ) const;

    /// Try all possible splits.
    std::vector<containers::CandidateSplit> try_all(
        const Float _old_intercept,
        const containers::DataFrameView& _output,
        const std::optional<containers::DataFrame>& _input,
        const containers::Subfeatures& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end );

    /// Helper function for all functions that try categorical columns.
    void try_categorical(
        const enums::Revert _revert,
        const Int _min,
        const std::shared_ptr<const std::vector<Int>> _critical_values,
        const size_t _num_column,
        const Float _old_intercept,
        const enums::DataUsed _data_used,
        const std::vector<size_t>& _indptr,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try categorical input columns as splits.
    void try_categorical_input(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try categorical output columns as splits.
    void try_categorical_output(
        const Float _old_intercept,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try discrete input columns as splits.
    void try_discrete_input(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try discrete output columns as splits.
    void try_discrete_output(
        const Float _old_intercept,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Called by all methods dealing with numerical or discrete columns.
    void try_numerical_or_discrete(
        const enums::DataUsed _data_used,
        const size_t _col1,
        const size_t _col2,
        const Float _old_intercept,
        const Float _max,
        const Float _step_size,
        const std::vector<size_t>& _indptr,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try numerical input columns as splits.
    void try_numerical_input(
        const Float _old_intercept,
        const containers::DataFrame& _peripheral,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try numerical output columns as splits.
    void try_numerical_output(
        const Float _old_intercept,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try splitting on whether columns are the same.
    void try_same_units_categorical(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try splitting on same units discrete.
    void try_same_units_discrete(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try splitting on same units numerical.
    void try_same_units_numerical(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try subfeatures as splits.
    void try_subfeatures(
        const Float _old_intercept,
        const containers::Subfeatures& _subfeatures,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    /// Try a window function on the difference between time stamps
    /// in input and output as splits.
    void try_time_stamps_window(
        const Float _old_intercept,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        std::vector<containers::Match>* _bins,
        std::vector<containers::CandidateSplit>* _candidates );

    // -----------------------------------------------------------------

   private:
    /// Calculates the appropriate number of bins for a numerical column.
    size_t calc_num_bins(
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _end )
    {
        assert_true( _end >= _begin );
        auto num_matches = static_cast<size_t>( std::distance( _begin, _end ) );
        utils::Reducer::reduce( std::plus<size_t>(), &num_matches, &comm() );
        return std::max(
            static_cast<size_t>(
                std::sqrt( static_cast<Float>( num_matches ) ) ),
            static_cast<size_t>( 1 ) );
    }

    /// Trivial (private) accessor.
    multithreading::Communicator& comm()
    {
        assert_true( comm_ != nullptr );
        return *comm_;
    }

    /// Trivial (private) accessor.
    const Hyperparameters& hyperparameters() const
    {
        assert_true( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Trivial (private) accessor
    const containers::Placeholder& input() const
    {
        assert_true( input_ );
        return *input_;
    }

    /// Whether a DataUsed is same units.
    bool is_same_units( const enums::DataUsed _data_used ) const
    {
        return (
            _data_used == enums::DataUsed::same_units_categorical ||
            _data_used == enums::DataUsed::same_units_discrete ||
            _data_used == enums::DataUsed::same_units_discrete_ts ||
            _data_used == enums::DataUsed::same_units_numerical ||
            _data_used == enums::DataUsed::same_units_numerical_ts );
    }

    /// Trivial accessor.
    lossfunctions::LossFunction& loss_function()
    {
        assert_true( loss_function_ );
        return *loss_function_;
    }

    /// Trivial (private) accessor
    const containers::Placeholder& output() const
    {
        assert_true( output_ );
        return *output_;
    }

    // -----------------------------------------------------------------

   private:
    /// raw pointer to the communicator.
    multithreading::Communicator* comm_;

    /// Branch when value is greater than critical_value
    containers::Optional<DecisionTreeNode> child_greater_;

    /// Branch when value is smaller or equal to critical_value
    containers::Optional<DecisionTreeNode> child_smaller_;

    /// Encoding for the categorical data, maps integers to underlying category.
    const utils::ConditionMaker condition_maker_;

    /// Depth of the current node.
    const Int depth_;

    /// Hyperparameters used to train the relboost model
    const std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The input table used (we keep it, because we need the colnames)
    containers::Optional<containers::Placeholder> input_;

    /// Reference to the loss function used.
    const std::shared_ptr<lossfunctions::LossFunction> loss_function_;

    /// The reduction of the loss achieved by this node (if applicable)
    Float loss_reduction_;

    /// The output table used (we keep it, because we need the colnames)
    containers::Optional<containers::Placeholder> output_;

    /// Describes the split that this node uses.
    containers::Split split_;

    /// The weight on the node.
    const Float weight_;
};

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_DECISIONTREES_DECISIONTREENODE_HPP_
