#ifndef RELBOOST_AGGREGATIONS_AVG_HPP_
#define RELBOOST_AGGREGATIONS_AVG_HPP_

namespace relboost
{
namespace aggregations
{
// -------------------------------------------------------------------------

/// Note that an aggregation implements the LossFunction trait - thus
/// aggregations look just like loss functions to the tree.
class Avg : public lossfunctions::LossFunction
{
    // -----------------------------------------------------------------

   public:
    Avg( const std::shared_ptr<lossfunctions::LossFunction>& _child,
         const std::vector<const containers::Match*>& _matches_ptr,
         const containers::DataFrame& _input,
         const containers::DataFrameView& _output,
         multithreading::Communicator* _comm )
        : child_( _child ),
          comm_( _comm ),
          depth_( _child->depth() + 1 ),
          indices_( _output.nrows() ),
          indices_current_( _output.nrows() ),
          input_join_keys_( _input.join_keys() ),
          num_samples_1_( 0.0 ),
          num_samples_2_( 0.0 ),
          output_indices_( _output.indices() ),
          impl_( AggregationImpl(
              _child.get(),
              &eta1_,
              &eta1_old_,
              &eta2_,
              &eta2_old_,
              &indices_,
              &indices_current_ ) )
    {
        resize( _output.nrows() );

        init_count_committed( _matches_ptr );
    }

    Avg( const std::shared_ptr<AggregationIndex>& _agg_index,
         const std::shared_ptr<lossfunctions::LossFunction>& _child,
         const containers::DataFrame& _input,
         const containers::DataFrameView& _output,
         multithreading::Communicator* _comm )
        : Avg( _child,
               std::vector<const containers::Match*>( 0 ),
               _input,
               _output,
               _comm )
    {
        agg_index_ = _agg_index;
        intermediate_agg_ =
            std::make_shared<IntermediateAggregationImpl>( agg_index_, _child );
    }

    Avg( const std::shared_ptr<lossfunctions::LossFunction>& _child )
        : child_( _child ),
          comm_( nullptr ),
          depth_( _child->depth() + 1 ),
          indices_( 0 ),
          indices_current_( 0 ),
          impl_( AggregationImpl(
              _child.get(),
              &eta1_,
              &eta1_old_,
              &eta2_,
              &eta2_old_,
              &indices_,
              &indices_current_ ) )
    {
    }

    Avg( const std::shared_ptr<AggregationIndex>& _agg_index,
         const std::shared_ptr<lossfunctions::LossFunction>& _child )
        : Avg( _child )
    {
        agg_index_ = _agg_index;
        intermediate_agg_ =
            std::make_shared<IntermediateAggregationImpl>( agg_index_, _child );
    }

    ~Avg() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) final;

    /// Calculates _indices, _eta1 and _eta2 given matches.
    std::vector<std::array<Float, 3>> calc_weights(
        const enums::Revert _revert,
        const enums::Update _update,
        const Float _min_num_samples,
        const Float _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end ) final;

    /// Calculates _indices, _eta1 and _eta2  given the previous
    /// iteration's variables.
    std::array<Float, 3> calc_weights(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) final;

    /// Commits the split described by the iterators and the weights.
    void commit(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) final;

    /// Returns the loss reduction associated with a split.
    Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights ) final;

    /// Returns the loss reduction associated with a split.
    Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2 ) final;

    /// Resizes critical resources.
    void resize( size_t _size ) final;

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    void revert( const Float _old_weight ) final;

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit() final;

    /// Generates the predictions.
    Float transform( const std::vector<Float>& _weights ) const final;

    // -----------------------------------------------------------------

   public:
    // Applies the inverse of the transformation function below. Some loss
    // functions (such as CrossEntropyLoss) require this. For others, this won't
    // do anything at all.
    void apply_inverse( Float* yhat_ ) const final {}

    // Applies a transformation function. Some loss functions (such as
    // CrossEntropyLoss) require this. For others, this won't do anything at
    // all.
    void apply_transformation( std::vector<Float>* yhat_ ) const final {}

    /// Aggregations do not calculate gradients, only real loss functions do.
    void calc_gradients() final { child_->calc_gradients(); }

    /// Calculates the sampling rate (the share of samples that will be
    /// drawn for each feature).
    void calc_sampling_rate(
        const unsigned int _seed,
        const Float _sampling_factor,
        multithreading::Communicator* _comm ) final
    {
        child_->calc_sampling_rate( _seed, _sampling_factor, _comm );
    }

    /// Calculates sum_g_ and sum_h_.
    void calc_sums() { child_->calc_sums(); }

    /// Calculates the update rate.
    Float calc_update_rate( const std::vector<Float>& _predictions ) final
    {
        return child_->calc_update_rate( _predictions );
    }

    /// Returns a const shared pointer to the child.
    std::shared_ptr<const lossfunctions::LossFunction> child() const final
    {
        return child_;
    }

    /// Deletes all resources.
    void clear() final { resize( 0 ); }

    /// Commits yhat_old_.
    void commit() final { child_->commit(); }

    /// Commits the values described by the _indices.
    void commit(
        const std::vector<size_t>& _indices,
        const std::array<Float, 3>& _weights ) final
    {
        child_->commit( intermediate_agg().indices(), _weights );
        intermediate_agg().reset();
    }

    /// Evaluates an entire tree.
    Float evaluate_tree(
        const Float _update_rate, const std::vector<Float>& _predictions ) final
    {
        return child_->evaluate_tree( _update_rate, _predictions );
    }

    /// Trivial getter
    size_t depth() const final { return depth_; };

    /// Initializes yhat_old_ by setting it to the initial prediction.
    void init_yhat_old( const Float _initial_prediction ) final
    {
        assert_true( false && "TODO" );
    }

    /// Generates the sample weights.
    const std::shared_ptr<const std::vector<Float>> make_sample_weights() final
    {
        const auto sample_weights_parent = child_->make_sample_weights();
        return agg_index().make_sample_weights( sample_weights_parent );
    }

    /// Reduces the predictions - this is called by the decision tree.
    void reduce_predictions( std::vector<Float>* _predictions ) final
    {
        *_predictions =
            intermediate_agg().reduce_predictions( true, *_predictions );
        child_->reduce_predictions( _predictions );
    }

    /// Resets the critical resources to zero.
    void reset() final
    {
        if ( intermediate_agg_ )
            intermediate_agg().reset();
        else
            impl_.reset();
    }

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit( const std::vector<size_t>& _indices ) final
    {
        child_->revert_to_commit( intermediate_agg().indices() );
        intermediate_agg().reset();
    };

    /// Trivial setter.
    void set_comm( multithreading::Communicator* _comm ) final
    {
        comm_ = _comm;
        child_->set_comm( _comm );
    }

    /// Describes the type of the aggregation.
    std::string type() const final { return "AVG"; }

    /// Updates yhat_old_ by adding the predictions.
    void update_yhat_old(
        const Float _update_rate, const std::vector<Float>& _predictions ) final
    {
        child_->update_yhat_old( _update_rate, _predictions );
    }

    // -----------------------------------------------------------------

   private:
    /// Activates a set of indices by increasing the number of counts.
    void activate(
        const containers::IntSet::Iterator _begin,
        const containers::IntSet::Iterator _end );

    /// Calculates eta1_ and eta2_ for ALL matches, not just the difference to
    /// the last split.
    void calc_all(
        const enums::Revert _revert,
        const Float _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end );

    /// Calculates eta1_ and eta2_ for only the difference to the last split.
    void calc_diff(
        const Float _old_weight,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end );

    /// Calculates the new yhat assuming that this is the
    /// lowest aggregation.
    void calc_yhat(
        const Float _old_weight, const std::array<Float, 3>& _new_weights );

    /// Dectivates a set of indices by decreasing the number of counts.
    void deactivate(
        const Float _old_weight,
        const containers::IntSet::Iterator _begin,
        const containers::IntSet::Iterator _end );

    /// Initialized count_committed_ by calculating the total count.
    void init_count_committed(
        const std::vector<const containers::Match*>& _matches_ptr );

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    AggregationIndex& agg_index()
    {
        assert_true( agg_index_ );
        return *agg_index_;
    }

    /// Trivial (private) accessor
    IntermediateAggregationImpl& intermediate_agg()
    {
        assert_true( intermediate_agg_ );
        return *intermediate_agg_;
    }

    // -----------------------------------------------------------------

   private:
    /// The aggregation index is needed by the intermediate aggregation.
    std::shared_ptr<AggregationIndex> agg_index_;

    /// Either the next higher level of aggregation or the loss function.
    const std::shared_ptr<lossfunctions::LossFunction>& child_;

    /// A communicator used for exchaning information between threads.
    multithreading::Communicator* comm_;

    /// The total number of counts minus those matches for which in the
    /// corresponding weight is NAN.
    std::vector<Float> count_committed_;

    /// Counts number of peripheral samples per population sample in eta1_
    std::vector<Float> count1_;

    /// Counts number of peripheral samples per population sample in eta2_
    std::vector<Float> count2_;

    /// The count ratio is necessary for calculatring fixed weights.
    std::vector<Float> count_ratio_1_;

    /// The count ratio is necessary for calculatring fixed weights.
    std::vector<Float> count_ratio_2_;

    /// Depth at this aggregation.
    const size_t depth_;

    /// Parameters for weight 1.
    std::vector<Float> eta1_;

    /// Parameters for weight 1 when weight 2 is NULL.
    std::vector<Float> eta1_2_null_;

    /// Parameters for weight 1 when weight 2 is NULL as of the last split.
    std::vector<Float> eta1_2_null_old_;

    /// Parameters for weight 1 as of the last split.
    std::vector<Float> eta1_old_;

    /// Parameters for weight 2.
    std::vector<Float> eta2_;

    /// Parameters for weight 2 when weight 1 is NULL.
    std::vector<Float> eta2_1_null_;

    /// Parameters for weight 1 when weight 2 is NULL as of the last split.
    std::vector<Float> eta2_1_null_old_;

    /// Parameters for weight 2 as of the last split.
    std::vector<Float> eta2_old_;

    /// Eta used by the old weight - needed for calculating the regularization.
    std::vector<Float> eta_old_;

    /// Keeps track of the samples that have been altered.
    containers::IntSet indices_;

    /// Keeps track of the samples that have been altered since the last split.
    containers::IntSet indices_current_;

    /// The join keys of the input table.
    std::vector<containers::Column<Int>> input_join_keys_;

    /// The implementation of the intermediate aggregation.
    std::shared_ptr<IntermediateAggregationImpl> intermediate_agg_;

    /// Total number of samples for eta1_.
    Float num_samples_1_;

    /// Total number of samples for eta2_.
    Float num_samples_2_;

    /// The indices of the output table.
    std::vector<std::shared_ptr<containers::Index>> output_indices_;

    /// The fixed weights when weight 2 is NULL.
    std::vector<Float> w_fixed_1_;

    /// The fixed weights when weight 2 is NULL as of the last split.
    std::vector<Float> w_fixed_1_old_;

    /// The fixed weights when weight 1 is NULL.
    std::vector<Float> w_fixed_2_;

    /// The fixed weights when weight 1 is NULL as of the last split.
    std::vector<Float> w_fixed_2_old_;

    /// The fixed that have been committed. In cases of a simple star schema,
    /// this is identical to yhat_committed_.
    std::vector<Float> w_fixed_committed_;

    /// Implementation class. Because impl_ depends on some other variables, it
    /// is the last member variable.
    AggregationImpl impl_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

#endif  // RELBOOST_AGGREGATIONS_AVG_HPP_
