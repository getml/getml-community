#ifndef RELBOOST_AGGREGATIONS_SUM_HPP_
#define RELBOOST_AGGREGATIONS_SUM_HPP_

namespace relboost
{
namespace aggregations
{
// -------------------------------------------------------------------------

/// Note that an aggregation implements the LossFunction trait - thus
/// aggregations just like loss functions to the tree.
class Sum : public lossfunctions::LossFunction
{
    // -----------------------------------------------------------------

   public:
    Sum( const std::shared_ptr<lossfunctions::LossFunction>& _child,
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
              _child.get(), &eta1_, &eta2_, &indices_, &indices_current_ ) )
    {
        resize( _output.nrows() );
    }

    Sum( const std::shared_ptr<AggregationIndex>& _agg_index,
         const std::shared_ptr<lossfunctions::LossFunction>& _child,
         const containers::DataFrame& _input,
         const containers::DataFrameView& _output,
         multithreading::Communicator* _comm )
        : Sum( _child, _input, _output, _comm )
    {
        agg_index_ = _agg_index;
    }

    Sum( const std::shared_ptr<lossfunctions::LossFunction>& _child )
        : child_( _child ),
          comm_( nullptr ),
          depth_( _child->depth() + 1 ),
          indices_( 0 ),
          indices_current_( 0 ),
          num_samples_1_( 0.0 ),
          num_samples_2_( 0.0 ),
          impl_( AggregationImpl(
              _child.get(), &eta1_, &eta2_, &indices_, &indices_current_ ) )

    {
    }

    Sum( const std::shared_ptr<AggregationIndex>& _agg_index,
         const std::shared_ptr<lossfunctions::LossFunction>& _child )
        : Sum( _child )
    {
        agg_index_ = _agg_index;
    }

    ~Sum() = default;

    // -----------------------------------------------------------------

   public:
    /// Commits the values described by the _indices.
    void commit(
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2,
        const std::vector<size_t>& _indices,
        const std::array<Float, 3>& _weights ) final;

    /// Commits the split described by the iterators and the weights.
    void commit(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) final;

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
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2 ) final;

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

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    void revert( const Float _old_weight ) final;

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

    /// Calculates an index that contains all non-zero samples.
    void calc_sample_index(
        const std::shared_ptr<const std::vector<Float>>& _sample_weights )
    {
        child_->calc_sample_index( _sample_weights );
    }

    /// Calculates sum_g_ and sum_h_.
    void calc_sums() { child_->calc_sums(); }

    /// Calculates the update rate.
    Float calc_update_rate( const std::vector<Float>& _predictions ) final
    {
        return child_->calc_update_rate( _predictions );
    }

    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2 ) final
    {
        child_->calc_yhat(
            _agg,
            _old_weight,
            _new_weights,
            indices_.unique_integers(),
            eta1_,
            eta2_ );
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

    /// Evaluates an entire tree.
    Float evaluate_tree(
        const Float _update_rate, const std::vector<Float>& _yhat_new ) final
    {
        assert_true( false && "ToDO" );
        return child_->evaluate_tree( _update_rate, _yhat_new );
    }

    /// Trivial getter
    size_t depth() const final { return depth_; };

    /// Initializes yhat_old_ by setting it to the initial prediction.
    void init_yhat_old( const Float _initial_prediction ) final
    {
        assert_true( false && "ToDO" );
    }

    /// Resets the critical resources to zero.
    void reset() final { impl_.reset(); }

    /// Resizes critical resources.
    void resize( size_t _size ) final { impl_.resize( _size ); }

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit() final { impl_.revert_to_commit(); }

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit( const std::vector<size_t>& _indices ) final
    {
        revert_to_commit();
    };

    /// Trivial setter.
    void set_comm( multithreading::Communicator* _comm ) final
    {
        comm_ = _comm;
        child_->set_comm( _comm );
    }

    /// Describes the type of the aggregation.
    std::string type() const final { return "SUM"; }

    /// Updates yhat_old_ by adding the predictions.
    void update_yhat_old(
        const Float _update_rate, const std::vector<Float>& _predictions ) final
    {
        assert_true( false && "ToDO" );
    }

    // -----------------------------------------------------------------

   private:
    /// Calculates _eta1 and _eta2 for ALL matches, not just the difference to
    /// the last split.
    void calc_all(
        const enums::Revert _revert,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end );

    /// Calculates eta1_ and eta2_ for only the difference to the last split.
    void calc_diff(
        const enums::Revert _revert,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end );

    // -----------------------------------------------------------------

   private:
    /// Calculates the new yhat assuming that this is the
    /// lowest aggregation.
    void calc_yhat(
        const Float _old_weight, const std::array<Float, 3>& _new_weights )
    {
        child_->calc_yhat(
            enums::Aggregation::sum,
            _old_weight,
            _new_weights,
            indices_.unique_integers(),
            eta1_,
            eta2_ );
    }

    // -----------------------------------------------------------------

   private:
    /// The aggregation index is needed by the intermediate aggregation.
    std::shared_ptr<AggregationIndex> agg_index_;

    /// Either The next higher level of aggregation or the loss function.
    const std::shared_ptr<lossfunctions::LossFunction>& child_;

    /// A communicator used for exchaning information between threads.
    multithreading::Communicator* comm_;

    /// Depth at this aggregation.
    const size_t depth_;

    /// Parameters for weight 1.
    std::vector<Float> eta1_;

    /// Parameters for weight 2.
    std::vector<Float> eta2_;

    /// Keeps track of the samples that have been altered.
    containers::IntSet indices_;

    /// Keeps track of the samples that have been altered since the last split.
    containers::IntSet indices_current_;

    /// The join keys of the input table.
    std::vector<containers::Column<Int>> input_join_keys_;

    /// Total number of samples for eta1_.
    Float num_samples_1_;

    /// Total number of samples for eta2_.
    Float num_samples_2_;

    /// The indices of the output table.
    std::vector<std::shared_ptr<containers::Index>> output_indices_;

    /// Implementation class. Because impl_ depends on some other variables, it
    /// is the last member variable.
    AggregationImpl impl_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

#endif  // RELBOOST_AGGREGATIONS_SUM_HPP_
