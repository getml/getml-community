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
         const containers::DataFrame& _output )
        : child_( _child ),
          depth_( _child->depth() + 1 ),
          indices_( _output.nrows() ),
          indices_current_( _output.nrows() ),
          input_join_keys_( _input.join_keys_ ),
          output_indices_( _output.indices_ ),
          impl_( AggregationImpl(
              _child.get(), &eta1_, &eta2_, &indices_, &indices_current_ ) )
    {
        resize( _output.nrows() );
    }

    Sum( const std::shared_ptr<lossfunctions::LossFunction>& _child )
        : child_( _child ),
          depth_( _child->depth() + 1 ),
          indices_( 0 ),
          indices_current_( 0 ),
          impl_( AggregationImpl(
              _child.get(), &eta1_, &eta2_, &indices_, &indices_current_ ) )
    {
    }

    ~Sum() = default;

    // -----------------------------------------------------------------

   public:
    /// Commits the values described by the _indices.
    void commit(
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<size_t>& _indices,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) final;

    /// Commits the split described by the iterators and the weights.
    void commit(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) final;

    /// Calculates _indices, _eta1 and _eta2 given matches.
    std::vector<std::array<RELBOOST_FLOAT, 3>> calc_weights(
        const enums::Revert _revert,
        const enums::Update _update,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end ) final;

    /// Calculates _indices, _eta1 and _eta2  given the previous
    /// iteration's variables.
    std::array<RELBOOST_FLOAT, 3> calc_weights(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) final;

    /// Returns the loss reduction associated with a split.
    RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) final;

    /// Returns the loss reduction associated with a split.
    RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) final;

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    void revert( const RELBOOST_FLOAT _old_weight ) final;

    /// Generates the predictions.
    RELBOOST_FLOAT transform(
        const std::vector<RELBOOST_FLOAT>& _weights ) const final;

    // -----------------------------------------------------------------

   public:
    // Applies the inverse of the transformation function below. Some loss
    // functions (such as CrossEntropyLoss) require this. For others, this won't
    // do anything at all.
    void apply_inverse( RELBOOST_FLOAT* yhat_ ) const final {}

    // Applies a transformation function. Some loss functions (such as
    // CrossEntropyLoss) require this. For others, this won't do anything at
    // all.
    void apply_transformation( std::vector<RELBOOST_FLOAT>* yhat_ ) const final
    {
    }

    /// Aggregations do not calculate gradients, only real loss functions do.
    void calc_gradients(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& _yhat_old )
        final
    {
        child_->calc_gradients( _yhat_old );
    }

    /// Calculates an index that contains all non-zero samples.
    void calc_sample_index(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _sample_weights )
    {
        child_->calc_sample_index( _sample_weights );
    }

    /// Calculates sum_g_ and sum_h_.
    void calc_sums() { child_->calc_sums(); }

    /// Calculates the update rate.
    RELBOOST_FLOAT calc_update_rate(
        const std::vector<RELBOOST_FLOAT>& _yhat_old,
        const std::vector<RELBOOST_FLOAT>& _predictions ) final
    {
        return child_->calc_update_rate( _yhat_old, _predictions );
    }

    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) final
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
    void clear() final { impl_.resize( 0 ); }

    /// Commits _yhat_old.
    void commit() final { child_->commit(); }

    /// Evaluates an entire tree.
    RELBOOST_FLOAT evaluate_tree(
        const std::vector<RELBOOST_FLOAT>& _yhat_new ) final
    {
        return child_->evaluate_tree( _yhat_new );
    }

    /// Trivial getter
    size_t depth() const final { return depth_; };

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

    /// Describes the type of the aggregation.
    std::string type() const final { return "SUM"; }

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
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights )
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
    /// Either The next higher level of aggregation or the loss function.
    const std::shared_ptr<lossfunctions::LossFunction>& child_;

    /// Depth at this aggregation.
    const size_t depth_;

    /// Parameters for weight 1.
    std::vector<RELBOOST_FLOAT> eta1_;

    /// Parameters for weight 2.
    std::vector<RELBOOST_FLOAT> eta2_;

    /// Keeps track of the samples that have been altered.
    containers::IntSet indices_;

    /// Keeps track of the samples that have been altered since the last split.
    containers::IntSet indices_current_;

    /// The join keys of the input table.
    std::vector<containers::Matrix<RELBOOST_INT>> input_join_keys_;

    /// The indices of the output table.
    std::vector<std::shared_ptr<RELBOOST_INDEX>> output_indices_;

    /// Implementation class. Because impl_ depends on some other variables, it
    /// is the last member variable.
    AggregationImpl impl_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

#endif  // RELBOOST_AGGREGATIONS_SUM_HPP_
