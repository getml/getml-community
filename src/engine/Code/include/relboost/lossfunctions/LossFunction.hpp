#ifndef RELBOOST_LOSSFUNCTIONS_LOSSFUNCTION_HPP_
#define RELBOOST_LOSSFUNCTIONS_LOSSFUNCTION_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace lossfunctions
{
// ------------------------------------------------------------------------

class LossFunction
{
   public:
    // Applies the inverse of the transformation function below. Some loss
    // functions (such as CrossEntropyLoss) require this. For others, this won't
    // do anything at all.
    virtual void apply_inverse( Float* yhat_ ) const = 0;

    // Applies a transformation function. Some loss functions (such as
    // CrossEntropyLoss) require this. For others, this won't do anything at
    // all.
    virtual void apply_transformation( std::vector<Float>* yhat_ ) const = 0;

    /// Only calculates the etas given values from a parent aggregation
    /// without calculating the weights.
    /// This is needed for reverting the last split.
    virtual void calc_etas(
        const enums::Aggregation _agg,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) = 0;

    /// Calculates first and second derivatives.
    virtual void calc_gradients() = 0;

    /// Calculates the sampling rate (the share of samples that will be
    /// drawn for each feature).
    virtual void calc_sampling_rate(
        const unsigned int _seed,
        const Float _sampling_factor,
        multithreading::Communicator* _comm ) = 0;

    /// Calculates sum_g_ and sum_h_.
    virtual void calc_sums() = 0;

    /// Calculates the update rate.
    virtual Float calc_update_rate(
        const std::vector<Float>& _predictions ) = 0;

    /// Calculates the weights given values from a parent aggregation.
    virtual std::array<Float, 3> calc_weights(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) = 0;

    /// Calculates weights given the matches.
    virtual std::vector<std::array<Float, 3>> calc_weights(
        const enums::Revert _revert,
        const enums::Update _update,
        const Float _min_num_samples,
        const Float _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end ) = 0;

    /// Calculates the new yhat given eta, indices and the new weights.
    virtual void calc_yhat(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) = 0;

    /// Returns a const shared ptr to the child of the LossFunction object
    virtual std::shared_ptr<const lossfunctions::LossFunction> child()
        const = 0;

    /// Commits the values for yhat_old_.
    virtual void commit() = 0;

    /// Commits the split described by the iterators.
    virtual void commit(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) = 0;

    /// Commits the values described by the indices and yhat.
    virtual void commit(
        const std::vector<size_t>& _indices,
        const std::array<Float, 3>& _weights ) = 0;

    /// Deletes all resources.
    virtual void clear() = 0;

    /// Returns the current depth of the loss function, so the parent
    /// aggregation, will be able to calculate its own depth.
    virtual size_t depth() const = 0;

    /// _indices refer to the values in _yhat_committed and _yhat
    /// that have actually changed.
    virtual Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2 ) = 0;

    /// Evaluates split given matches.
    virtual Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights ) = 0;

    /// Evaluates and entire tree.
    virtual Float evaluate_tree(
        const Float _update_rate, const std::vector<Float>& _yhat_new ) = 0;

    /// Initializes yhat_old_ by setting it to the initial prediction.
    virtual void init_yhat_old( const Float _initial_prediction ) = 0;

    /// Generates the sample weights.
    virtual const std::shared_ptr<const std::vector<Float>>
    make_sample_weights() = 0;

    /// Reduces the predictions.
    virtual void reduce_predictions( std::vector<Float>* _predictions ) = 0;

    /// Resets the critical resources to zero.
    virtual void reset() = 0;

    /// Resets yhat_old to the initial prediction.
    virtual void reset_yhat_old() = 0;

    /// Resizes critical values.
    virtual void resize( const size_t _size ) = 0;

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    virtual void revert( const Float _old_weight ) = 0;

    /// Reverts the weights to the last time commit has been called.
    virtual void revert_to_commit() = 0;

    /// Reverts the weights to the last time commit has been called.
    virtual void revert_to_commit( const std::vector<size_t>& _indices ) = 0;

    /// Trivial setter.
    virtual void set_comm( multithreading::Communicator* _comm ) = 0;

    /// Generates the predictions.
    virtual Float transform( const std::vector<Float>& _weights ) const = 0;

    /// Describes the type of the loss function (SquareLoss, CrossEntropyLoss,
    /// etc.)
    virtual std::string type() const = 0;

    /// Updates yhat_old_ by adding the predictions.
    virtual void update_yhat_old(
        const Float _update_rate, const std::vector<Float>& _predictions ) = 0;
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTION_HPP_
