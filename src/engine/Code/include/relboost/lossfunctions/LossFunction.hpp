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
    virtual void apply_inverse( RELBOOST_FLOAT* yhat_ ) const = 0;

    // Applies a transformation function. Some loss functions (such as
    // CrossEntropyLoss) require this. For others, this won't do anything at
    // all.
    virtual void apply_transformation(
        std::vector<RELBOOST_FLOAT>* yhat_ ) const = 0;

    /// Calculates first and second derivatives.
    virtual void calc_gradients(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _yhat_old ) = 0;

    /// Calculates an index that contains all non-zero samples.
    virtual void calc_sample_index(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _sample_weights ) = 0;

    /// Calculates sum_g_ and sum_h_.
    virtual void calc_sums() = 0;

    /// Calculates the update rate.
    virtual RELBOOST_FLOAT calc_update_rate(
        const std::vector<RELBOOST_FLOAT>& _yhat_old,
        const std::vector<RELBOOST_FLOAT>& _predictions ) = 0;

    /// Calculates weights given the matches.
    virtual std::vector<std::array<RELBOOST_FLOAT, 3>> calc_weights(
        const enums::Revert _revert,
        const enums::Update _update,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end ) = 0;

    /// Calculates the weights given values from a parent aggregation.
    virtual std::array<RELBOOST_FLOAT, 3> calc_weights(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) = 0;

    /// Calculates the new yhat given eta, indices and the new weights.
    virtual void calc_yhat(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) = 0;

    /// Returns a const shared ptr to the child of the LossFunction object
    virtual std::shared_ptr<const lossfunctions::LossFunction> child()
        const = 0;

    /// Commits the values for _yhat_old.
    virtual void commit() = 0;

    /// Commits the split described by the iterators.
    virtual void commit(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) = 0;

    /// Commits the values described by the indices and yhat.
    virtual void commit(
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<size_t>& _indices,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) = 0;

    /// Deletes all resources.
    virtual void clear() = 0;

    /// Returns the current depth of the loss function, so the parent
    /// aggregation, will be able to calculate its own depth.
    virtual size_t depth() const = 0;

    /// _indices refer to the values in _yhat_committed and _yhat
    /// that have actually changed.
    virtual RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) = 0;

    /// Evaluates split given matches.
    virtual RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) = 0;

    /// Evaluates and entire tree.
    virtual RELBOOST_FLOAT evaluate_tree(
        const std::vector<RELBOOST_FLOAT>& _yhat_new ) = 0;

    /// Resets the critical resources to zero.
    virtual void reset() = 0;

    /// Resizes critical values.
    virtual void resize( const size_t _size ) = 0;

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    virtual void revert( const RELBOOST_FLOAT _old_weight ) = 0;

    /// Reverts the weights to the last time commit has been called.
    virtual void revert_to_commit() = 0;

    /// Reverts the weights to the last time commit has been called.
    virtual void revert_to_commit( const std::vector<size_t>& _indices ) = 0;

    /// Trivial setter.
    virtual void set_comm( multithreading::Communicator* _comm ) = 0;

    /// Generates the predictions.
    virtual RELBOOST_FLOAT transform(
        const std::vector<RELBOOST_FLOAT>& _weights ) const = 0;

    /// Describes the type of the loss function (SquareLoss, CrossEntropyLoss,
    /// etc.)
    virtual std::string type() const = 0;
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTION_HPP_