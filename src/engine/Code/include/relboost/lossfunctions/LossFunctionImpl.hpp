#ifndef RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONIMPL_HPP_
#define RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONIMPL_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace lossfunctions
{
// ------------------------------------------------------------------------

class LossFunctionImpl
{
    // -----------------------------------------------------------------

   public:
    LossFunctionImpl(
        const std::vector<RELBOOST_FLOAT>& _g,
        const std::vector<RELBOOST_FLOAT>& _h,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _sample_weights,
        const RELBOOST_FLOAT& _sum_g,
        const RELBOOST_FLOAT& _sum_h,
        const RELBOOST_FLOAT& _sum_h_yhat,
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& _targets )
        : g_( _g ),
          h_( _h ),
          hyperparameters_( _hyperparameters ),
          sample_weights_( _sample_weights ),
          sum_g_( _sum_g ),
          sum_h_( _sum_h ),
          sum_h_yhat_committed_( _sum_h_yhat ),
          targets_( _targets )
    {
    }

    ~LossFunctionImpl() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates the regularization of the weights.
    RELBOOST_FLOAT calc_regularization_reduction(
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<size_t>& _indices,
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const RELBOOST_FLOAT _sum_sample_weights,
        multithreading::Communicator* _comm ) const;

    /// Calculates the sample index (which contains the indices of all samples
    /// with non-zero sample weight).
    std::vector<size_t> calc_sample_index(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _sample_weights ) const;

    /// Calculates _sum_g and _sum_h.
    void calc_sums(
        const std::vector<size_t>& _sample_index,
        const std::vector<RELBOOST_FLOAT>& _sample_weights,
        RELBOOST_FLOAT* _sum_g,
        RELBOOST_FLOAT* _sum_h,
        RELBOOST_FLOAT* _sum_sample_weights,
        multithreading::Communicator* _comm ) const;

    /// Calculates the update rate.
    RELBOOST_FLOAT calc_update_rate(
        const std::vector<RELBOOST_FLOAT>& _yhat_old,
        const std::vector<RELBOOST_FLOAT>& _predictions,
        multithreading::Communicator* _comm ) const;

    /// Calculates two new weights given matches. This just reduces to the
    /// normal XGBoost approach.
    std::vector<std::array<RELBOOST_FLOAT, 3>> calc_weights(
        const enums::Update _update,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm ) const;

    /// Commits yhat.
    RELBOOST_FLOAT commit(
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _yhat,
        std::vector<RELBOOST_FLOAT>* _yhat_committed ) const;

    /// Resets _yhat to _yhat_committed.
    void revert_to_commit(
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        std::vector<RELBOOST_FLOAT>* _yhat ) const;

    /// Generates the predictions.
    void transform(
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        const std::vector<RELBOOST_FLOAT>& _weights,
        std::vector<RELBOOST_FLOAT>* _predictions ) const;

    // -----------------------------------------------------------------

   public:
    /// Calculates two new weights given eta and indices.
    std::array<RELBOOST_FLOAT, 3> calc_weights(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        multithreading::Communicator* _comm ) const
    {
        if ( _agg == enums::Aggregation::avg ||
             _agg == enums::Aggregation::sum )
            {
                return calc_weights(
                    _old_weight,
                    _indices,
                    _eta1,
                    _eta2,
                    _yhat_committed,
                    _comm );
            }
        else if (
            _agg == enums::Aggregation::avg_first_null ||
            _agg == enums::Aggregation::avg_second_null )
            {
                return calc_weights_avg_null(
                    _agg,
                    _old_weight,
                    _indices,
                    _eta1,
                    _eta2,
                    _yhat_committed,
                    _comm );
            }
        else
            {
                assert( false && "Aggregation not known!" );
            }
    }
    // -----------------------------------------------------------------

    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        std::vector<RELBOOST_FLOAT>* _yhat ) const
    {
        if ( _agg == enums::Aggregation::avg ||
             _agg == enums::Aggregation::sum )
            {
                calc_yhat(
                    _old_weight,
                    _new_weights,
                    _indices,
                    _eta1,
                    _eta2,
                    _yhat_committed,
                    _yhat );
            }
        else if (
            _agg == enums::Aggregation::avg_first_null ||
            _agg == enums::Aggregation::avg_second_null )
            {
                // _eta2 serves the role of fixed weights.
                calc_yhat_avg_null(
                    _old_weight, _new_weights, _indices, _eta1, _eta2, _yhat );
            }
        else
            {
                assert( false && "Aggregation not known!" );
            }
    }

    // -----------------------------------------------------------------

   private:
    /// Calculate the regularization reduction when one of the weights is nan.
    RELBOOST_FLOAT calc_regularization_reduction(
        const std::vector<RELBOOST_FLOAT>& _eta_old,
        const std::vector<RELBOOST_FLOAT>& _eta_new,
        const std::vector<size_t>& _indices,
        const RELBOOST_FLOAT _old_weight,
        const RELBOOST_FLOAT _new_weight ) const;

    /// Calculates two new weights given eta and indices when the aggregation at
    /// the lowest level is AVG or SUM and there are no NULL values.
    std::array<RELBOOST_FLOAT, 3> calc_weights(
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        multithreading::Communicator* _comm ) const;

    /// Calculates a new weight given eta and indices when the aggregation at
    /// the lowest level is AVG and the other weight is NULL.
    std::array<RELBOOST_FLOAT, 3> calc_weights_avg_null(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta,
        const std::vector<RELBOOST_FLOAT>& _w_fixed,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        multithreading::Communicator* _comm ) const;

    /// Calculates new yhat given the new_weights, eta and indices when
    /// the aggregation at the lowest level is AVG or SUM and there are no NULL
    /// values.
    void calc_yhat(
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<RELBOOST_FLOAT>& _yhat_committed,
        std::vector<RELBOOST_FLOAT>* _yhat ) const;

    /// Calculates a new yhat given the new_weights, eta and indices when the
    /// aggregation at the lowest level is AVG and the other weight is NULL.
    void calc_yhat_avg_null(
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta,
        const std::vector<RELBOOST_FLOAT>& _w_fixed,
        std::vector<RELBOOST_FLOAT>* _yhat ) const;

    // -----------------------------------------------------------------

   private:
    /// Trivial accessor
    const Hyperparameters& hyperparameters() const { return *hyperparameters_; }

    const RELBOOST_FLOAT sample_weights( size_t _i ) const
    {
        assert( sample_weights_ );
        assert( _i < sample_weights_->size() );
        return ( *sample_weights_ )[_i];
    }

    /// Trivial accessor
    const std::vector<RELBOOST_FLOAT>& targets() const { return *targets_; }

    // -----------------------------------------------------------------

   private:
    /// First derivative
    const std::vector<RELBOOST_FLOAT>& g_;

    /// Second derivative
    const std::vector<RELBOOST_FLOAT>& h_;

    /// Shared pointer to hyperparameters
    const std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The sample weights, which are used for the sampling procedure.
    const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& sample_weights_;

    /// Sum of g_, needed for the intercept.
    const RELBOOST_FLOAT& sum_g_;

    /// Sum of h_, needed for the intercept.
    const RELBOOST_FLOAT& sum_h_;

    /// Dot product of h_ and yhat_, needed for the intercept.
    const RELBOOST_FLOAT& sum_h_yhat_committed_;

    /// The target variables (previous trees already substracted).
    const std::shared_ptr<const std::vector<RELBOOST_FLOAT>> targets_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONIMPL_HPP_