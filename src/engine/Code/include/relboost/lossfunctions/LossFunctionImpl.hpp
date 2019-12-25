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
        const std::vector<Float>& _g,
        const std::vector<Float>& _h,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<Float>>& _sample_weights,
        const Float& _sum_g,
        const Float& _sum_h,
        const Float& _sum_h_yhat,
        const std::shared_ptr<const std::vector<Float>>& _targets )
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
    Float calc_regularization_reduction(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights ) const;

    /// Calculates the sample index (which contains the indices of all samples
    /// with non-zero sample weight).
    std::vector<size_t> calc_sample_index(
        const std::shared_ptr<const std::vector<Float>>& _sample_weights )
        const;

    /// Calculates _sum_g and _sum_h.
    void calc_sums(
        const std::vector<size_t>& _sample_index,
        const std::vector<Float>& _sample_weights,
        Float* _sum_g,
        Float* _sum_h,
        Float* _sum_sample_weights,
        multithreading::Communicator* _comm ) const;

    /// Calculates the update rate.
    Float calc_update_rate(
        const std::vector<Float>& _yhat_old,
        const std::vector<Float>& _predictions,
        multithreading::Communicator* _comm ) const;

    /// Commits yhat.
    Float commit(
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _yhat,
        std::vector<Float>* _yhat_committed ) const;

    /// Reverts the last split - only called when this is a predictor.
    void revert( std::array<Float, 6>* _sufficient_stats ) const;

    /// Resets _yhat to _yhat_committed.
    void revert_to_commit(
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _yhat_committed,
        std::vector<Float>* _yhat ) const;

    /// Generates the predictions.
    void transform(
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end,
        const std::vector<Float>& _weights,
        std::vector<Float>* _predictions ) const;

    /// Updates _yhat_old by adding the predictions.
    void update_yhat_old(
        const Float _update_rate,
        const std::vector<Float>& _predictions,
        std::vector<Float>* _yhat_old ) const;

    // -----------------------------------------------------------------

   public:
    /// Calculates two new weights and the corresponding partial loss
    /// given eta and indices.
    std::pair<Float, std::array<Float, 3>> calc_pair(
        const enums::Aggregation _agg,
        const enums::Revert _revert,
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 13>* _sufficient_stats_non_null,
        std::array<Float, 8>* _sufficient_stats_first_null,
        std::array<Float, 8>* _sufficient_stats_second_null,
        multithreading::Communicator* _comm ) const
    {
        switch ( _agg )
            {
                case enums::Aggregation::avg:
                case enums::Aggregation::sum:
                    return calc_pair_non_null(
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_non_null,
                        _comm );

                case enums::Aggregation::avg_first_null:
                    return calc_pair_avg_null(
                        _agg,
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_first_null,
                        _comm );

                case enums::Aggregation::avg_second_null:
                    return calc_pair_avg_null(
                        _agg,
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_second_null,
                        _comm );

                default:
                    assert_true( false && "Unknown agg" );
                    return std::make_pair( 0.0, std::array<Float, 3>() );
            }
    }

    /// Calculates two new weights and the loss given matches. This just reduces
    /// to the normal XGBoost approach.
    std::pair<Float, std::array<Float, 3>> calc_pair(
        const enums::Revert _revert,
        const enums::Update _update,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _split_begin,
        const std::vector<containers::Match>::iterator _split_end,
        const std::vector<containers::Match>::iterator _end,
        Float* _loss_old,
        std::array<Float, 6>* _sufficient_stats,
        multithreading::Communicator* _comm ) const
    {
        switch ( _update )
            {
                case enums::Update::calc_all:
                    return calc_all(
                        _begin,
                        _split_begin,
                        _split_end,
                        _end,
                        _loss_old,
                        _sufficient_stats,
                        _comm );
                    break;

                case enums::Update::calc_diff:
                    return calc_diff(
                        _revert,
                        _begin,
                        _split_begin,
                        _split_end,
                        _end,
                        *_loss_old,
                        _sufficient_stats,
                        _comm );
                    break;

                default:
                    assert_true( false && "Unknown update!" );
                    return std::make_pair( 0.0, std::array<Float, 3>() );
            }
    }

    /// Calculates the sufficient statistics.
    /// This is called by the calc_etas of the loss functions.
    void calc_sufficient_stats(
        const enums::Aggregation _agg,
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 13>* _sufficient_stats_non_null,
        std::array<Float, 8>* _sufficient_stats_first_null,
        std::array<Float, 8>* _sufficient_stats_second_null ) const
    {
        switch ( _agg )
            {
                case enums::Aggregation::avg:
                case enums::Aggregation::sum:
                    return calc_sufficient_stats_non_null(
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_non_null );
                    break;

                case enums::Aggregation::avg_first_null:
                    calc_sufficient_stats_avg_null(
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_first_null );
                    break;

                case enums::Aggregation::avg_second_null:
                    calc_sufficient_stats_avg_null(
                        _update,
                        _old_weight,
                        _indices_current,
                        _eta1,
                        _eta1_old,
                        _eta2,
                        _eta2_old,
                        _yhat_committed,
                        _sufficient_stats_second_null );
                    break;

                default:
                    assert_true( false && "Unknown agg" );
            }
    }

    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _yhat_committed,
        std::vector<Float>* _yhat ) const
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
                assert_true( false && "Aggregation not known!" );
            }
    }

    // -----------------------------------------------------------------

   private:
    /// Calculate all matches, meaning that we are at the first split.
    std::pair<Float, std::array<Float, 3>> calc_all(
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _split_begin,
        const std::vector<containers::Match>::iterator _split_end,
        const std::vector<containers::Match>::iterator _end,
        Float* _loss_old,
        std::array<Float, 6>* _sufficient_stats,
        multithreading::Communicator* _comm ) const;

    /// Calculate only the difference to the last split.
    std::pair<Float, std::array<Float, 3>> calc_diff(
        const enums::Revert _revert,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _split_begin,
        const std::vector<containers::Match>::iterator _split_end,
        const std::vector<containers::Match>::iterator _end,
        const Float _loss_old,
        std::array<Float, 6>* _sufficient_stats,
        multithreading::Communicator* _comm ) const;

    /// Calculates a new weight given eta and indices when the aggregation at
    /// the lowest level is AVG and the other weight is NULL.
    std::pair<Float, std::array<Float, 3>> calc_pair_avg_null(
        const enums::Aggregation _agg,
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta,
        const std::vector<Float>& _eta_old,
        const std::vector<Float>& _w_fixed,
        const std::vector<Float>& _w_fixed_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 8>* _sufficient_stats,
        multithreading::Communicator* _comm ) const;

    /// Calculates two new weights given eta and indices when the aggregation at
    /// the lowest level is AVG or SUM and there are no NULL values.
    std::pair<Float, std::array<Float, 3>> calc_pair_non_null(
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 13>* _sufficient_stats,
        multithreading::Communicator* _comm ) const;

    /// Calculates the sufficient stats for the case
    /// when one of the new weights is NULL.
    void calc_sufficient_stats_avg_null(
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta,
        const std::vector<Float>& _eta_old,
        const std::vector<Float>& _w_fixed,
        const std::vector<Float>& _w_fixed_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 8>* _sufficient_stats ) const;

    /// Calculates the sufficient stats for the case
    /// when one of the new weights is NULL.
    void calc_sufficient_stats_non_null(
        const enums::Update _update,
        const Float _old_weight,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old,
        const std::vector<Float>& _yhat_committed,
        std::array<Float, 13>* _sufficient_stats ) const;

    /// Calculates new yhat given the new_weights, eta and indices when
    /// the aggregation at the lowest level is AVG or SUM and there are no
    /// NULL values.
    void calc_yhat(
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _yhat_committed,
        std::vector<Float>* _yhat ) const;

    /// Calculates a new yhat given the new_weights, eta and indices when the
    /// aggregation at the lowest level is AVG and the other weight is NULL.
    void calc_yhat_avg_null(
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta,
        const std::vector<Float>& _w_fixed,
        std::vector<Float>* _yhat ) const;

    // -----------------------------------------------------------------

   private:
    /// Applies the XGBoost formula to calculate the weight and loss.
    /// See Chen and Guestrin, 2016, XGBoost: A Scalable Tree Boosting System
    std::pair<Float, Float> apply_xgboost(
        const Float _sum_g, const Float _sum_h ) const
    {
        const auto weight = _sum_g / ( _sum_h + hyperparameters().reg_lambda_ );
        const auto loss = -0.5 * _sum_g * weight;
        return std::make_pair( loss, weight );
    }

    /// Trivial accessor
    const Hyperparameters& hyperparameters() const { return *hyperparameters_; }

    const Float sample_weights( size_t _i ) const
    {
        assert_true( sample_weights_ );
        assert_true( _i < sample_weights_->size() );
        return ( *sample_weights_ )[_i];
    }

    /// Trivial accessor
    const std::vector<Float>& targets() const { return *targets_; }

    // -----------------------------------------------------------------

   private:
    /// First derivative
    const std::vector<Float>& g_;

    /// Second derivative
    const std::vector<Float>& h_;

    /// Shared pointer to hyperparameters
    const std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The sample weights, which are used for the sampling procedure.
    const std::shared_ptr<const std::vector<Float>>& sample_weights_;

    /// Sum of g_, needed for the intercept.
    const Float& sum_g_;

    /// Sum of h_, needed for the intercept.
    const Float& sum_h_;

    /// Dot product of h_ and yhat_, needed for the intercept.
    const Float& sum_h_yhat_committed_;

    /// The target variables (previous trees already substracted).
    const std::shared_ptr<const std::vector<Float>> targets_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONIMPL_HPP_
