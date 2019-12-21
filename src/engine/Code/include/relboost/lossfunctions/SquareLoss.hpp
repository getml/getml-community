#ifndef RELBOOST_LOSSFUNCTIONS_SQUARELOSS_HPP_
#define RELBOOST_LOSSFUNCTIONS_SQUARELOSS_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace lossfunctions
{
// ------------------------------------------------------------------------

class SquareLoss : public LossFunction
{
    // -----------------------------------------------------------------

   public:
    SquareLoss(
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<std::vector<Float>>& _targets )
        : comm_( nullptr ),
          hyperparameters_( _hyperparameters ),
          loss_committed_( 0.0 ),
          loss_old_( 0.0 ),
          sufficient_stats_(
              std::array<Float, 6>{0.0, 0.0, 0.0, 0.0, 0.0, 0.0} ),
          sum_h_yhat_committed_( 0.0 ),
          sum_sample_weights_( 0.0 ),
          targets_( _targets ),
          yhat_( std::vector<Float>( _targets->size() ) ),
          yhat_committed_( std::vector<Float>( _targets->size() ) ),
          impl_( LossFunctionImpl(
              g_,
              h_,
              hyperparameters_,
              sample_weights_,
              sum_g_,
              sum_h_,
              sum_h_yhat_committed_,
              targets_ ) )
    {
    }

    ~SquareLoss() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates first and second derivatives.
    void calc_gradients() final;

    /// Evaluates and entire tree.
    Float evaluate_tree(
        const Float _update_rate, const std::vector<Float>& _yhat_new ) final;

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

    /// Calculates the sampling rate (the share of samples that will be
    /// drawn for each feature).
    void calc_sampling_rate(
        const bool _set_rate,
        const Float _sampling_factor,
        multithreading::Communicator* _comm ) final
    {
        sampler_ = std::make_unique<utils::Sampler>( hyperparameters().seed_ );
        if ( _set_rate )
            {
                sampler().set_sampling_rate( _sampling_factor );
            }
        else
            {
                sampler().calc_sampling_rate(
                    targets().size(), _sampling_factor, _comm );
            }
    }

    /// Calculates sum_g_ and sum_h_.
    void calc_sums()
    {
        impl_.calc_sums(
            sample_index_,
            *sample_weights_,
            &sum_g_,
            &sum_h_,
            &sum_sample_weights_,
            &comm() );
    }

    /// Calculates the update rate.
    Float calc_update_rate( const std::vector<Float>& _predictions ) final
    {
        return impl_.calc_update_rate( yhat_old_, _predictions, &comm() );
    }

    /// Loss functions have no etas - nothing to do here.
    void calc_etas(
        const enums::Aggregation _agg,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) final
    {
    }

    /// Calculates two new weights given eta and indices.
    std::pair<Float, std::array<Float, 3>> calc_weights(
        const enums::Aggregation _agg,
        const Float _old_intercept,
        const Float _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) final
    {
        return impl_.calc_weights(
            _agg,
            _old_intercept,
            _old_weight,
            _indices,
            _eta1,
            _eta2,
            yhat_committed_,
            &comm() );
    }

    /// Calculates two new weights given matches. This just reduces to the
    /// normal XGBoost approach.
    std::vector<std::pair<Float, std::array<Float, 3>>> calc_pairs(
        const enums::Revert _revert,
        const enums::Update _update,
        const Float _min_num_samples,
        const Float _old_intercept,
        const Float _old_weight,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _split_begin,
        const std::vector<containers::Match>::iterator _split_end,
        const std::vector<containers::Match>::iterator _end ) final
    {
        const auto p = impl_.calc_pair(
            _revert,
            _update,
            _begin,
            _split_begin,
            _split_end,
            _end,
            &loss_old_,
            &sufficient_stats_,
            &comm() );

        if ( std::isnan( p.first ) )
            {
                return std::vector<std::pair<Float, std::array<Float, 3>>>( 0 );
            }
        else
            {
                return {p};
            }
    }

    /// Calculates the new yhat given eta, indices and the new weights.
    void calc_yhat(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::array<Float, 3>& _new_weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old ) final
    {
        impl_.calc_yhat(
            _agg,
            _old_weight,
            _new_weights,
            _indices,
            _eta1,
            _eta2,
            yhat_committed_,
            &yhat_ );
    }

    /// Returns a const shared pointer to the child (which doesn't exist, so
    /// it's empty.)
    std::shared_ptr<const lossfunctions::LossFunction> child() const final
    {
        return std::shared_ptr<const lossfunctions::LossFunction>();
    }

    /// Deletes all resources.
    void clear() final
    {
        resize( 0 );
        sample_index_.clear();
        sample_weights_.reset();
    }

    /// Commits yhat_old_.
    void commit() final
    {
        assert_true( yhat_old().size() == targets().size() );
        auto weights = std::array<Float, 3>( {0.0, 0.0, 0.0} );
        auto indices = std::vector<size_t>( 0 );
        commit( indices, weights );
    }

    /// Recalculates sum_h_yhat_committed_ and loss_committed_.
    void commit(
        const std::vector<size_t>& _indices,
        const std::array<Float, 3>& _weights ) final
    {
        loss_committed_ = calc_loss( _weights );
        sum_h_yhat_committed_ =
            impl_.commit( _indices, yhat_, &yhat_committed_ );
    }

    /// Keeps the current weights - this is directly called by
    /// DecisionTreeNode, meaning that this is used as a predictor.
    /// In this case, there is nothing to commit.
    void commit(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _split,
        const std::vector<containers::Match>::iterator _end ) final{};

    /// Actual loss functions always have depth 0.
    size_t depth() const final { return 0; }

    /// Returns the loss reduction achieved by a split.
    Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta2 )
    {
        return loss_committed_ - calc_loss( _weights ) +
               impl_.calc_regularization_reduction(
                   _eta1,
                   _eta2,
                   _indices,
                   _old_intercept,
                   _old_weight,
                   _weights,
                   sum_sample_weights_,
                   &comm() );
    }

    /// Initializes yhat_old_ by setting it to the initial prediction.
    void init_yhat_old( const Float _initial_prediction ) final
    {
        initial_prediction_ = _initial_prediction;
        yhat_old_ = std::vector<Float>( targets().size(), _initial_prediction );
    }

    /// Generates the sample weights.
    const std::shared_ptr<const std::vector<Float>> make_sample_weights() final
    {
        sample_weights_ = sampler().make_sample_weights( targets().size() );
        sample_index_ = impl_.calc_sample_index( sample_weights_ );
        return sample_weights_;
    }

    /// Reduces the predictions - since this is a loss function, there is
    /// nothing to reduce, but we do have to intercept.
    void reduce_predictions(
        const Float _intercept, std::vector<Float>* _predictions ) final
    {
        for ( auto& val : *_predictions ) val += _intercept;
    }

    /// Resets critical resources to zero.
    void reset() final
    {
        sum_h_yhat_committed_ = 0.0;
        std::fill( yhat_.begin(), yhat_.end(), 0.0 );
        std::fill( yhat_committed_.begin(), yhat_committed_.end(), 0.0 );
    }

    /// Resets yhat_old to the initial prediction.
    void reset_yhat_old() final { init_yhat_old( initial_prediction_ ); }

    /// Resizes critical resources.
    void resize( size_t _size ) final
    {
        g_.resize( _size );
        h_.resize( _size );

        yhat_.resize( _size );
        yhat_committed_.resize( _size );

        reset();
    }

    /// Reverts the effects of calc_diff (or the part in calc_all the
    /// corresponds to calc_diff). This is needed for supporting categorical
    /// columns.
    void revert( const Float _old_weight ) final{};

    /// Keeps the current weights.
    void revert_to_commit() final{};

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit( const std::vector<size_t>& _indices ) final
    {
        impl_.revert_to_commit( _indices, yhat_committed_, &yhat_ );
    };

    /// Trivial setter.
    void set_comm( multithreading::Communicator* _comm ) final
    {
        comm_ = _comm;
    }

    /// Generates the predictions.
    Float transform( const std::vector<Float>& _weights ) const final
    {
        assert_true( _weights.size() == 1 );
        return _weights[0];
    }

    /// Describes the type of the loss function (SquareLoss, CrossEntropyLoss,
    /// etc.)
    std::string type() const final { return "SquareLoss"; }

    /// Updates yhat_old_ by adding the predictions.
    void update_yhat_old(
        const Float _update_rate, const std::vector<Float>& _predictions ) final
    {
        impl_.update_yhat_old( _update_rate, _predictions, &yhat_old_ );
    }

    // -----------------------------------------------------------------

   private:
    /// Calculates the loss given a set of predictions.
    Float calc_loss( const std::array<Float, 3>& _weights );

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    multithreading::Communicator& comm() const
    {
        assert_true( comm_ != nullptr );
        return *comm_;
    }

    /// Trivial accessor
    const Hyperparameters& hyperparameters() const
    {
        assert_true( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Trivial accessor
    utils::Sampler& sampler()
    {
        assert_true( sampler_ );
        return *sampler_;
    }

    /// Trivial accessor
    const std::vector<Float>& targets() const
    {
        assert_true( targets_ );
        return *targets_;
    }

    /// Trivial accessor
    const std::vector<Float>& yhat_old() const { return yhat_old_; }

    // -----------------------------------------------------------------

   private:
    /// Communicator
    multithreading::Communicator* comm_;

    /// First derivative
    std::vector<Float> g_;

    /// Second derivative
    std::vector<Float> h_;

    /// Shared pointer to hyperparameters
    const std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The initial prediction (average of the target values).
    Float initial_prediction_;

    /// The committed loss, needed for calculating the loss reduction.
    Float loss_committed_;

    /// The loss calculated using the old weight (needed for calc_pairs)
    Float loss_old_;

    /// Indices of all non-zero sample weights.
    std::vector<size_t> sample_index_;

    /// The weights used for the samples.
    std::shared_ptr<const std::vector<Float>> sample_weights_;

    /// The sampler used to determine the sample weights.
    std::unique_ptr<utils::Sampler> sampler_;

    /// The sufficient statistics needed to apply the xgboost formula
    /// (needed for calc_pairs).
    std::array<Float, 6> sufficient_stats_;

    /// Sum of g_, needed for the intercept.
    Float sum_g_;

    /// Sum of h_, needed for the intercept.
    Float sum_h_;

    /// Dot product of h_ and yhat_, needed for the intercept.
    Float sum_h_yhat_committed_;

    /// The sum of the sample weights, which is needed for calculating the loss.
    Float sum_sample_weights_;

    /// The target variables.
    const std::shared_ptr<const std::vector<Float>> targets_;

    /// The output.
    std::vector<Float> yhat_;

    /// The output that has been committed.
    std::vector<Float> yhat_committed_;

    /// Sum of all previous trees.
    std::vector<Float> yhat_old_;

    /// Implementation class. Because impl_ depends on some other variables, it
    /// is the last member variable.
    const LossFunctionImpl impl_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_SQUARELOSS_HPP_
