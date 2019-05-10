#ifndef RELBOOST_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_
#define RELBOOST_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace lossfunctions
{
// ------------------------------------------------------------------------

class CrossEntropyLoss : public LossFunction
{
    // -----------------------------------------------------------------

   public:
    CrossEntropyLoss(
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<std::vector<RELBOOST_FLOAT>>& _targets )
        : comm_( nullptr ),
          hyperparameters_( _hyperparameters ),
          loss_committed_( 0.0 ),
          sum_h_yhat_committed_( 0.0 ),
          sum_sample_weights_( 0.0 ),
          targets_( _targets ),
          yhat_( std::vector<RELBOOST_FLOAT>( _targets->size() ) ),
          yhat_committed_( std::vector<RELBOOST_FLOAT>( _targets->size() ) ),
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
        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.0001 ) ) -
                0.0001 ) < 1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.001 ) ) -
                0.001 ) < 1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.01 ) ) -
                0.01 ) < 1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.1 ) ) - 0.1 ) <
            1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.5 ) ) - 0.5 ) <
            1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.9 ) ) - 0.9 ) <
            1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.99 ) ) -
                0.99 ) < 1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.999 ) ) -
                0.999 ) < 1e-7 );

        assert(
            std::abs(
                logistic_function( inverse_logistic_function( 0.9999 ) ) -
                0.9999 ) < 1e-7 );
    }

    ~CrossEntropyLoss() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates first and second derivatives.
    void calc_gradients(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& _yhat_old )
        final;

    /// Evaluates split given matches. In this case, the loss function effective
    /// turns into XGBoost.
    RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) final;

    /// Evaluates and entire tree.
    RELBOOST_FLOAT evaluate_tree(
        const std::vector<RELBOOST_FLOAT>& _yhat_new ) final;

    // -----------------------------------------------------------------

   public:
    // Applies the inverse of the transformation function below. Some loss
    // functions (such as CrossEntropyLoss) require this. For others, this won't
    // do anything at all.
    void apply_inverse( RELBOOST_FLOAT* yhat_ ) const final
    {
        *yhat_ = inverse_logistic_function( *yhat_ );
    }

    // Applies a transformation function. Some loss functions (such as
    // CrossEntropyLoss) require this. For others, this won't do anything at
    // all.
    void apply_transformation( std::vector<RELBOOST_FLOAT>* yhat_ ) const final
    {
        std::for_each(
            yhat_->begin(), yhat_->end(), [this]( RELBOOST_FLOAT& _val ) {
                _val = logistic_function( _val );
            } );
    }

    /// Calculates an index that contains all non-zero samples.
    void calc_sample_index(
        const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>&
            _sample_weights )
    {
        sample_weights_ = _sample_weights;
        sample_index_ = impl_.calc_sample_index( _sample_weights );
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
    RELBOOST_FLOAT calc_update_rate(
        const std::vector<RELBOOST_FLOAT>& _yhat_old,
        const std::vector<RELBOOST_FLOAT>& _predictions ) final
    {
        return impl_.calc_update_rate( _yhat_old, _predictions, &comm() );
    }

    /// Calculates two new weights given matches. This just reduces to the
    /// normal XGBoost approach.
    std::vector<std::array<RELBOOST_FLOAT, 3>> calc_weights(
        const enums::Revert _revert,
        const enums::Update _update,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split_begin,
        const std::vector<const containers::Match*>::iterator _split_end,
        const std::vector<const containers::Match*>::iterator _end ) final
    {
        return impl_.calc_weights(
            _update,
            _old_weight,
            _begin,
            _split_begin,
            _split_end,
            _end,
            &comm() );
    }

    /// Calculates two new weights given eta and indices.
    std::array<RELBOOST_FLOAT, 3> calc_weights(
        const enums::Aggregation _agg,
        const RELBOOST_FLOAT _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 ) final
    {
        return impl_.calc_weights(
            _agg,
            _old_weight,
            _indices,
            _eta1,
            _eta2,
            yhat_committed_,
            &comm() );
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
    };

    /// Deletes all resources.
    void clear() final
    {
        resize( 0 );
        sample_index_.clear();
    }

    /// Commits _yhat_old.
    void commit() final
    {
        assert( yhat_old().size() == targets().size() );
        auto zeros = std::vector<RELBOOST_FLOAT>( targets().size() );
        auto weights = std::array<RELBOOST_FLOAT, 3>( {0.0, 0.0, 0.0} );
        auto indices = std::vector<size_t>( 0 );
        commit( zeros, zeros, indices, weights );
    }

    /// Recalculates sum_h_yhat_committed_ and loss_committed_.
    void commit(
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2,
        const std::vector<size_t>& _indices,
        const std::array<RELBOOST_FLOAT, 3>& _weights ) final
    {
        loss_committed_ = calc_loss( _weights );
        sum_h_yhat_committed_ =
            impl_.commit( _indices, yhat_, &yhat_committed_ );
    }

    /// Keeps the current weights.
    void commit(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _split,
        const std::vector<const containers::Match*>::iterator _end ) final{};

    /// Actual loss functions always have depth 0.
    size_t depth() const final { return 0; }

    /// Returns the loss reduction achieved by a split.
    RELBOOST_FLOAT evaluate_split(
        const RELBOOST_FLOAT _old_intercept,
        const RELBOOST_FLOAT _old_weight,
        const std::array<RELBOOST_FLOAT, 3>& _weights,
        const std::vector<size_t>& _indices,
        const std::vector<RELBOOST_FLOAT>& _eta1,
        const std::vector<RELBOOST_FLOAT>& _eta2 )
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

    /// Resets critical resources to zero.
    void reset() final
    {
        sum_h_yhat_committed_ = 0.0;
        std::fill( yhat_.begin(), yhat_.end(), 0.0 );
        std::fill( yhat_committed_.begin(), yhat_committed_.end(), 0.0 );
    }

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
    void revert( const RELBOOST_FLOAT _old_weight ) final{};

    /// Keeps the current weights.
    void revert_to_commit() final
    {
        assert( false );
        // ToDO
    };

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
    RELBOOST_FLOAT transform(
        const std::vector<RELBOOST_FLOAT>& _weights ) const final
    {
        assert( false && "ToDO" );
        return 0.0;
    }

    /// Describes the type of the loss function (SquareLoss, CrossEntropyLoss,
    /// etc.)
    std::string type() const final { return "CrossEntropyLoss"; }

    // -----------------------------------------------------------------

   private:
    /// Calculates the loss given a set of predictions.
    RELBOOST_FLOAT calc_loss( const std::array<RELBOOST_FLOAT, 3>& _weights );

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    multithreading::Communicator& comm() const
    {
        assert( comm_ != nullptr );
        return *comm_;
    }

    /// Trivial accessor
    const Hyperparameters& hyperparameters() const
    {
        assert( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Applies the inverse logistic function.
    RELBOOST_FLOAT inverse_logistic_function( const RELBOOST_FLOAT _val ) const
    {
        const RELBOOST_FLOAT result = std::log( _val ) - std::log( 1.0 - _val );

        if ( std::isnan( result ) || std::isinf( result ) )
            {
                if ( _val > 0.5 )
                    {
                        return 1e10;
                    }
                else
                    {
                        return -1e10;
                    }
            }

        return result;
    }

    /// Applies the logistic function.
    RELBOOST_FLOAT logistic_function( const RELBOOST_FLOAT _val ) const
    {
        const RELBOOST_FLOAT result = 1.0 / ( 1.0 + std::exp( -_val ) );

        if ( std::isnan( result ) || std::isinf( result ) )
            {
                if ( _val > 0.0 )
                    {
                        return 1.0;
                    }
                else
                    {
                        return 0.0;
                    }
            }

        return result;
    }

    /// Applies the log loss function.
    RELBOOST_FLOAT log_loss(
        const RELBOOST_FLOAT _sigma_yhat, const RELBOOST_FLOAT _target ) const
    {
        RELBOOST_FLOAT part1 = -_target * std::log( _sigma_yhat );

        if ( std::isnan( part1 ) || std::isinf( part1 ) )
            {
                part1 = _target * 1e10;
            }

        RELBOOST_FLOAT part2 =
            -( 1.0 - _target ) * std::log( 1.0 - _sigma_yhat );

        if ( std::isnan( part2 ) || std::isinf( part2 ) )
            {
                part2 = -_target * 1e10;
            }

        return part1 + part2;
    }

    /// Trivial accessor
    const std::vector<RELBOOST_FLOAT>& targets() const
    {
        assert( targets_ );
        return *targets_;
    }

    /// Trivial accessor
    const std::vector<RELBOOST_FLOAT>& yhat_old() const
    {
        assert( yhat_old_ );
        return *yhat_old_;
    }

    // -----------------------------------------------------------------

   private:
    /// Communicator
    multithreading::Communicator* comm_;

    /// First derivative
    std::vector<RELBOOST_FLOAT> g_;

    /// Second derivative
    std::vector<RELBOOST_FLOAT> h_;

    /// Shared pointer to hyperparameters
    const std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The committed loss, needed for calculating the loss reduction.
    RELBOOST_FLOAT loss_committed_;

    /// Indices of all non-zero sample weights.
    std::vector<size_t> sample_index_;

    /// The weights used for the samples.
    std::shared_ptr<const std::vector<RELBOOST_FLOAT>> sample_weights_;

    /// Sum of g_, needed for the intercept.
    RELBOOST_FLOAT sum_g_;

    /// Sum of h_, needed for the intercept.
    RELBOOST_FLOAT sum_h_;

    /// Dot product of h_ and yhat_, needed for the intercept.
    RELBOOST_FLOAT sum_h_yhat_committed_;

    /// The sum of the sample weights, which is needed for calculating the loss.
    RELBOOST_FLOAT sum_sample_weights_;

    /// The target variables.
    const std::shared_ptr<const std::vector<RELBOOST_FLOAT>> targets_;

    /// The output.
    std::vector<RELBOOST_FLOAT> yhat_;

    /// The output that has been committed.
    std::vector<RELBOOST_FLOAT> yhat_committed_;

    /// Sum of all previous trees.
    std::shared_ptr<const std::vector<RELBOOST_FLOAT>> yhat_old_;

    /// Implementation class. Because impl_ depends on some other variables, it
    /// is the last member variable.
    const LossFunctionImpl impl_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_