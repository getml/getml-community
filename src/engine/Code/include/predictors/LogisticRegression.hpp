#ifndef PREDICTORS_LOGISTICREGRESSION_HPP_
#define PREDICTORS_LOGISTICREGRESSION_HPP_

namespace predictors
{
// -----------------------------------------------------------------------------

/// LogisticRegression predictor.
class LogisticRegression : public Predictor
{
    // -------------------------------------------------------------------------

   public:
    LogisticRegression(
        const std::shared_ptr<LinearHyperparams>& _hyperparams,
        const std::shared_ptr<const PredictorImpl>& _impl )
        : hyperparams_( _hyperparams ), impl_( _impl ){};

    ~LogisticRegression() = default;

    // -------------------------------------------------------------------------

   public:
    /// Returns an importance measure for the individual features.
    std::vector<Float> feature_importances(
        const size_t _num_features ) const final;

    /// Implements the fit(...) method in scikit-learn style
    std::string fit(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y ) final;

    /// Loads the predictor
    void load( const std::string& _fname ) final;

    /// Implements the predict(...) method in scikit-learn style
    CFloatColumn predict(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const final;

    /// Stores the predictor
    void save( const std::string& _fname ) const final;

    // -------------------------------------------------------------------------

   public:
    /// Whether the predictor accepts null values.
    bool accepts_null() const final { return false; }

    // -------------------------------------------------------------------------

   private:
    /// Trivial (private const) accessor.
    const LinearHyperparams& hyperparams() const
    {
        assert_true( hyperparams_ );
        return *hyperparams_;
    }

    // -------------------------------------------------------------------------

   private:
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    /// Fit on dense data.
    void fit_dense(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y );

    /// Fit on sparse data.
    void fit_sparse(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y );

    /// Generates predictions when no categorical columns have been passed.
    CFloatColumn predict_dense(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Generates predictions when at least one categorical column has been
    /// passed.
    CFloatColumn predict_sparse(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    // -------------------------------------------------------------------------

   private:
    /// Calculates the gradients needed for the updates (sparse).
    const void calculate_gradients(
        const std::vector<CFloatColumn>& _X,
        const size_t _i,
        const Float _delta,
        std::vector<Float>* _gradients )
    {
        assert_true( _gradients->size() == weights_.size() );
        assert_true( _gradients->size() == _X.size() + 1 );

        for ( size_t j = 0; j < _X.size(); ++j )
            {
                ( *_gradients )[j] += _delta * ( *_X[j] )[_i];
            }

        _gradients->back() += _delta;
    }

    /// Calculates the gradients needed for the updates (sparse).
    const void calculate_gradients(
        const size_t _begin,
        const size_t _end,
        const unsigned int* _indices,
        const Float* _data,
        const Float _delta,
        std::vector<Float>* _gradients )
    {
        assert_true( _gradients->size() == weights_.size() );

        for ( auto ix = _begin; ix < _end; ++ix )
            {
                assert_true( _indices[ix] < _gradients->size() );
                ( *_gradients )[_indices[ix]] += _delta * _data[ix];
            }

        _gradients->back() += _delta;
    }

    /// Applies the L2 regularization term for numerical optimization.
    const void calculate_regularization(
        const Float _bsize_float, std::vector<Float>* _gradients )
    {
        if ( hyperparams().lambda_ > 0.0 )
            {
                for ( size_t i = 0; i < weights_.size(); ++i )
                    {
                        ( *_gradients )[i] +=
                            hyperparams().lambda_ * weights_[i] * _bsize_float;
                    }
            }
    }

    /// Logistic function.
    Float logistic_function( const Float _x ) const
    {
        return 1.0 / ( 1.0 + std::exp( -1.0 * _x ) );
    }

    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert_true( impl_ );
        return *impl_;
    }

    /// Returns a dense prediction.
    const Float predict_dense(
        const std::vector<CFloatColumn>& _X, const size_t _i ) const
    {
        Float yhat = weights_.back();
        for ( size_t j = 0; j < _X.size(); ++j )
            {
                yhat += ( *_X[j] )[_i] * weights_[j];
            }
        yhat = logistic_function( yhat );

        return yhat;
    }

    /// Returns a sparse prediction.
    const Float predict_sparse(
        const size_t _begin,
        const size_t _end,
        const unsigned int* _indices,
        const Float* _data ) const
    {
        Float yhat = weights_.back();
        for ( auto ix = _begin; ix < _end; ++ix )
            {
                assert_true( _indices[ix] < weights_.size() );
                yhat += _data[ix] * weights_[_indices[ix]];
            }
        yhat = logistic_function( yhat );
        return yhat;
    }

    // -------------------------------------------------------------------------

   private:
    /// The hyperparameters used for the LinearRegression.
    std::shared_ptr<const LinearHyperparams> hyperparams_;

    /// Implementation class for member functions common to most predictors.
    std::shared_ptr<const PredictorImpl> impl_;

    /// For rescaling the input data such that the standard deviation of each
    /// column is 1.0
    StandardScaler scaler_;

    /// The slopes of the linear regression.
    std::vector<Float> weights_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LOGISTICREGRESSION_HPP_
