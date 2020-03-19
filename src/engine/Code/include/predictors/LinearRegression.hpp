#ifndef PREDICTORS_LINEARREGRESSION_HPP_
#define PREDICTORS_LINEARREGRESSION_HPP_

namespace predictors
{
// -----------------------------------------------------------------------------

/// Linear regression predictor.
class LinearRegression : public Predictor
{
    // -------------------------------------------------------------------------

   public:
    LinearRegression(
        const std::shared_ptr<LinearHyperparams>& _hyperparams,
        const std::shared_ptr<const PredictorImpl>& _impl )
        : hyperparams_( _hyperparams ), impl_( _impl ){};

    ~LinearRegression() = default;

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

    /// Whether the predictor is used for classification;
    bool is_classification() const final { return false; }

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

    /// Generates predictions when no categorical columns have been passed.
    CFloatColumn predict_dense(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Generates predictions when at least one categorical column has been
    /// passed.
    CFloatColumn predict_sparse(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// When possible, the linear regression will be fitted
    ///  arithmetically.
    void solve_arithmetically(
        const std::vector<CFloatColumn>& _X_numerical, const CFloatColumn& _y );

    /// When necessary, we will use numerical algorithms.
    void solve_numerically(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y );

    // -------------------------------------------------------------------------

   private:
    /// Calculates the gradients needed for the updates.
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
        if ( hyperparams().reg_lambda_ > 0.0 )
            {
                for ( size_t i = 0; i < weights_.size(); ++i )
                    {
                        ( *_gradients )[i] += hyperparams().reg_lambda_ *
                                              weights_[i] * _bsize_float;
                    }
            }
    }

    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert_true( impl_ );
        return *impl_;
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

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
