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
    LinearRegression( const std::shared_ptr<const PredictorImpl>& _impl )
        : impl_( _impl ){};

    ~LinearRegression() = default;

    // -------------------------------------------------------------------------

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

    /// Returns an importance measure for the individual features
    std::vector<Float> feature_importances(
        const size_t _num_features ) const final
    {
        return feature_importances_;
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
        const std::vector<CFloatColumn>& _X, const CFloatColumn& _y );

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
        assert( _gradients->size() == weights_.size() );

        for ( auto ix = _begin; ix < _end; ++ix )
            {
                assert( _indices[ix] < _gradients->size() );
                ( *_gradients )[_indices[ix]] += _delta * _data[ix];
            }
        _gradients->back() += _delta;
    }

    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert( impl_ );
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
                assert( _indices[ix] < weights_.size() );
                yhat += _data[ix] * weights_[_indices[ix]];
            }
        return yhat;
    }

    // -------------------------------------------------------------------------

   private:
    /// The feature importances for the linear regression.
    std::vector<Float> feature_importances_;

    /// Implementation class for member functions common to most predictors.
    std::shared_ptr<const PredictorImpl> impl_;

    /// The slopes of the linear regression.
    std::vector<Float> weights_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
