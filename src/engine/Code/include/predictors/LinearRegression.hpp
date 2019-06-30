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
    LinearRegression(){};

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

    /// When possible, the linear regression will be fitted
    /// solved arithmetically.
    void solve_arithmetically(
        const std::vector<CFloatColumn>& _X, const CFloatColumn& _y );

    // -------------------------------------------------------------------------

   private:
    /// The feature importances for the linear regression.
    std::vector<Float> feature_importances_;

    /// The slopes of the linear regression.
    std::vector<Float> weights_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARREGRESSION_HPP_
