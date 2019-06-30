#ifndef PREDICTORS_PREDICTOR_HPP_
#define PREDICTORS_PREDICTOR_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Abstract base class for a predictor
class Predictor
{
    // -----------------------------------------

   public:
    Predictor(){};

    virtual ~Predictor() = default;

    // -----------------------------------------

    /// Returns an importance measure for the individual features
    virtual std::vector<Float> feature_importances(
        const size_t _num_features ) const = 0;

    /// Implements the fit(...) method in scikit-learn style
    virtual std::string fit(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y ) = 0;

    /// Loads the predictor
    virtual void load( const std::string& _fname ) = 0;

    /// Implements the predict(...) method in scikit-learn style
    virtual CFloatColumn predict(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const = 0;

    /// Stores the predictor
    virtual void save( const std::string& _fname ) const = 0;

    // -----------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // ENGINE_PREDICTORS_PREDICTOR_HPP_
