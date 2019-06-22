#ifndef ENGINE_PREDICTORS_PREDICTOR_HPP_
#define ENGINE_PREDICTORS_PREDICTOR_HPP_

namespace engine
{
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
        const std::shared_ptr<const monitoring::Logger> _logger,
        const containers::Matrix<Float>& _X,
        const containers::Matrix<Float>& _y ) = 0;

    /// Loads the predictor
    virtual void load( const std::string& _fname ) = 0;

    /// Implements the predict(...) method in scikit-learn style
    virtual containers::Matrix<Float> predict(
        const containers::Matrix<Float>& _X ) const = 0;

    /// Stores the predictor
    virtual void save( const std::string& _fname ) const = 0;

    // -----------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace predictors
}  // namespace engine

#endif  // ENGINE_PREDICTORS_PREDICTOR_HPP_
