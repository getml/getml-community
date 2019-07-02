#ifndef PREDICTORS_STANDARDSCALER_HPP_
#define PREDICTORS_STANDARDSCALER_HPP_

namespace predictors
{
// -----------------------------------------------------------------------------

/// For rescaling the input data to a standard deviation of one.
class StandardScaler
{
    // -------------------------------------------------------------------------

   public:
    StandardScaler(){};

    ~StandardScaler() = default;

    // -------------------------------------------------------------------------

    /// Calculates the standard deviations for dense data.
    void fit( const std::vector<CFloatColumn>& _X_numerical );

    /// Transforms dense data.
    std::vector<CFloatColumn> transform(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    // -------------------------------------------------------------------------

   private:
    /// The slopes of the linear regression.
    std::vector<Float> std_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_STANDARDSCALER_HPP_
