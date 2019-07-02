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

    /// Calculates the standard deviations for sparse data.
    void fit( const CSRMatrix<Float, unsigned int, size_t>& _X_sparse );

    /// Transforms dense data.
    std::vector<CFloatColumn> transform(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Transforms sparse data.
    const CSRMatrix<Float, unsigned int, size_t> transform(
        const CSRMatrix<Float, unsigned int, size_t>& _X_sparse ) const;

    // -------------------------------------------------------------------------

   private:
    /// The slopes of the linear regression.
    std::vector<Float> std_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_STANDARDSCALER_HPP_
