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

    StandardScaler( const Poco::JSON::Object _obj )
        : mean_( JSON::array_to_vector<Float>(
              JSON::get_array( _obj, "mean_" ) ) ),
          std_( JSON::array_to_vector<Float>(
              JSON::get_array( _obj, "std_" ) ) ){};

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

    // Transforms StandardScaler to JSON object.
    Poco::JSON::Object to_json_obj() const
    {
        Poco::JSON::Object obj;
        obj.set( "mean_", JSON::vector_to_array( std_ ) );
        obj.set( "std_", JSON::vector_to_array( std_ ) );
        return obj;
    }

    // -------------------------------------------------------------------------

   private:
    /// Means of the individual columns.
    std::vector<Float> mean_;

    /// Standard deviations of the individual columns.
    std::vector<Float> std_;

    // -------------------------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_STANDARDSCALER_HPP_
