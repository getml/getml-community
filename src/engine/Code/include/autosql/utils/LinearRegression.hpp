#ifndef AUTOSQL_UTILS_LINEARREGRESSION_HPP_
#define AUTOSQL_UTILS_LINEARREGRESSION_HPP_

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

class LinearRegression
{
   public:
    LinearRegression();

    LinearRegression( size_t _ncols );

    ~LinearRegression() = default;

    // -----------------------------------------

    // Fits a simple linear regression on each column
    // of _residuals w.r.t. _yhat, which has only one column
    void fit(
        const std::vector<AUTOSQL_FLOAT>& _yhat,
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _residuals,
        const std::vector<AUTOSQL_FLOAT>& _sample_weights );

    // Generates predictions based on _yhat
    std::vector<std::vector<AUTOSQL_FLOAT>> predict(
        const std::vector<AUTOSQL_FLOAT>& _yhat ) const;

    // -----------------------------------------

    multithreading::Communicator& comm() const { return *( comm_ ); }

    std::vector<AUTOSQL_FLOAT>& intercepts() { return intercepts_; }

    void set_comm( multithreading::Communicator* _comm ) { comm_ = _comm; }

    size_t size() const
    {
        assert( intercepts_.size() == slopes_.size() );
        return intercepts_.size();
    }

    std::vector<AUTOSQL_FLOAT>& slopes() { return slopes_; }

    // -----------------------------------------

   private:
    // Communicator object
    multithreading::Communicator* comm_;

    // Zero intercepts or biases of the linear regression
    std::vector<AUTOSQL_FLOAT> intercepts_;

    // Slope parameters of the linear regression
    std::vector<AUTOSQL_FLOAT> slopes_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace utils
}  // namespace autosql

#endif  // AUTOSQL_UTILS_LINEARREGRESSION_HPP_
