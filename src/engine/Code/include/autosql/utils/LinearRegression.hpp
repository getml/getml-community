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

    inline multithreading::Communicator& comm() { return *( comm_ ); }

    const std::vector<AUTOSQL_FLOAT>& intercepts() { return intercepts_; }

    inline AUTOSQL_FLOAT& intercepts( AUTOSQL_INT _i )
    {
        assert( _i < intercepts_.size() );
        return intercepts_[_i];
    }

    inline void set_comm( multithreading::Communicator* _comm )
    {
        comm_ = _comm;
    }

    inline void set_slopes_and_intercepts(
        const std::vector<AUTOSQL_FLOAT>& _update_rates1,
        const std::vector<AUTOSQL_FLOAT>& _update_rates2 )
    {
        assert( _update_rates1.size() == _update_rates2.size() );

        slopes_ = _update_rates1;
        intercepts_ = _update_rates2;
    }

    inline size_t size()
    {
        assert( intercepts_.size() == slopes_.size() );
        return intercepts_.size();
    }

    const std::vector<AUTOSQL_FLOAT>& slopes() const { return slopes_; }

    inline AUTOSQL_FLOAT& slopes( size_t _i )
    {
        assert( _i < slopes_.size() );
        return slopes_[_i];
    }

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
