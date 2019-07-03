#ifndef AUTOSQL_ENSEMBLE_THREADUTILS_HPP_
#define AUTOSQL_ENSEMBLE_THREADUTILS_HPP_

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class Threadutils
{
    // ------------------------------------------------------------------------

   public:
    /// Fits a feature engineerer  or throws an exception.
    static void fit_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const std::shared_ptr<const descriptors::Hyperparameters>&
            _hyperparameters,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const decisiontrees::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm,
        ensemble::DecisionTreeEnsemble* _ensemble );

    /// Number of threads.
    static size_t get_num_threads( const size_t _num_threads );

    /// Generates features.
    static void transform_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const DecisionTreeEnsemble& _ensemble,
        containers::Features* _features );

    // ------------------------------------------------------------------------

   private:
    /// Copies to the features.
    static void copy(
        const std::vector<size_t> _rows,
        const std::vector<Float>& _local_feature,
        std::vector<Float>* _global_feature );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_ENSEMBLE_THREADUTILS_HPP_
