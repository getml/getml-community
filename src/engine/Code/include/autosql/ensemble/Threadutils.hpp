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
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        DecisionTreeEnsemble* _ensemble );

    /// Number of threads.
    static AUTOSQL_INT get_num_threads( const AUTOSQL_INT _num_threads );

    /// Generates features.
    static void transform_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const DecisionTreeEnsemble& _ensemble,
        std::vector<AUTOSQL_FLOAT>* features );

    // ------------------------------------------------------------------------

   private:
    /// Copies to the features.
    static void copy(
        const std::vector<size_t> _rows,
        const size_t _col,
        const size_t _num_features,
        const std::vector<AUTOSQL_FLOAT>& _new_feature,
        std::vector<AUTOSQL_FLOAT>* _features );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_ENSEMBLE_THREADUTILS_HPP_
