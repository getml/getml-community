#ifndef RELBOOST_ENSEMBLE_THREADUTILS_HPP_
#define RELBOOST_ENSEMBLE_THREADUTILS_HPP_

namespace relboost
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
        ensemble::DecisionTreeEnsemble* _ensemble );

    /// Number of threads.
    static Int get_num_threads( const Int _num_threads );

    /// Generates features.
    static void transform_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const ensemble::DecisionTreeEnsemble& _ensemble,
        std::vector<Float>* features );

    // ------------------------------------------------------------------------

   private:
    /// Copies to the features.
    static void copy(
        const std::vector<size_t> _rows,
        const size_t _col,
        const size_t _num_features,
        const std::vector<Float>& _new_feature,
        std::vector<Float>* _features );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_THREADUTILS_HPP_
