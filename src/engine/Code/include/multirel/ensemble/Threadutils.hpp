#ifndef MULTIREL_ENSEMBLE_THREADUTILS_HPP_
#define MULTIREL_ENSEMBLE_THREADUTILS_HPP_

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class Threadutils
{
    // ------------------------------------------------------------------------

   public:
    /// Fits a feature learner  or throws an exception.
    static void fit_ensemble( const ThreadutilsFitParams& _params );

    /// Number of threads.
    static size_t get_num_threads( const size_t _num_threads );

    /// Generates features.
    static void transform_ensemble( const ThreadutilsTransformParams& _params );

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
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_ENSEMBLE_THREADUTILS_HPP_
