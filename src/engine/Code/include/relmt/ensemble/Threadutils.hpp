#ifndef RELMT_ENSEMBLE_THREADUTILS_HPP_
#define RELMT_ENSEMBLE_THREADUTILS_HPP_

namespace relmt
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class Threadutils
{
    // ------------------------------------------------------------------------

   public:
    /// Fits an ensemble.
    static void fit_ensemble( const ThreadutilsFitParams _params );

    /// Number of threads.
    static Int get_num_threads( const Int _num_threads );

    /// Generates features or predictions.
    static void transform_ensemble( const ThreadutilsTransformParams _params );

    // ------------------------------------------------------------------------

   private:
    /// Copies to the features.
    static void copy(
        const std::vector<size_t> _rows,
        const std::vector<Float>& _local_feature,
        std::vector<Float>* _global_feature );

    /// Fits the relboost ensemble as a feature learner.
    static void fit_as_feature_learner( const ThreadutilsFitParams& _params );

    /// Generates features.
    static void transform_as_feature_learner(
        const ThreadutilsTransformParams& _params );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_ENSEMBLE_THREADUTILS_HPP_
