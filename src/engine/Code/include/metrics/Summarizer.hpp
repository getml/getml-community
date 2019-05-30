#ifndef METRICS_SUMMARIZER_HPP_
#define METRICS_SUMMARIZER_HPP_

namespace metrics
{
// -------------------------------------------------------------------------

class Summarizer
{
    // ---------------------------------------------------------------------

   public:
    /// Calculates the pearson r between features and
    /// a set of targets.
    static Poco::JSON::Object calculate_feature_correlations(
        const std::vector<METRICS_FLOAT>& _features,
        const size_t _nrows,
        const size_t _ncols,
        const std::vector<const METRICS_FLOAT*>& _targets );

    /// Calculates the distribution of a feature.
    static Poco::JSON::Object calculate_feature_densities(
        const std::vector<METRICS_FLOAT>& _features,
        const size_t _nrows,
        const size_t _ncols,
        const size_t _num_bins );

    // ---------------------------------------------------------------------

   private:
    /// Helper function for calculating labels, which
    /// are needed for column densities and average targets.
    static Poco::JSON::Array::Ptr calculate_labels(
        const std::vector<METRICS_FLOAT>& _minima,
        const std::vector<METRICS_FLOAT>& _step_sizes,
        const std::vector<size_t>& _actual_num_bins,
        const std::vector<std::vector<METRICS_INT>>& _feature_densities,
        const std::vector<METRICS_FLOAT>& _features,
        const size_t _nrows,
        const size_t _ncols );

    /// Helper function for calculating the step size and number of bins, which
    /// is needed for column densities and average targets.
    static void calculate_step_sizes_and_num_bins(
        const std::vector<METRICS_FLOAT>& _minima,
        const std::vector<METRICS_FLOAT>& _maxima,
        const METRICS_FLOAT _num_bins,
        std::vector<METRICS_FLOAT>* _step_sizes,
        std::vector<size_t>* _actual_num_bins );

    /// Helper function for identifying the correct bin, which
    /// finds the minimum and maximum.
    static void find_min_and_max(
        const std::vector<METRICS_FLOAT>& _features,
        const size_t _nrows,
        const size_t _ncols,
        std::vector<METRICS_FLOAT>* _minima,
        std::vector<METRICS_FLOAT>* _maxima );

    /// Helper function for identifying the correct bin, which
    /// is needed for column densities and average targets.
    static size_t identify_bin(
        const size_t _num_bins,
        const METRICS_FLOAT _step_size,
        const METRICS_FLOAT _val,
        const METRICS_FLOAT _min );

    // ---------------------------------------------------------------------

   private:
    /// Helper function
    template <typename T>
    static const T& get(
        const size_t _i,
        const size_t _j,
        const size_t _ncols,
        const std::vector<T>& _vec )
    {
        assert( _j < _ncols );
        assert( _i * _ncols + _j < _vec.size() );
        return _vec[_i * _ncols + _j];
    }

    /// Helper function
    template <typename T>
    static T& get(
        const size_t _i,
        const size_t _j,
        const size_t _ncols,
        std::vector<T>* _vec )
    {
        assert( _j < _ncols );
        assert( _i * _ncols + _j < _vec->size() );
        return ( *_vec )[_i * _ncols + _j];
    }

    // ---------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SUMMARIZER_HPP_
