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
