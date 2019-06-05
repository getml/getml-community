#ifndef AUTOSQL_CONTAINERS_SUMMARIZER_HPP_
#define AUTOSQL_CONTAINERS_SUMMARIZER_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

class Summarizer
{
   public:
    /// Calculates the density plots of each column.
    static std::vector<std::vector<AUTOSQL_INT>> calculate_column_densities(
        const AUTOSQL_INT _num_bins,
        const containers::Matrix<AUTOSQL_FLOAT>& _mat,
        AUTOSQL_COMMUNICATOR* _comm );

    /// Calculates the pearson r between features and
    /// a set of targets.
    static std::vector<std::vector<AUTOSQL_FLOAT>>
    calculate_feature_correlations(
        const containers::Matrix<AUTOSQL_FLOAT>& _features,
        const containers::DataFrameView& _targets,
        AUTOSQL_COMMUNICATOR* _comm );

    /// Calculates the data needed for the plots in the feature view.
    static void calculate_feature_plots(
        const AUTOSQL_INT _num_bins,
        const containers::Matrix<AUTOSQL_FLOAT>& _mat,
        const containers::DataFrameView& _targets,
        AUTOSQL_COMMUNICATOR* _comm,
        std::vector<std::vector<AUTOSQL_FLOAT>>& _labels,
        std::vector<std::vector<AUTOSQL_INT>>& _feature_densities,
        std::vector<std::vector<std::vector<AUTOSQL_FLOAT>>>& _average_targets );

#ifdef AUTOSQL_PARALLEL

    /// Identifies the global minimum and the global maximum
    template <typename T>
    static void reduce_min_max( AUTOSQL_COMMUNICATOR& _comm, T& _min, T& _max );

#endif  // AUTOSQL_PARALLEL

    /// Returns statistics summarizing a data frame
    static Poco::JSON::Object summarize( const DataFrame& _df );

    // ---------------------------------------------------------------

   private:
    /// This is needed for the column_densities and target_averages
    static void calculate_step_sizes_and_num_bins(
        const std::vector<AUTOSQL_FLOAT>& _minima,
        const std::vector<AUTOSQL_FLOAT>& _maxima,
        const AUTOSQL_FLOAT _num_bins,
        std::vector<AUTOSQL_FLOAT>& _step_sizes,
        std::vector<AUTOSQL_INT>& _actual_num_bins );

    /// Divides results by the number of rows
    static void divide_by_nrows(
        const AUTOSQL_INT _nrows, std::vector<AUTOSQL_FLOAT>& _results );

    /// Identifies the correct bin for _val based on _step_size and _min.
    static AUTOSQL_INT identify_bin(
        const AUTOSQL_INT _num_bins,
        const AUTOSQL_FLOAT _step_size,
        const AUTOSQL_FLOAT _val,
        const AUTOSQL_FLOAT _min );

    /// Finds the maximum elements in each column
    static std::vector<AUTOSQL_FLOAT> max( const Matrix<AUTOSQL_FLOAT>& _mat );

    /// Finds the mean elements in each column
    static std::vector<AUTOSQL_FLOAT> mean( const Matrix<AUTOSQL_FLOAT>& _mat );

    /// Finds the minimum elements in each column
    static std::vector<AUTOSQL_FLOAT> min( const Matrix<AUTOSQL_FLOAT>& _mat );

    /// Finds the minimum and maximum, in parallel.
    static void min_and_max(
        const containers::Matrix<AUTOSQL_FLOAT>& _mat,
        AUTOSQL_COMMUNICATOR* _comm,
        std::vector<AUTOSQL_FLOAT>& _minima,
        std::vector<AUTOSQL_FLOAT>& _maxima );

    /// Finds the share of nan (for AUTOSQL_FLOAT)
    static std::vector<AUTOSQL_FLOAT> share_nan(
        const Matrix<AUTOSQL_FLOAT>& _mat );

    /// Finds the share of nan (for AUTOSQL_INT)
    static std::vector<AUTOSQL_FLOAT> share_nan(
        const Matrix<AUTOSQL_INT>& _mat );

    /// Returns statistics summarizing a float matrix
    static Poco::JSON::Object summarize( const Matrix<AUTOSQL_FLOAT>& _mat );

    /// Returns statistics summarizing an int matrix
    static Poco::JSON::Object summarize( const Matrix<AUTOSQL_INT>& _mat );
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

#ifdef AUTOSQL_PARALLEL

template <typename T>
void Summarizer::reduce_min_max( AUTOSQL_COMMUNICATOR& _comm, T& _min, T& _max )
{
    T global_min = 0;

    T global_max = 0;

    AUTOSQL_PARALLEL_LIB::all_reduce(
        _comm,              // comm
        _min,               // in_value
        global_min,         // out_value
        AUTOSQL_MIN_OP<T>()  // op
    );

    _comm.barrier();

    _min = global_min;

    AUTOSQL_PARALLEL_LIB::all_reduce(
        _comm,              // comm
        _max,               // in_value
        global_max,         // out_value
        AUTOSQL_MAX_OP<T>()  // op
    );

    _comm.barrier();

    _max = global_max;
}

#endif  // AUTOSQL_PARALLEL

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_SUMMARIZER_HPP_
