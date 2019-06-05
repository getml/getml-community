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
    static std::vector<std::vector<SQLNET_INT>> calculate_column_densities(
        const SQLNET_INT _num_bins,
        const containers::Matrix<SQLNET_FLOAT>& _mat,
        SQLNET_COMMUNICATOR* _comm );

    /// Calculates the pearson r between features and
    /// a set of targets.
    static std::vector<std::vector<SQLNET_FLOAT>>
    calculate_feature_correlations(
        const containers::Matrix<SQLNET_FLOAT>& _features,
        const containers::DataFrameView& _targets,
        SQLNET_COMMUNICATOR* _comm );

    /// Calculates the data needed for the plots in the feature view.
    static void calculate_feature_plots(
        const SQLNET_INT _num_bins,
        const containers::Matrix<SQLNET_FLOAT>& _mat,
        const containers::DataFrameView& _targets,
        SQLNET_COMMUNICATOR* _comm,
        std::vector<std::vector<SQLNET_FLOAT>>& _labels,
        std::vector<std::vector<SQLNET_INT>>& _feature_densities,
        std::vector<std::vector<std::vector<SQLNET_FLOAT>>>& _average_targets );

#ifdef SQLNET_PARALLEL

    /// Identifies the global minimum and the global maximum
    template <typename T>
    static void reduce_min_max( SQLNET_COMMUNICATOR& _comm, T& _min, T& _max );

#endif  // SQLNET_PARALLEL

    /// Returns statistics summarizing a data frame
    static Poco::JSON::Object summarize( const DataFrame& _df );

    // ---------------------------------------------------------------

   private:
    /// This is needed for the column_densities and target_averages
    static void calculate_step_sizes_and_num_bins(
        const std::vector<SQLNET_FLOAT>& _minima,
        const std::vector<SQLNET_FLOAT>& _maxima,
        const SQLNET_FLOAT _num_bins,
        std::vector<SQLNET_FLOAT>& _step_sizes,
        std::vector<SQLNET_INT>& _actual_num_bins );

    /// Divides results by the number of rows
    static void divide_by_nrows(
        const SQLNET_INT _nrows, std::vector<SQLNET_FLOAT>& _results );

    /// Identifies the correct bin for _val based on _step_size and _min.
    static SQLNET_INT identify_bin(
        const SQLNET_INT _num_bins,
        const SQLNET_FLOAT _step_size,
        const SQLNET_FLOAT _val,
        const SQLNET_FLOAT _min );

    /// Finds the maximum elements in each column
    static std::vector<SQLNET_FLOAT> max( const Matrix<SQLNET_FLOAT>& _mat );

    /// Finds the mean elements in each column
    static std::vector<SQLNET_FLOAT> mean( const Matrix<SQLNET_FLOAT>& _mat );

    /// Finds the minimum elements in each column
    static std::vector<SQLNET_FLOAT> min( const Matrix<SQLNET_FLOAT>& _mat );

    /// Finds the minimum and maximum, in parallel.
    static void min_and_max(
        const containers::Matrix<SQLNET_FLOAT>& _mat,
        SQLNET_COMMUNICATOR* _comm,
        std::vector<SQLNET_FLOAT>& _minima,
        std::vector<SQLNET_FLOAT>& _maxima );

    /// Finds the share of nan (for SQLNET_FLOAT)
    static std::vector<SQLNET_FLOAT> share_nan(
        const Matrix<SQLNET_FLOAT>& _mat );

    /// Finds the share of nan (for SQLNET_INT)
    static std::vector<SQLNET_FLOAT> share_nan(
        const Matrix<SQLNET_INT>& _mat );

    /// Returns statistics summarizing a float matrix
    static Poco::JSON::Object summarize( const Matrix<SQLNET_FLOAT>& _mat );

    /// Returns statistics summarizing an int matrix
    static Poco::JSON::Object summarize( const Matrix<SQLNET_INT>& _mat );
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

#ifdef SQLNET_PARALLEL

template <typename T>
void Summarizer::reduce_min_max( SQLNET_COMMUNICATOR& _comm, T& _min, T& _max )
{
    T global_min = 0;

    T global_max = 0;

    SQLNET_PARALLEL_LIB::all_reduce(
        _comm,              // comm
        _min,               // in_value
        global_min,         // out_value
        SQLNET_MIN_OP<T>()  // op
    );

    _comm.barrier();

    _min = global_min;

    SQLNET_PARALLEL_LIB::all_reduce(
        _comm,              // comm
        _max,               // in_value
        global_max,         // out_value
        SQLNET_MAX_OP<T>()  // op
    );

    _comm.barrier();

    _max = global_max;
}

#endif  // SQLNET_PARALLEL

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_SUMMARIZER_HPP_
