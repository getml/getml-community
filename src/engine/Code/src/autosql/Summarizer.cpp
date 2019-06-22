#include "containers/containers.hpp"

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

std::vector<std::vector<Int>> Summarizer::calculate_column_densities(
    const Int _num_bins,
    const containers::Matrix<Float>& _mat,
    multithreading::Communicator* _comm )
{
    // ------------------------------------------------------------------------
    // Find minima and maxima

    auto minima = std::vector<Float>( 0 );

    auto maxima = std::vector<Float>( 0 );

    min_and_max( _mat, _comm, minima, maxima );

    // ------------------------------------------------------------------------
    // Calculate step_sizes and actual_num_bins. Note that actual_num_bins
    // can be smaller than num_bins.

    auto step_sizes = std::vector<Float>( 0 );

    auto actual_num_bins = std::vector<Int>( 0 );

    calculate_step_sizes_and_num_bins(
        minima,
        maxima,
        static_cast<Float>( _num_bins ),
        step_sizes,
        actual_num_bins );

    // ------------------------------------------------------------------------
    // Calculate column densities.

    assert( actual_num_bins.size() == step_sizes.size() );

    assert( actual_num_bins.size() == static_cast<size_t>( _mat.ncols() ) );

    auto column_densities =
        std::vector<std::vector<Int>>( _mat.ncols() );

    for ( size_t j = 0; j < column_densities.size(); ++j )
        {
            assert( actual_num_bins[j] >= 0 );
            column_densities[j].resize( actual_num_bins[j] );
        }

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    const auto val = _mat( i, j );

                    if ( actual_num_bins[j] == 0 || std::isinf( val ) ||
                         std::isnan( val ) )
                        {
                            continue;
                        }

                    auto bin = identify_bin(
                        actual_num_bins[j], step_sizes[j], val, minima[j] );

                    ++column_densities[j][bin];
                }
        }

#ifdef AUTOSQL_PARALLEL

    for ( size_t j = 0; j < column_densities.size(); ++j )
        {
            auto global = std::vector<Int>( column_densities[j].size() );

            AUTOSQL_PARALLEL_LIB::all_reduce(
                *_comm,                                    // comm
                column_densities[j].data(),                // in_values
                static_cast<Int>( global.size() ),  // count,
                global.data(),                             // out_values
                std::plus<Int>()                    // op
            );

            _comm->barrier();

            column_densities[j] = std::move( global );
        }

#endif  // AUTOSQL_PARALLEL

    // ------------------------------------------------------------------------

    return column_densities;
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>>
Summarizer::calculate_feature_correlations(
    const containers::Matrix<Float>& _features,
    const containers::DataFrameView& _targets,
    multithreading::Communicator* _comm )
{
    // -----------------------------------------------------------

    assert( _features.nrows() == _targets.nrows() );

    // -----------------------------------------------------------
    // Prepare datasets

    containers::Matrix<Float> sum_yhat( _features.ncols(), 1 );

    containers::Matrix<Float> sum_yhat_yhat( _features.ncols(), 1 );

    containers::Matrix<Float> sum_y(
        _targets.df().targets().ncols(), 1 );

    containers::Matrix<Float> sum_y_y(
        _targets.df().targets().ncols(), 1 );

    containers::Matrix<Float> sum_yhat_y(
        _features.ncols(), _targets.df().targets().ncols() );

    // -----------------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( Int i = 0; i < _features.nrows(); ++i )
        for ( Int j = 0; j < _features.ncols(); ++j )
            sum_yhat[j] += _features( i, j );

    // Calculate sum yhat_yhat
    for ( Int i = 0; i < _features.nrows(); ++i )
        for ( Int j = 0; j < _features.ncols(); ++j )
            sum_yhat_yhat[j] += _features( i, j ) * _features( i, j );

    // Calculate sum y
    for ( Int i = 0; i < _targets.nrows(); ++i )
        for ( Int k = 0; k < _targets.df().targets().ncols(); ++k )
            sum_y[k] += _targets.targets( i, k );

    // Calculate sum y_y
    for ( Int i = 0; i < _targets.nrows(); ++i )
        for ( Int k = 0; k < _targets.df().targets().ncols(); ++k )
            sum_y_y[k] += _targets.targets( i, k ) * _targets.targets( i, k );

    // Calculate sum_yhat_y
    for ( Int i = 0; i < _features.nrows(); ++i )
        for ( Int j = 0; j < _features.ncols(); ++j )
            for ( Int k = 0; k < _targets.df().targets().ncols(); ++k )
                sum_yhat_y( j, k ) +=
                    _features( i, j ) * _targets.targets( i, k );

    Float n = static_cast<Float>( _features.nrows() );

    // -----------------------------------------------------
    // Reduce sufficient statistics, if necessary

    // sum_yhat
    {
#ifdef AUTOSQL_PARALLEL

        containers::Matrix<Float> global(
            sum_yhat.nrows(), sum_yhat.ncols() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                       // comm
            sum_yhat.data(),                  // in_values
            global.nrows() * global.ncols(),  // count,
            global.data(),                    // out_values
            std::plus<Float>()         // op
        );

        _comm->barrier();

        sum_yhat = std::move( global );

#endif  // AUTOSQL_PARALLEL
    }

    // sum_yhat_yhat
    {
#ifdef AUTOSQL_PARALLEL

        containers::Matrix<Float> global(
            sum_yhat_yhat.nrows(), sum_yhat_yhat.ncols() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                       // comm
            sum_yhat_yhat.data(),             // in_values
            global.nrows() * global.ncols(),  // count,
            global.data(),                    // out_values
            std::plus<Float>()         // op
        );

        _comm->barrier();

        sum_yhat_yhat = std::move( global );

#endif  // AUTOSQL_PARALLEL
    }

    // sum_y
    {
#ifdef AUTOSQL_PARALLEL

        containers::Matrix<Float> global( sum_y.nrows(), sum_y.ncols() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                       // comm
            sum_y.data(),                     // in_values
            global.nrows() * global.ncols(),  // count,
            global.data(),                    // out_values
            std::plus<Float>()         // op
        );

        _comm->barrier();

        sum_y = std::move( global );

#endif  // AUTOSQL_PARALLEL
    }

    // sum_y_y
    {
#ifdef AUTOSQL_PARALLEL

        containers::Matrix<Float> global(
            sum_y_y.nrows(), sum_y_y.ncols() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                       // comm
            sum_y_y.data(),                   // in_values
            global.nrows() * global.ncols(),  // count,
            global.data(),                    // out_values
            std::plus<Float>()         // op
        );

        _comm->barrier();

        sum_y_y = std::move( global );

#endif  // AUTOSQL_PARALLEL
    }

    // sum_yhat_y
    {
#ifdef AUTOSQL_PARALLEL

        containers::Matrix<Float> global(
            sum_yhat_y.nrows(), sum_yhat_y.ncols() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                       // comm
            sum_yhat_y.data(),                // in_values
            global.nrows() * global.ncols(),  // count,
            global.data(),                    // out_values
            std::plus<Float>()         // op
        );

        _comm->barrier();

        sum_yhat_y = std::move( global );

#endif  // AUTOSQL_PARALLEL
    }

    // n
    {
#ifdef AUTOSQL_PARALLEL

        Float global = 0.0;

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *( _comm ),                // comm
            &n,                        // in_value
            1,                         // count
            &global,                   // out_value
            std::plus<Float>()  // op
        );

        _comm->barrier();

        n = global;

#endif  // AUTOSQL_PARALLEL
    }

    // -----------------------------------------------------------
    // Calculate correlations from sufficient statistics

    auto feature_correlations = std::vector<std::vector<Float>>(
        _features.ncols(),
        std::vector<Float>( _targets.df().targets().ncols() ) );

#ifdef AUTOSQL_MULTITHREADING
    if ( _comm->rank() == 0 )
        {
#endif  // AUTOSQL_MULTITHREADING

            for ( Int j = 0; j < _features.ncols(); ++j )
                for ( Int k = 0; k < _targets.df().targets().ncols();
                      ++k )
                    {
                        const Float var_yhat =
                            sum_yhat_yhat[j] / n -
                            ( sum_yhat[j] / n ) * ( sum_yhat[j] / n );

                        const Float var_y =
                            sum_y_y[k] / n -
                            ( sum_y[k] / n ) * ( sum_y[k] / n );

                        const Float cov_y_yhat =
                            sum_yhat_y( j, k ) / n -
                            ( sum_yhat[j] / n ) * ( sum_y[k] / n );

                        feature_correlations[j][k] =
                            cov_y_yhat / sqrt( var_yhat * var_y );

                        if ( feature_correlations[j][k] !=
                             feature_correlations[j][k] )
                            {
                                feature_correlations[j][k] = 0.0;
                            }
                    }

#ifdef AUTOSQL_MULTITHREADING
        }
#endif  // AUTOSQL_MULTITHREADING

    // -----------------------------------------------------------

    return feature_correlations;
}

// ----------------------------------------------------------------------------

void Summarizer::calculate_feature_plots(
    const Int _num_bins,
    const containers::Matrix<Float>& _mat,
    const containers::DataFrameView& _targets,
    multithreading::Communicator* _comm,
    std::vector<std::vector<Float>>& _labels,
    std::vector<std::vector<Int>>& _feature_densities,
    std::vector<std::vector<std::vector<Float>>>& _average_targets )
{
    // ------------------------------------------------------------------------
    // Find minima and maxima

    auto minima = std::vector<Float>( 0 );

    auto maxima = std::vector<Float>( 0 );

    min_and_max( _mat, _comm, minima, maxima );

    // ------------------------------------------------------------------------
    // Calculate step_sizes and actual_num_bins. Note that actual_num_bins
    // can be smaller than num_bins.

    auto step_sizes = std::vector<Float>( 0 );

    auto actual_num_bins = std::vector<Int>( 0 );

    calculate_step_sizes_and_num_bins(
        minima,
        maxima,
        static_cast<Float>( _num_bins ),
        step_sizes,
        actual_num_bins );

    // ------------------------------------------------------------------------
    // Init labels

    auto labels =
        std::vector<std::vector<Float>>( actual_num_bins.size() );

    for ( size_t i = 0; i < labels.size(); ++i )
        {
            labels[i].resize( actual_num_bins[i] );
        }

    // ------------------------------------------------------------------------
    // Init feature_densities and _average_targets.

    assert( actual_num_bins.size() == step_sizes.size() );

    assert( actual_num_bins.size() == static_cast<size_t>( _mat.ncols() ) );

    assert( _targets.nrows() == _mat.nrows() );

    const auto num_targets = _targets.df().targets().ncols();

    auto feature_densities =
        std::vector<std::vector<Int>>( _mat.ncols() );

    auto average_targets =
        std::vector<std::vector<std::vector<Float>>>( _mat.ncols() );

    std::for_each(
        average_targets.begin(),
        average_targets.end(),
        [num_targets]( std::vector<std::vector<Float>>& vec ) {
            vec.resize( num_targets );
        } );

    for ( size_t j = 0; j < feature_densities.size(); ++j )
        {
            assert( actual_num_bins[j] >= 0 );

            feature_densities[j].resize( actual_num_bins[j] );

            for ( Int k = 0; k < num_targets; ++k )
                {
                    average_targets[j][k].resize( actual_num_bins[j] );
                }
        }

    // ------------------------------------------------------------------------
    // Calculate sums.

    assert( feature_densities.size() == static_cast<size_t>( _mat.ncols() ) );

    assert( labels.size() == static_cast<size_t>( _mat.ncols() ) );

    assert( average_targets.size() == static_cast<size_t>( _mat.ncols() ) );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    const auto val = _mat( i, j );

                    if ( actual_num_bins[j] == 0 || std::isinf( val ) ||
                         std::isnan( val ) )
                        {
                            continue;
                        }

                    const auto bin = identify_bin(
                        actual_num_bins[j],
                        step_sizes[j],
                        val,
                        minima[j] );

                    assert( bin >= 0 );

                    assert(
                        bin < static_cast<Int>(
                                  feature_densities[j].size() ) );

                    ++feature_densities[j][bin];

                    labels[j][bin] += val;

                    for ( size_t k = 0; k < average_targets[j].size(); ++k )
                        {
                            assert(
                                bin < static_cast<Int>(
                                          average_targets[j][k].size() ) );

                            average_targets[j][k][bin] +=
                                _targets.targets( i, k );
                        }
                }
        }

        // ------------------------------------------------------------------------
        // Reduce, if necessary

#ifdef AUTOSQL_PARALLEL

    for ( size_t j = 0; j < labels.size(); ++j )
        {
            auto global = std::vector<Float>( labels[j].size() );

            AUTOSQL_PARALLEL_LIB::all_reduce(
                *_comm,                                      // comm
                labels[j].data(),                            // in_values
                static_cast<Float>( global.size() ),  // count,
                global.data(),                               // out_values
                std::plus<Float>()                    // op
            );

            _comm->barrier();

            labels[j] = std::move( global );
        }

    for ( size_t j = 0; j < feature_densities.size(); ++j )
        {
            auto global =
                std::vector<Int>( feature_densities[j].size() );

            AUTOSQL_PARALLEL_LIB::all_reduce(
                *_comm,                                    // comm
                feature_densities[j].data(),               // in_values
                static_cast<Int>( global.size() ),  // count,
                global.data(),                             // out_values
                std::plus<Int>()                    // op
            );

            _comm->barrier();

            feature_densities[j] = std::move( global );
        }

    for ( size_t j = 0; j < average_targets.size(); ++j )
        {
            for ( size_t k = 0; k < average_targets[j].size(); ++k )
                {
                    auto global = std::vector<Float>(
                        average_targets[j][k].size() );

                    AUTOSQL_PARALLEL_LIB::all_reduce(
                        *_comm,                                    // comm
                        average_targets[j][k].data(),              // in_values
                        static_cast<Int>( global.size() ),  // count,
                        global.data(),                             // out_values
                        std::plus<Float>()                  // op
                    );

                    _comm->barrier();

                    average_targets[j][k] = std::move( global );
                }
        }

#endif  // AUTOSQL_PARALLEL

    // ------------------------------------------------------------------------
    // Divide sums of targets by frequencies to get average_targets.

    assert( feature_densities.size() == average_targets.size() );

    assert( feature_densities.size() == labels.size() );

    for ( size_t j = 0; j < feature_densities.size(); ++j )
        {
            for ( size_t bin = 0; bin < feature_densities[j].size(); ++bin )
                {
                    assert( feature_densities[j].size() == labels[j].size() );

                    if ( feature_densities[j][bin] == 0 )
                        {
                            continue;
                        }

                    const auto freq =
                        static_cast<Float>( feature_densities[j][bin] );

                    labels[j][bin] /= freq;

                    for ( size_t k = 0; k < average_targets[j].size(); ++k )
                        {
                            assert(
                                feature_densities[j].size() ==
                                average_targets[j][k].size() );

                            average_targets[j][k][bin] /= freq;
                        }
                }
        }

    // ------------------------------------------------------------------------
    // Copy to data passed by reference

    assert( feature_densities.size() == average_targets.size() );

    assert( feature_densities.size() == labels.size() );

    _labels =
        std::vector<std::vector<Float>>( feature_densities.size() );

    _feature_densities =
        std::vector<std::vector<Int>>( feature_densities.size() );

    _average_targets = std::vector<std::vector<std::vector<Float>>>(
        feature_densities.size(),
        std::vector<std::vector<Float>>( num_targets ) );

    for ( size_t j = 0; j < feature_densities.size(); ++j )
        {
            for ( size_t bin = 0; bin < feature_densities[j].size(); ++bin )
                {
                    assert( feature_densities[j].size() == labels[j].size() );

                    if ( feature_densities[j][bin] == 0 )
                        {
                            continue;
                        }

                    _labels[j].push_back( labels[j][bin] );

                    _feature_densities[j].push_back(
                        feature_densities[j][bin] );

                    for ( size_t k = 0; k < average_targets[j].size(); ++k )
                        {
                            _average_targets[j][k].push_back(
                                average_targets[j][k][bin] );
                        }
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Summarizer::calculate_step_sizes_and_num_bins(
    const std::vector<Float>& _minima,
    const std::vector<Float>& _maxima,
    const Float _num_bins,
    std::vector<Float>& _step_sizes,
    std::vector<Int>& _actual_num_bins )
{
    assert( _minima.size() == _maxima.size() );

    _step_sizes.resize( _minima.size() );

    _actual_num_bins.resize( _minima.size() );

    for ( size_t j = 0; j < _minima.size(); ++j )
        {
            const auto min = _minima[j];

            const auto max = _maxima[j];

            if ( min >= max )
                {
                    continue;
                }

            _step_sizes[j] = ( max - min ) / _num_bins;

            _actual_num_bins[j] =
                static_cast<Int>( ( max - min ) / _step_sizes[j] );
        }
}

// ----------------------------------------------------------------------------

void Summarizer::divide_by_nrows(
    const Int _nrows, std::vector<Float>& _results )
{
    const auto nrows = static_cast<Float>( _nrows );

    std::for_each(
        _results.begin(), _results.end(), [nrows]( Float& val ) {
            val /= nrows;
        } );
}

// ----------------------------------------------------------------------------

Int Summarizer::identify_bin(
    const Int _num_bins,
    const Float _step_size,
    const Float _val,
    const Float _min )
{
    assert( _step_size > 0.0 );

    // Note that this always rounds down.
    auto bin = static_cast<Int>( ( _val - _min ) / _step_size );

    assert( bin >= 0 );

    // The maximum value will be out of range, if we do not
    // do this!
    if ( bin >= _num_bins )
        {
            bin = _num_bins - 1;
        }

    return bin;
}

// ----------------------------------------------------------------------------

std::vector<Float> Summarizer::max( const Matrix<Float>& _mat )
{
    std::vector<Float> results(
        _mat.ncols(), std::numeric_limits<Float>::lowest() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) > results[j] )
                        {
                            results[j] = _mat( i, j );
                        }
                }
        }

    return results;
}

// ----------------------------------------------------------------------------

std::vector<Float> Summarizer::mean( const Matrix<Float>& _mat )
{
    std::vector<Float> results( _mat.ncols() );

    std::vector<Float> nrows( _mat.ncols() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) == _mat( i, j ) )
                        {
                            results[j] += _mat( i, j );
                            ++nrows[j];
                        }
                }
        }

    std::transform(
        results.begin(),
        results.end(),
        nrows.begin(),
        results.begin(),
        []( const Float val, const Float nrows ) {
            if ( nrows != 0.0 )
                {
                    return val / nrows;
                }
            else
                {
                    return 0.0;
                }
        } );

    return results;
}

// ----------------------------------------------------------------------------

std::vector<Float> Summarizer::min( const Matrix<Float>& _mat )
{
    std::vector<Float> results(
        _mat.ncols(), std::numeric_limits<Float>::max() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) < results[j] )
                        {
                            results[j] = _mat( i, j );
                        }
                }
        }

    return results;
}

// ----------------------------------------------------------------------------

void Summarizer::min_and_max(
    const containers::Matrix<Float>& _mat,
    multithreading::Communicator* _comm,
    std::vector<Float>& _minima,
    std::vector<Float>& _maxima )
{
    _minima = std::vector<Float>(
        _mat.ncols(), std::numeric_limits<Float>::max() );

    _maxima = std::vector<Float>(
        _mat.ncols(), std::numeric_limits<Float>::lowest() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) < _minima[j] )
                        {
                            _minima[j] = _mat( i, j );
                        }
                    else if ( _mat( i, j ) > _maxima[j] )
                        {
                            _maxima[j] = _mat( i, j );
                        }
                }
        }

#ifdef AUTOSQL_PARALLEL

    assert( _minima.size() == _maxima.size() );

    for ( size_t j = 0; j < _maxima.size(); ++j )
        {
            reduce_min_max( *_comm, _minima[j], _maxima[j] );
        }

#endif  // AUTOSQL_PARALLEL
}

// ----------------------------------------------------------------------------

std::vector<Float> Summarizer::share_nan(
    const Matrix<Float>& _mat )
{
    std::vector<Float> results( _mat.ncols() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) != _mat( i, j ) )
                        {
                            results[j] += 1.0;
                        }
                }
        }

    divide_by_nrows( _mat.nrows(), results );

    return results;
}

// ----------------------------------------------------------------------------

std::vector<Float> Summarizer::share_nan(
    const Matrix<Int>& _mat )
{
    std::vector<Float> results( _mat.ncols() );

    for ( Int i = 0; i < _mat.nrows(); ++i )
        {
            for ( Int j = 0; j < _mat.ncols(); ++j )
                {
                    if ( _mat( i, j ) == -1 )
                        {
                            results[j] += 1.0;
                        }
                }
        }

    divide_by_nrows( _mat.nrows(), results );

    return results;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::summarize( const Matrix<Float>& _mat )
{
    Poco::JSON::Object summary;

    summary.set( "max_", max( _mat ) );

    summary.set( "mean_", mean( _mat ) );

    summary.set( "min_", min( _mat ) );

    summary.set( "share_nan_", share_nan( _mat ) );

    return summary;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::summarize( const Matrix<Int>& _mat )
{
    Poco::JSON::Object summary;

    summary.set( "share_nan_", share_nan( _mat ) );

    return summary;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::summarize( const DataFrame& _df )
{
    Poco::JSON::Object summary;

    Poco::JSON::Object();

    summary.set( "categorical_", summarize( _df.categorical() ) );

    summary.set( "discrete_", summarize( _df.discrete() ) );

    Poco::JSON::Array join_keys;

    for ( auto& jk : _df.join_keys() )
        {
            join_keys.add( summarize( jk ) );
        }

    summary.set( "join_keys_", join_keys );

    summary.set( "numerical_", summarize( _df.numerical() ) );

    summary.set( "targets_", summarize( _df.targets() ) );

    Poco::JSON::Array time_stamps;

    for ( auto& ts : _df.time_stamps_all() )
        {
            time_stamps.add( summarize( ts ) );
        }

    summary.set( "time_stamps_", time_stamps );

    return summary;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql
