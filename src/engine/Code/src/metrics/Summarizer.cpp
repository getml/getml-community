#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Summarizer::calculate_average_targets(
    const std::vector<Float>& _minima,
    const std::vector<Float>& _step_sizes,
    const std::vector<size_t>& _actual_num_bins,
    const std::vector<std::vector<Int>>& _feature_densities,
    const Features& _features,
    const size_t _nrows,
    const size_t _ncols,
    const std::vector<const Float*>& _targets )
{
    // ------------------------------------------------------------------------
    // Init average_targets.

    assert( _actual_num_bins.size() == _step_sizes.size() );

    assert( _actual_num_bins.size() == _ncols );

    const auto num_targets = _targets.size();

    auto average_targets =
        std::vector<std::vector<std::vector<Float>>>( _ncols );

    std::for_each(
        average_targets.begin(),
        average_targets.end(),
        [num_targets]( std::vector<std::vector<Float>>& vec ) {
            vec.resize( num_targets );
        } );

    for ( size_t j = 0; j < _feature_densities.size(); ++j )
        {
            for ( size_t k = 0; k < num_targets; ++k )
                {
                    average_targets[j][k].resize( _actual_num_bins[j] );
                }
        }

    // ------------------------------------------------------------------------
    // Calculate sums.

    assert( _feature_densities.size() == _ncols );

    for ( size_t i = 0; i < _nrows; ++i )
        {
            for ( size_t j = 0; j < _ncols; ++j )
                {
                    const auto val = get( i, j, _features );

                    if ( _actual_num_bins[j] == 0 || std::isinf( val ) ||
                         std::isnan( val ) )
                        {
                            continue;
                        }

                    const auto bin = identify_bin(
                        _actual_num_bins[j], _step_sizes[j], val, _minima[j] );

                    assert( bin < _feature_densities[j].size() );

                    for ( size_t k = 0; k < average_targets[j].size(); ++k )
                        {
                            assert( bin < average_targets[j][k].size() );

                            average_targets[j][k][bin] += _targets[k][i];
                        }
                }
        }

    // ------------------------------------------------------------------------
    // Divide targets by column densities .

    for ( size_t j = 0; j < _ncols; ++j )
        {
            for ( size_t bin = 0; bin < _feature_densities[j].size(); ++bin )
                {
                    if ( _feature_densities[j][bin] > 0 )
                        {
                            for ( size_t k = 0; k < average_targets[j].size();
                                  ++k )
                                {
                                    average_targets[j][k][bin] /=
                                        static_cast<Float>(
                                            _feature_densities[j][bin] );
                                }
                        }
                }
        }

    // ------------------------------------------------------------------------
    // Transform to JSON Array.

    Poco::JSON::Array::Ptr arr( new Poco::JSON::Array() );

    for ( const auto& vec : average_targets )
        {
            Poco::JSON::Array::Ptr subarr( new Poco::JSON::Array() );

            for ( const auto& v : vec )
                {
                    subarr->add( JSON::vector_to_array_ptr( v ) );
                }

            arr->add( subarr );
        }

    // ------------------------------------------------------------------------

    return arr;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_plots(
    const Features& _features,
    const size_t _nrows,
    const size_t _ncols,
    const size_t _num_bins,
    const std::vector<const Float*>& _targets )
{
    // ------------------------------------------------------------------------
    // Find minima and maxima

    auto minima = std::vector<Float>( 0 );

    auto maxima = std::vector<Float>( 0 );

    find_min_and_max( _features, _nrows, _ncols, &minima, &maxima );

    // ------------------------------------------------------------------------
    // Calculate step_sizes and actual_num_bins. Note that actual_num_bins
    // can be smaller than num_bins.

    auto step_sizes = std::vector<Float>( 0 );

    auto actual_num_bins = std::vector<size_t>( 0 );

    calculate_step_sizes_and_num_bins(
        minima,
        maxima,
        static_cast<Float>( _num_bins ),
        &step_sizes,
        &actual_num_bins );

    assert( step_sizes.size() == _ncols );
    assert( actual_num_bins.size() == _ncols );

    // ------------------------------------------------------------------------
    // Init feature densities.

    auto feature_densities = std::vector<std::vector<Int>>( _ncols );

    for ( size_t j = 0; j < _ncols; ++j )
        {
            feature_densities[j].resize( actual_num_bins[j] );
        }

    // ------------------------------------------------------------------------
    // Calculate feature densities.

    for ( size_t i = 0; i < _nrows; ++i )
        {
            for ( size_t j = 0; j < _ncols; ++j )
                {
                    const auto val = get( i, j, _features );

                    if ( actual_num_bins[j] == 0 || std::isinf( val ) ||
                         std::isnan( val ) )
                        {
                            continue;
                        }

                    auto bin = identify_bin(
                        actual_num_bins[j], step_sizes[j], val, minima[j] );

                    ++feature_densities[j][bin];
                }
        }

    // ------------------------------------------------------------------------
    // Transform to JSON Array.

    Poco::JSON::Array::Ptr densities_arr( new Poco::JSON::Array() );

    for ( const auto& c : feature_densities )
        {
            densities_arr->add( JSON::vector_to_array_ptr( c ) );
        }

    // ------------------------------------------------------------------------

    auto labels_arr = calculate_labels(
        minima,
        step_sizes,
        actual_num_bins,
        feature_densities,
        _features,
        _nrows,
        _ncols );

    // ------------------------------------------------------------------------

    auto average_targets_arr = calculate_average_targets(
        minima,
        step_sizes,
        actual_num_bins,
        feature_densities,
        _features,
        _nrows,
        _ncols,
        _targets );

    // ------------------------------------------------------------------------
    // Add to JSON object.

    Poco::JSON::Object obj;

    obj.set( "average_targets_", average_targets_arr );

    obj.set( "feature_densities_", densities_arr );

    obj.set( "labels_", labels_arr );

    return obj;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_correlations(
    const Features& _features,
    const size_t _nrows,
    const size_t _ncols,
    const std::vector<const Float*>& _targets )
{
    // -----------------------------------------------------------

    assert( _ncols == _features.size() );

    const auto t_ncols = _targets.size();

    // -----------------------------------------------------------
    // Prepare datasets

    std::vector<Float> sum_yhat( _ncols );

    std::vector<Float> sum_yhat_yhat( _ncols );

    std::vector<Float> sum_y( t_ncols );

    std::vector<Float> sum_y_y( t_ncols );

    std::vector<Float> sum_yhat_y( _ncols * t_ncols );

    // -----------------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat[j] += get( i, j, _features );

    // Calculate sum yhat_yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat_yhat[j] += get( i, j, _features ) * get( i, j, _features );

    // Calculate sum y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i ) sum_y[k] += _targets[k][i];

    // Calculate sum y_y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i )
            sum_y_y[k] += _targets[k][i] * _targets[k][i];

    // Calculate sum_yhat_y
    for ( size_t k = 0; k < t_ncols; ++k )
        for ( size_t i = 0; i < _nrows; ++i )
            for ( size_t j = 0; j < _ncols; ++j )
                get( j, k, t_ncols, &sum_yhat_y ) +=
                    get( i, j, _features ) * _targets[k][i];

    // -----------------------------------------------------------
    // Calculate correlations from sufficient statistics

    const auto n = static_cast<Float>( _nrows );

    auto feature_correlations = std::vector<std::vector<Float>>(
        _ncols, std::vector<Float>( t_ncols ) );

    for ( size_t j = 0; j < _ncols; ++j )
        for ( size_t k = 0; k < t_ncols; ++k )
            {
                const Float var_yhat =
                    sum_yhat_yhat[j] / n -
                    ( sum_yhat[j] / n ) * ( sum_yhat[j] / n );

                const Float var_y =
                    sum_y_y[k] / n - ( sum_y[k] / n ) * ( sum_y[k] / n );

                const Float cov_y_yhat = get( j, k, t_ncols, &sum_yhat_y ) / n -
                                         ( sum_yhat[j] / n ) * ( sum_y[k] / n );

                feature_correlations[j][k] =
                    cov_y_yhat / sqrt( var_yhat * var_y );

                if ( feature_correlations[j][k] != feature_correlations[j][k] )
                    {
                        feature_correlations[j][k] = 0.0;
                    }
            }

    // -----------------------------------------------------------
    // Transform to JSON Array.

    Poco::JSON::Array::Ptr arr( new Poco::JSON::Array() );

    for ( const auto& f : feature_correlations )
        {
            arr->add( JSON::vector_to_array_ptr( f ) );
        }

    // -----------------------------------------------------------
    // Add to JSON object.

    Poco::JSON::Object obj;

    obj.set( "feature_correlations_", arr );

    return obj;

    // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Summarizer::calculate_labels(
    const std::vector<Float>& _minima,
    const std::vector<Float>& _step_sizes,
    const std::vector<size_t>& _actual_num_bins,
    const std::vector<std::vector<Int>>& _feature_densities,
    const Features& _features,
    const size_t _nrows,
    const size_t _ncols )
{
    // ------------------------------------------------------------------------
    // Init labels.

    auto labels = std::vector<std::vector<Float>>( _ncols );

    for ( size_t j = 0; j < _ncols; ++j )
        {
            labels[j].resize( _actual_num_bins[j] );
        }

    // ------------------------------------------------------------------------
    // Calculate feature densities.

    for ( size_t i = 0; i < _nrows; ++i )
        {
            for ( size_t j = 0; j < _ncols; ++j )
                {
                    const auto val = get( i, j, _features );

                    if ( _actual_num_bins[j] == 0 || std::isinf( val ) ||
                         std::isnan( val ) )
                        {
                            continue;
                        }

                    auto bin = identify_bin(
                        _actual_num_bins[j], _step_sizes[j], val, _minima[j] );

                    labels[j][bin] += val;
                }
        }

    // ------------------------------------------------------------------------
    // Divide labels by column densities or replace with appropriate value.

    for ( size_t j = 0; j < _ncols; ++j )
        {
            assert( labels[j].size() == _feature_densities[j].size() );

            for ( size_t bin = 0; bin < labels[j].size(); ++bin )
                {
                    if ( _feature_densities[j][bin] > 0 )
                        {
                            labels[j][bin] /= static_cast<Float>(
                                _feature_densities[j][bin] );
                        }
                    else
                        {
                            labels[j][bin] =
                                _minima[j] +
                                ( static_cast<Float>( bin ) + 0.5 ) *
                                    _step_sizes[j];
                        }
                }
        }

    // ------------------------------------------------------------------------
    // Transform to JSON Array.

    Poco::JSON::Array::Ptr arr( new Poco::JSON::Array() );

    for ( const auto& l : labels )
        {
            arr->add( JSON::vector_to_array_ptr( l ) );
        }

    // ------------------------------------------------------------------------

    return arr;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Summarizer::calculate_step_sizes_and_num_bins(
    const std::vector<Float>& _minima,
    const std::vector<Float>& _maxima,
    const Float _num_bins,
    std::vector<Float>* _step_sizes,
    std::vector<size_t>* _actual_num_bins )
{
    assert( _minima.size() == _maxima.size() );

    _step_sizes->resize( _minima.size() );

    _actual_num_bins->resize( _minima.size() );

    for ( size_t j = 0; j < _minima.size(); ++j )
        {
            const auto min = _minima[j];

            const auto max = _maxima[j];

            if ( min >= max )
                {
                    continue;
                }

            ( *_step_sizes )[j] = ( max - min ) / _num_bins;

            ( *_actual_num_bins )[j] =
                static_cast<size_t>( ( max - min ) / ( *_step_sizes )[j] );
        }
}

// ----------------------------------------------------------------------------

void Summarizer::find_min_and_max(
    const Features& _features,
    const size_t _nrows,
    const size_t _ncols,
    std::vector<Float>* _minima,
    std::vector<Float>* _maxima )
{
    *_minima = std::vector<Float>( _ncols, std::numeric_limits<Float>::max() );

    *_maxima =
        std::vector<Float>( _ncols, std::numeric_limits<Float>::lowest() );

    for ( size_t i = 0; i < _nrows; ++i )
        {
            for ( size_t j = 0; j < _ncols; ++j )
                {
                    if ( get( i, j, _features ) < ( *_minima )[j] )
                        {
                            ( *_minima )[j] = get( i, j, _features );
                        }
                    else if ( get( i, j, _features ) > ( *_maxima )[j] )
                        {
                            ( *_maxima )[j] = get( i, j, _features );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

size_t Summarizer::identify_bin(
    const size_t _num_bins,
    const Float _step_size,
    const Float _val,
    const Float _min )
{
    assert( _step_size > 0.0 );

    // Note that this always rounds down.
    auto bin = static_cast<size_t>( ( _val - _min ) / _step_size );

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

}  // namespace metrics
