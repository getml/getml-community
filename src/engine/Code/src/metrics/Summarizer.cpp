#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_densities(
    const std::vector<METRICS_FLOAT>& _features,
    const size_t _nrows,
    const size_t _ncols,
    const size_t _num_bins )
{
    // ------------------------------------------------------------------------
    // Find minima and maxima

    auto minima = std::vector<METRICS_FLOAT>( 0 );

    auto maxima = std::vector<METRICS_FLOAT>( 0 );

    find_min_and_max( _features, _nrows, _ncols, &minima, &maxima );

    // ------------------------------------------------------------------------
    // Calculate step_sizes and actual_num_bins. Note that actual_num_bins
    // can be smaller than num_bins.

    auto step_sizes = std::vector<METRICS_FLOAT>( 0 );

    auto actual_num_bins = std::vector<size_t>( 0 );

    calculate_step_sizes_and_num_bins(
        minima,
        maxima,
        static_cast<METRICS_FLOAT>( _num_bins ),
        &step_sizes,
        &actual_num_bins );

    assert( step_sizes.size() == _ncols );
    assert( actual_num_bins.size() == _ncols );

    // ------------------------------------------------------------------------
    // Init feature densities.

    auto feature_densities = std::vector<std::vector<METRICS_INT>>( _ncols );

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
                    const auto val = get( i, j, _ncols, _features );

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
    // Add to JSON object.

    Poco::JSON::Object obj;

    obj.set( "feature_densities_", densities_arr );

    obj.set( "labels_", labels_arr );

    return obj;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_correlations(
    const std::vector<METRICS_FLOAT>& _features,
    const size_t _nrows,
    const size_t _ncols,
    const std::vector<const METRICS_FLOAT*>& _targets )
{
    // -----------------------------------------------------------

    assert( _nrows * _ncols == _features.size() );

    const auto t_ncols = _targets.size();

    // -----------------------------------------------------------
    // Prepare datasets

    std::vector<METRICS_FLOAT> sum_yhat( _ncols );

    std::vector<METRICS_FLOAT> sum_yhat_yhat( _ncols );

    std::vector<METRICS_FLOAT> sum_y( t_ncols );

    std::vector<METRICS_FLOAT> sum_y_y( t_ncols );

    std::vector<METRICS_FLOAT> sum_yhat_y( _ncols * t_ncols );

    // -----------------------------------------------------------
    // Calculate sufficient statistics

    // Calculate sum yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat[j] += get( i, j, _ncols, _features );

    // Calculate sum yhat_yhat
    for ( size_t i = 0; i < _nrows; ++i )
        for ( size_t j = 0; j < _ncols; ++j )
            sum_yhat_yhat[j] +=
                get( i, j, _ncols, _features ) * get( i, j, _ncols, _features );

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
                    get( i, j, _ncols, _features ) * _targets[k][i];

    // -----------------------------------------------------------
    // Calculate correlations from sufficient statistics

    const auto n = static_cast<METRICS_FLOAT>( _nrows );

    auto feature_correlations = std::vector<std::vector<METRICS_FLOAT>>(
        _ncols, std::vector<METRICS_FLOAT>( t_ncols ) );

    for ( size_t j = 0; j < _ncols; ++j )
        for ( size_t k = 0; k < t_ncols; ++k )
            {
                const METRICS_FLOAT var_yhat =
                    sum_yhat_yhat[j] / n -
                    ( sum_yhat[j] / n ) * ( sum_yhat[j] / n );

                const METRICS_FLOAT var_y =
                    sum_y_y[k] / n - ( sum_y[k] / n ) * ( sum_y[k] / n );

                const METRICS_FLOAT cov_y_yhat =
                    get( j, k, t_ncols, &sum_yhat_y ) / n -
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
    const std::vector<METRICS_FLOAT>& _minima,
    const std::vector<METRICS_FLOAT>& _step_sizes,
    const std::vector<size_t>& _actual_num_bins,
    const std::vector<std::vector<METRICS_INT>>& _feature_densities,
    const std::vector<METRICS_FLOAT>& _features,
    const size_t _nrows,
    const size_t _ncols )
{
    // ------------------------------------------------------------------------
    // Init labels.

    auto labels = std::vector<std::vector<METRICS_FLOAT>>( _ncols );

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
                    const auto val = get( i, j, _ncols, _features );

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
                            labels[j][bin] /= static_cast<METRICS_FLOAT>(
                                _feature_densities[j][bin] );
                        }
                    else
                        {
                            labels[j][bin] =
                                _minima[j] +
                                ( static_cast<METRICS_FLOAT>( bin ) + 0.5 ) *
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
    const std::vector<METRICS_FLOAT>& _minima,
    const std::vector<METRICS_FLOAT>& _maxima,
    const METRICS_FLOAT _num_bins,
    std::vector<METRICS_FLOAT>* _step_sizes,
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
    const std::vector<METRICS_FLOAT>& _features,
    const size_t _nrows,
    const size_t _ncols,
    std::vector<METRICS_FLOAT>* _minima,
    std::vector<METRICS_FLOAT>* _maxima )
{
    *_minima = std::vector<METRICS_FLOAT>(
        _ncols, std::numeric_limits<METRICS_FLOAT>::max() );

    *_maxima = std::vector<METRICS_FLOAT>(
        _ncols, std::numeric_limits<METRICS_FLOAT>::lowest() );

    for ( size_t i = 0; i < _nrows; ++i )
        {
            for ( size_t j = 0; j < _ncols; ++j )
                {
                    if ( get( i, j, _ncols, _features ) < ( *_minima )[j] )
                        {
                            ( *_minima )[j] = get( i, j, _ncols, _features );
                        }
                    else if ( get( i, j, _ncols, _features ) > ( *_maxima )[j] )
                        {
                            ( *_maxima )[j] = get( i, j, _ncols, _features );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

size_t Summarizer::identify_bin(
    const size_t _num_bins,
    const METRICS_FLOAT _step_size,
    const METRICS_FLOAT _val,
    const METRICS_FLOAT _min )
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
