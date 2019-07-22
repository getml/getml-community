#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object AUC::score( const Features _yhat, const Features _y )
{
    // -----------------------------------------------------

    impl_.set_data( _yhat, _y );

    // -----------------------------------------------------

    if ( nrows() < 1 )
        {
            std::invalid_argument(
                "There needs to be at least one row for the AUC score to "
                "work!" );
        }

    // -----------------------------------------------------

    std::vector<Float> auc( ncols() );

    auto true_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    auto false_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    // -----------------------------------------------------

    for ( size_t j = 0; j < ncols(); ++j )
        {
            // ---------------------------------------------
            // Find minimum and maximum

            Float yhat_min = yhat( 0, j );

            Float yhat_max = yhat( 0, j );

            for ( size_t i = 0; i < nrows(); ++i )
                {
                    if ( yhat( i, j ) < yhat_min )
                        yhat_min = yhat( i, j );

                    else if ( yhat( i, j ) > yhat_max )
                        yhat_max = yhat( i, j );
                }

            // ---------------------------------------------
            // Reduce, if applicable.

            if ( impl_.has_comm() )
                {
                    impl_.reduce( multithreading::minimum<Float>(), &yhat_min );

                    impl_.reduce( multithreading::maximum<Float>(), &yhat_max );
                }

            // ---------------------------------------------
            // If there is no variation in _yhat, then auc is 0.5;

            if ( yhat_min == yhat_max )
                {
                    true_positive_arr->add( JSON::vector_to_array_ptr(
                        std::vector<Float>( {1.0, 0.0} ) ) );

                    false_positive_arr->add( JSON::vector_to_array_ptr(
                        std::vector<Float>( {0.0, 1.0} ) ) );

                    auc[j] = 0.5;

                    continue;
                }

            // ---------------------------------------------
            // Create prediction-target-pairs.

            std::vector<std::pair<Float, Float>> pairs( nrows() );

            for ( size_t i = 0; i < pairs.size(); ++i )
                {
                    pairs[i] =
                        std::pair<Float, Float>( yhat( i, j ), y( i, j ) );
                }

            // ---------------------------------------------
            // Sort prediction-target-pairs by prediction.

            const auto sort_by_prediction =
                []( const std::pair<Float, Float>& p1,
                    const std::pair<Float, Float>& p2 ) {
                    return ( std::get<0>( p1 ) < std::get<0>( p2 ) );
                };

            std::stable_sort( pairs.begin(), pairs.end(), sort_by_prediction );

            // ---------------------------------------------
            // Calculate true_positives_uncompressed

            auto true_positives_uncompressed = std::vector<Float>( nrows() );

            for ( size_t i = 0; i < nrows(); ++i )
                {
                    true_positives_uncompressed[i] = std::get<1>( pairs[i] );
                }

            std::partial_sum(
                true_positives_uncompressed.begin(),
                true_positives_uncompressed.end(),
                true_positives_uncompressed.begin() );

            const Float all_positives = true_positives_uncompressed.back();

            std::for_each(
                true_positives_uncompressed.begin(),
                true_positives_uncompressed.end(),
                [all_positives]( Float& val ) { val = all_positives - val; } );

            // ---------------------------------------------
            // Compress true_positives_uncompressed to get the actual
            // true_positives and predicted_negative.

            auto true_positives = std::vector<Float>( {all_positives} );

            auto predicted_negative = std::vector<Float>( {0.0} );

            for ( size_t i = 0; i < pairs.size(); )
                {
                    const auto it1 = pairs.begin() + i;

                    const auto prediction_is_greater =
                        [it1]( const std::pair<Float, Float>& p ) {
                            return std::get<0>( p ) > std::get<0>( *it1 );
                        };

                    const auto it2 =
                        std::find_if( it1, pairs.end(), prediction_is_greater );

                    const auto dist =
                        static_cast<size_t>( std::distance( it1, it2 ) );

                    assert( dist > 0 );
                    assert( i + dist - 1 < true_positives_uncompressed.size() );

                    true_positives.push_back(
                        true_positives_uncompressed[i + dist - 1] );

                    predicted_negative.push_back(
                        predicted_negative.back() +
                        static_cast<Float>( dist ) );

                    i += dist;
                }

            // ---------------------------------------------
            // Calculate false positives

            Float nrow_float = static_cast<Float>( nrows() );

            std::vector<Float> false_positives( true_positives.size() );

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                predicted_negative.begin(),
                false_positives.begin(),
                [nrow_float]( const Float& tp, const Float& pn ) {
                    return nrow_float - tp - pn;
                } );

            // ---------------------------------------------
            // Calculate true positive rate and false positive rate

            std::vector<Float> true_positive_rate( true_positives.size() );

            std::vector<Float> false_positive_rate( true_positives.size() );

            const Float all_negatives = nrow_float - all_positives;

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                true_positive_rate.begin(),
                [all_positives]( const Float& tp ) {
                    return tp / all_positives;
                } );

            std::transform(
                false_positives.begin(),
                false_positives.end(),
                false_positive_rate.begin(),
                [all_negatives]( const Float& fp ) {
                    return fp / all_negatives;
                } );

            // ---------------------------------------------
            // Calculate area under curve.

            for ( size_t i = 1; i < true_positives.size(); ++i )
                {
                    auc[j] +=
                        ( false_positive_rate[i - 1] -
                          false_positive_rate[i] ) *
                        ( true_positive_rate[i] + true_positive_rate[i - 1] ) *
                        0.5;
                }

            // ---------------------------------------------
            // Downsample true_postive_rate and false_positive_rate to be
            // displayed.

            const auto step_size = true_positives.size() / 100;

            auto tpr_downsampled = std::vector<Float>( 0 );

            auto fpr_downsampled = std::vector<Float>( 0 );

            for ( size_t i = 0; i < true_positives.size(); i += step_size )
                {
                    tpr_downsampled.push_back( true_positive_rate[i] );

                    fpr_downsampled.push_back( false_positive_rate[i] );
                }

            tpr_downsampled.push_back( true_positive_rate.back() );

            fpr_downsampled.push_back( false_positive_rate.back() );

            // -----------------------------------------------------
            // Add to arrays.

            true_positive_arr->add(
                JSON::vector_to_array_ptr( tpr_downsampled ) );

            false_positive_arr->add(
                JSON::vector_to_array_ptr( fpr_downsampled ) );

            // ---------------------------------------------
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "auc_", JSON::vector_to_array_ptr( auc ) );

    obj.set( "fpr_", false_positive_arr );

    obj.set( "tpr_", true_positive_arr );

    // -----------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace metrics
